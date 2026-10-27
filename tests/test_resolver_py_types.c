#include "cbm.h"
#include "lsp/py_proof.h"
#include "resolve/binding.h"
#include "resolve/policy.h"
#include "resolve/proof.h"
#include "test_framework.h"
static const char PY_SERVICE[] = "class Service:\n    def go(self):\n        pass\n";
typedef struct {
    const char *site, *caller;
    int caller_conflict;
} PyTypeQuery;
static int py_type_rows_for_context(const char *source, const char *library, int duplicate,
                                    CBMResolvedKind kind, const char *library_path,
                                    const char *source_path, const PyTypeQuery *query) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMFileResult *lib = cbm_extract_file(library, (int)strlen(library), CBM_LANG_PYTHON, "p",
                                          library_path, 0, NULL, NULL);
    CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "p",
                                           source_path, 0, NULL, NULL);
    if (file && query && query->caller_conflict) {
        CBMDefinition *typed = NULL, *decoy = NULL;
        for (int i = 0; i < file->defs.count; i++) {
            CBMDefinition *def = &file->defs.items[i];
            if (def->qualified_name && !strcmp(def->qualified_name, "p.main.Controller.typed"))
                typed = def;
            if (def->qualified_name && !strcmp(def->qualified_name, "p.main.Decoy.typed"))
                decoy = def;
        }
        if (typed && decoy && query->caller_conflict == 1)
            decoy->start_line = typed->start_line;
        else if (typed && query->caller_conflict == 2)
            typed->qualified_name = cbm_arena_strdup(&file->arena, "p.main.Impostor.typed");
    }
    CBMLSPDef defs[3] = {{.qualified_name = "p.lib.Service",
                          .graph_qualified_name = "p.lib.Service",
                          .short_name = "Service",
                          .label = "Class",
                          .source_start_line = 1,
                          .declaration_identity = "lib:0",
                          .def_module_qn = "p.lib",
                          .lang = CBM_LANG_PYTHON},
                         {.qualified_name = "p.lib.Service.go",
                          .graph_qualified_name = "p.lib.Service.go",
                          .short_name = "go",
                          .label = "Method",
                          .receiver_type = "p.lib.Service",
                          .source_start_line = 2,
                          .declaration_identity = "lib:1",
                          .def_module_qn = "p.lib",
                          .lang = CBM_LANG_PYTHON}};
    /* Keep source spans/decorators exact for decorated-library fixtures. */
    for (int i = 0; lib && i < lib->defs.count; i++) {
        const CBMDefinition *definition = &lib->defs.items[i];
        for (int d = 0; d < 2; d++) {
            if (definition->qualified_name &&
                !strcmp(definition->qualified_name, defs[d].qualified_name)) {
                defs[d].source_start_line = definition->start_line;
                defs[d].decorators = definition->decorators;
            }
        }
    }
    cbm_resolve_binding_index_t *bindings =
        lib ? cbm_resolve_binding_build(&arena, lib, CBM_LANG_PYTHON, library, strlen(library))
            : NULL;
    defs[0].export_binding_proven =
        (cbm_resolve_binding_export_kind(bindings, defs[0].qualified_name) &
         CBM_RESOLVE_EXPORT_TYPE) != 0;
    bool facts = cbm_py_collect_source_facts(&arena, library, (int)strlen(library),
                                             lib ? lib->module_qn : NULL, defs, 2);
    int count = 2;
    if (duplicate) {
        defs[2] = defs[duplicate == 1 ? 0 : 1];
        defs[2].declaration_identity = "other:0";
        count = 3;
    }
    CBMTypeRegistry *registry = cbm_py_build_cross_registry(&arena, defs, count);
    const char *names[] = {"Svc"}, *targets[] = {"p.lib.Service"};
    CBMResolvedCallArray out = {0};
    if (facts && registry && file)
        cbm_py_proof_resolve(&arena, source, (int)strlen(source), registry, file, names, targets, 1,
                             &out);
    int found = 0;
    for (int i = 0; i < out.count; i++) {
        CBMResolvedCall *row = &out.items[i];
        cbm_resolve_result_t proof = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                      .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                      .analysis = row->evidence.analysis,
                                      .rule = row->evidence.rule,
                                      .facts = row->evidence.facts,
                                      .target = row->callee_qn,
                                      .candidate_count = 1};
        const char *expected_site = query ? query->site : NULL;
        const char *expected_caller = query ? query->caller : NULL;
        const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(row->evidence.proof);
        bool matching_caller =
            !expected_caller ||
            (row->caller_qn && !strcmp(row->caller_qn, expected_caller) && row->callee_qn &&
             !strcmp(row->callee_qn, "p.lib.Service.go") && view && view->source.scope &&
             !strcmp(view->source.scope, expected_caller) &&
             view->key.start_byte == row->site_start_byte &&
             view->key.end_byte == row->site_end_byte && view->outcome.target &&
             !strcmp(view->outcome.target, row->callee_qn));
        bool matching_site =
            !expected_site ||
            (row->site_end_byte <= strlen(source) && row->site_start_byte < row->site_end_byte &&
             row->site_end_byte - row->site_start_byte == strlen(expected_site) &&
             !strncmp(source + row->site_start_byte, expected_site, strlen(expected_site)));
        if (row->kind == kind && cbm_resolve_admit(&proof) &&
            row->site_end_byte > row->site_start_byte && matching_site && matching_caller)
            found++;
    }
    cbm_free_result(lib);
    cbm_free_result(file);
    cbm_arena_destroy(&arena);
    return found;
}
static int py_type_rows_for_paths(const char *source, const char *library, int duplicate,
                                  CBMResolvedKind kind, const char *library_path,
                                  const char *source_path, const char *expected_site) {
    PyTypeQuery query = {.site = expected_site};
    return py_type_rows_for_context(source, library, duplicate, kind, library_path, source_path,
                                    &query);
}
static int py_type_rows(const char *source, const char *library, int duplicate,
                        CBMResolvedKind kind) {
    return py_type_rows_for_paths(source, library, duplicate, kind, "lib.py", "main.py", NULL);
}
TEST(resolver_python_parenthesized_reference_matches_exact_argument_site) {
    const char *source = "from lib import Service as Svc\ndef accept(callback):\n    pass\n"
                         "def run(value: Svc):\n    accept(((value.go)))\n    value.go()\n";
    ASSERT_EQ(py_type_rows_for_paths(source, PY_SERVICE, 0, CBM_RESOLVED_CALL_REFERENCE, "lib.py",
                                     "main.py", "((value.go))"),
              1);
    ASSERT_EQ(py_type_rows(source, PY_SERVICE, 0, CBM_RESOLVED_INVOCATION), 1);
    const char *unknown = "from lib import Service as Svc\ndef run(value: Svc):\n"
                          "    unknown((value.go))\n    value.go()\n";
    ASSERT_EQ(py_type_rows_for_paths(unknown, PY_SERVICE, 0, CBM_RESOLVED_CALL_REFERENCE, "lib.py",
                                     "main.py", "(value.go)"),
              1);
    ASSERT_EQ(py_type_rows(unknown, PY_SERVICE, 0, CBM_RESOLVED_INVOCATION), 0);
    const char *composite = "from lib import Service as Svc\ndef run(value: Svc):\n"
                            "    unknown((value.go, other))\n";
    ASSERT_EQ(py_type_rows(composite, PY_SERVICE, 0, CBM_RESOLVED_CALL_REFERENCE), 0);
    PASS();
}
TEST(resolver_python_package_declarations_and_effects_use_symbol_scope) {
    const char *source = "from vendor import *\nfrom lib import Service as Svc\n"
                         "def constant(value):\n    return value + 1\n"
                         "def run():\n    value=Svc()\n    constant(1)\n    value.go()\n";
    ASSERT_EQ(py_type_rows_for_paths(source, PY_SERVICE, 0, CBM_RESOLVED_INVOCATION,
                                     "lib/__init__.py", "main/__init__.py", "value.go()"),
              1);
    const char *shadow = "from lib import Service as Svc\nfrom vendor import *\n"
                         "def run():\n    value=Svc()\n    value.go()\n";
    ASSERT_EQ(py_type_rows_for_paths(shadow, PY_SERVICE, 0, CBM_RESOLVED_INVOCATION,
                                     "lib/__init__.py", "main/__init__.py", NULL),
              0);
    PASS();
}
TEST(resolver_python_declared_receiver_alias_and_reference) {
    const char *source = "from lib import Service as Svc\ndef run(value: Svc):\n    value.go()\n   "
                         " alias=value\n    alias.go()\n    callback=value.go\n";
    ASSERT_EQ(py_type_rows(source, PY_SERVICE, 0, CBM_RESOLVED_INVOCATION), 2);
    ASSERT_EQ(py_type_rows(source, PY_SERVICE, 0, CBM_RESOLVED_CALL_REFERENCE), 1);
    ASSERT_EQ(py_type_rows(source, PY_SERVICE, 0, CBM_RESOLVED_TYPE_REFERENCE), 1);
    PASS();
}
TEST(resolver_python_receiver_reassignment_scope_and_effects_abstain) {
    const char *bodies[] = {
        ("    value=other\n    value.go()\n"),
        ("    value=unknown()\n    value.go()\n"),
        ("    def change():\n        nonlocal value\n        value=other\n    value.go()\n"),
        ("    value.go=other\n    value.go()\n"),
        ("    unknown.update()\n    value.go()\n"),
        ("    if condition:\n        value=other\n    value.go()\n"),
        ("    value.go(1)\n")};
    for (size_t i = 0; i < sizeof(bodies) / sizeof(bodies[0]); i++) {
        char source[1024];
        snprintf(source, sizeof(source), "from lib import Service as Svc\ndef run(value: Svc):\n%s",
                 bodies[i]);
        int found = py_type_rows(source, PY_SERVICE, 0, CBM_RESOLVED_INVOCATION);
        if (found)
            fprintf(stderr, "Python typed unexpected proof %zu: %d\n", i, found);
        ASSERT_EQ(found, 0);
    }
    const char *shadow =
        "from lib import Service as Svc\nSvc=other\ndef run(value: Svc):\n    value.go()\n";
    ASSERT_EQ(py_type_rows(shadow, PY_SERVICE, 0, CBM_RESOLVED_INVOCATION), 0);
    PASS();
}
TEST(resolver_python_unproven_class_and_duplicate_domains_abstain) {
    const char *source = "from lib import Service as Svc\ndef run(value: Svc):\n    value.go()\n";
    const char *libraries[] = {("class Service(Other):\n    def go(self):\n        pass\n"),
                               ("class Service:\n    @property\n    def go(self):\n        pass\n"),
                               ("class Service:\n    def go(self):\n        pass\nService=other\n"),
                               ("class Service:\n    def go(self,arg):\n        pass\n"),
                               ("class Service:\n    def go(self):\n        pass\n    def "
                                "__getattribute__(self,name):\n        return other\n")};
    for (size_t i = 0; i < sizeof(libraries) / sizeof(libraries[0]); i++)
        ASSERT_EQ(py_type_rows(source, libraries[i], 0, CBM_RESOLVED_INVOCATION), 0);
    ASSERT_EQ(py_type_rows(source, PY_SERVICE, 1, CBM_RESOLVED_INVOCATION), 0);
    ASSERT_EQ(py_type_rows(source, PY_SERVICE, 2, CBM_RESOLVED_INVOCATION), 0);
    PASS();
}
TEST(resolver_type_qn_iterator_preserves_competitors_and_rejects_tail) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry base, overlay;
    cbm_registry_init(&base, &arena);
    cbm_registry_init(&overlay, &arena);
    cbm_registry_add_type(&base,
                          (CBMRegisteredType){.qualified_name = "p.Type", .short_name = "Type"});
    cbm_registry_add_type(&base,
                          (CBMRegisteredType){.qualified_name = "p.Type", .short_name = "Type"});
    cbm_registry_add_type(
        &base, (CBMRegisteredType){.qualified_name = "p.TypeOther", .short_name = "TypeOther"});
    cbm_registry_finalize(&base);
    cbm_registry_finalize(&overlay);
    overlay.fallback = &base;
    CBMTypeQNIter it;
    cbm_registry_types_with_qn(&overlay, "p.Type", &it);
    int count = 0;
    while (cbm_type_qn_next(&it, NULL))
        count++;
    ASSERT(it.complete);
    ASSERT_EQ(count, 2);
    cbm_registry_add_type(&overlay,
                          (CBMRegisteredType){.qualified_name = "p.Type", .short_name = "Type"});
    cbm_registry_types_with_qn(&overlay, "p.Type", &it);
    ASSERT_NULL(cbm_type_qn_next(&it, NULL));
    ASSERT(!it.complete);
    cbm_arena_destroy(&arena);
    PASS();
}
TEST(resolver_python_class_instance_rebinding_does_not_prove_method) {
    const char *source = "from lib import Service as Svc\ndef run(value: Svc):\n    value.go()\n";
    const char *library = "class Service:\n    def go(self):\n        pass\n"
                          "    def __init__(self):\n        self.go = other\n";
    ASSERT_EQ(py_type_rows(source, library, 0, CBM_RESOLVED_INVOCATION), 0);
    PASS();
}

TEST(resolver_python_constructor_types_propagate_to_aliases) {
    const char *source = "from lib import Service as Svc\ndef run():\n"
                         "    value=Svc()\n    value.go()\n    alias=value\n    alias.go()\n";
    ASSERT_EQ(py_type_rows(source, PY_SERVICE, 0, CBM_RESOLVED_INVOCATION), 2);
    const char *shadow = "from lib import Service as Svc\ndef run(Svc):\n"
                         "    value=Svc()\n    value.go()\n";
    ASSERT_EQ(py_type_rows(shadow, PY_SERVICE, 0, CBM_RESOLVED_INVOCATION), 0);
    const char *mutated = "from lib import Service as Svc\ndef run():\n"
                          "    value=Svc()\n    value=other\n    value.go()\n";
    ASSERT_EQ(py_type_rows(mutated, PY_SERVICE, 0, CBM_RESOLVED_INVOCATION), 0);
    const char *argument_required = "class Service:\n    def go(self):\n        pass\n"
                                    "    def __init__(self, value):\n        pass\n";
    ASSERT_EQ(py_type_rows(source, argument_required, 0, CBM_RESOLVED_INVOCATION), 0);
    ASSERT_EQ(py_type_rows(source, PY_SERVICE, 1, CBM_RESOLVED_INVOCATION), 0);
    PASS();
}

TEST(resolver_python_argument_references_precede_unknown_call_effects) {
    const char *known =
        "from lib import Service as Svc\ndef accept(callback):\n    pass\n"
        "def run(value: Svc):\n    value.go()\n    accept(value.go)\n    value.go()\n";
    ASSERT_EQ(py_type_rows(known, PY_SERVICE, 0, CBM_RESOLVED_INVOCATION), 2);
    ASSERT_EQ(py_type_rows(known, PY_SERVICE, 0, CBM_RESOLVED_CALL_REFERENCE), 1);
    const char *unknown = "from lib import Service as Svc\ndef run(value: Svc):\n"
                          "    value.go()\n    unknown(value.go)\n    value.go()\n";
    ASSERT_EQ(py_type_rows(unknown, PY_SERVICE, 0, CBM_RESOLVED_INVOCATION), 1);
    ASSERT_EQ(py_type_rows(unknown, PY_SERVICE, 0, CBM_RESOLVED_CALL_REFERENCE), 1);
    const char *shadow = "from lib import Service as Svc\ndef accept(callback):\n    pass\n"
                         "def run(value: Svc, accept):\n    accept(value.go)\n    value.go()\n";
    ASSERT_EQ(py_type_rows(shadow, PY_SERVICE, 0, CBM_RESOLVED_INVOCATION), 0);
    PASS();
}

TEST(resolver_python_decorated_values_do_not_donate_callable_identity) {
    const char *libraries[] = {
        "class Service:\n    @property\n    def go(self):\n        return 42\n",
        "def replace(function):\n    return 42\nclass Service:\n"
        "    @replace\n    def go(self):\n        pass\n"};
    const char *source = "from lib import Service as Svc\ndef run(value: Svc):\n"
                         "    callback=value.go\n    value.go()\n";
    for (size_t i = 0; i < sizeof(libraries) / sizeof(libraries[0]); i++) {
        ASSERT_EQ(py_type_rows(source, libraries[i], 0, CBM_RESOLVED_INVOCATION), 0);
        ASSERT_EQ(py_type_rows(source, libraries[i], 0, CBM_RESOLVED_CALL_REFERENCE), 0);
    }
    PASS();
}

TEST(resolver_python_value_domain_does_not_widen_callable_class_domain) {
    const char *library = "class Service:\n    def go(self):\n        pass\n"
                          "    @property\n    def value(self):\n        return 42\n";
    const char *source = "from lib import Service as Svc\ndef run(value: Svc):\n"
                         "    callback=value.go\n    value.go()\n";
    ASSERT_EQ(py_type_rows(source, library, 0, CBM_RESOLVED_INVOCATION), 0);
    ASSERT_EQ(py_type_rows(source, library, 0, CBM_RESOLVED_CALL_REFERENCE), 0);
    PASS();
}

static int py_caller_rows(const char *source, const char *path, const char *caller, int conflict) {
    PyTypeQuery query = {.site = "value.go()", .caller = caller, .caller_conflict = conflict};
    return py_type_rows_for_context(source, PY_SERVICE, 0, CBM_RESOLVED_INVOCATION, "lib.py", path,
                                    &query);
}

TEST(resolver_python_module_and_method_callers_keep_exact_scope) {
    const char *source = "from lib import Service as Svc\n"
                         "class Controller:\n"
                         "    def typed(self, value: Svc):\n        value.go()\n"
                         "    def made(self):\n        value=Svc()\n        value.go()\n"
                         "def plain(value: Svc):\n    value.go()\n"
                         "value=Svc()\nvalue.go()\n";
    ASSERT_EQ(py_caller_rows(source, "main.py", "p.main", 0), 1);
    ASSERT_EQ(py_caller_rows(source, "main.py", "p.main.Controller.typed", 0), 1);
    ASSERT_EQ(py_caller_rows(source, "main.py", "p.main.Controller.made", 0), 1);
    ASSERT_EQ(py_caller_rows(source, "main.py", "p.main.plain", 0), 1);
    ASSERT_EQ(py_caller_rows(source, "main.py", "p.main.Controller", 0), 0);
    ASSERT_EQ(py_caller_rows(source, "main.py", "p.main.__module__", 0), 0);
    ASSERT_EQ(py_type_rows(source, PY_SERVICE, 0, CBM_RESOLVED_INVOCATION), 4);
    ASSERT_EQ(py_caller_rows(source, "pkg/__init__.py", "p.pkg.__init__", 0), 1);
    ASSERT_EQ(py_caller_rows(source, "pkg/__init__.py", "p.pkg.Controller.typed", 0), 1);
    PASS();
}

TEST(resolver_python_caller_proof_does_not_execute_nested_bodies) {
    const char *source = "from lib import Service as Svc\n"
                         "class Outer:\n"
                         "    class Inner:\n"
                         "        def typed(self, value: Svc):\n            value.go()\n"
                         "def delayed(value: Svc):\n    value.go()\n";
    ASSERT_EQ(py_caller_rows(source, "main.py", "p.main", 0), 0);
    ASSERT_EQ(py_caller_rows(source, "main.py", "p.main.Outer", 0), 0);
    ASSERT_EQ(py_caller_rows(source, "main.py", "p.main.Outer.Inner", 0), 0);
    ASSERT_EQ(py_caller_rows(source, "main.py", "p.main.Outer.Inner.typed", 0), 1);
    ASSERT_EQ(py_caller_rows(source, "main.py", "p.main.delayed", 0), 1);
    ASSERT_EQ(py_type_rows(source, PY_SERVICE, 0, CBM_RESOLVED_INVOCATION), 2);
    PASS();
}

TEST(resolver_python_conflicting_or_wrong_caller_identity_abstains) {
    const char *source = "from lib import Service as Svc\n"
                         "class Controller:\n"
                         "    def typed(self, value: Svc):\n        value.go()\n"
                         "class Decoy:\n    def typed(self):\n        pass\n";
    ASSERT_EQ(py_caller_rows(source, "main.py", "p.main.Controller.typed", 0), 1);
    ASSERT_EQ(py_caller_rows(source, "main.py", NULL, 1), 0);
    ASSERT_EQ(py_caller_rows(source, "main.py", NULL, 2), 0);
    PASS();
}

TEST(resolver_python_new_caller_scopes_keep_unknown_receivers_unproven) {
    const char *sources[] = {"from lib import Service as Svc\nclass Controller:\n"
                             "    def typed(self, value):\n        value.go()\n",
                             "from lib import Service as Svc\nclass Controller(Svc):\n"
                             "    def typed(self):\n        self.go()\n",
                             "from lib import Service as Svc\nclass Controller:\n"
                             "    @unknown\n    def typed(self, value: Svc):\n        value.go()\n",
                             "from lib import Service as Svc\nvalue=Svc()\nunknown()\nvalue.go()\n",
                             "from lib import Service as Svc\nvalue=Svc()\n"
                             "if condition:\n    value=other\nvalue.go()\n",
                             "from lib import Service as Svc\nvalue=Svc()\nclass Controller:\n"
                             "    def typed(self):\n        value.go()\n"};
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++)
        ASSERT_EQ(py_type_rows(sources[i], PY_SERVICE, 0, CBM_RESOLVED_INVOCATION), 0);
    PASS();
}

SUITE(resolver_py_types) {
    RUN_TEST(resolver_python_module_and_method_callers_keep_exact_scope);
    RUN_TEST(resolver_python_caller_proof_does_not_execute_nested_bodies);
    RUN_TEST(resolver_python_conflicting_or_wrong_caller_identity_abstains);
    RUN_TEST(resolver_python_new_caller_scopes_keep_unknown_receivers_unproven);
    RUN_TEST(resolver_python_value_domain_does_not_widen_callable_class_domain);
    RUN_TEST(resolver_python_decorated_values_do_not_donate_callable_identity);
    RUN_TEST(resolver_python_parenthesized_reference_matches_exact_argument_site);
    RUN_TEST(resolver_python_package_declarations_and_effects_use_symbol_scope);
    RUN_TEST(resolver_python_argument_references_precede_unknown_call_effects);
    RUN_TEST(resolver_python_constructor_types_propagate_to_aliases);
    RUN_TEST(resolver_python_class_instance_rebinding_does_not_prove_method);
    RUN_TEST(resolver_python_declared_receiver_alias_and_reference);
    RUN_TEST(resolver_python_receiver_reassignment_scope_and_effects_abstain);
    RUN_TEST(resolver_python_unproven_class_and_duplicate_domains_abstain);
    RUN_TEST(resolver_type_qn_iterator_preserves_competitors_and_rejects_tail);
}
