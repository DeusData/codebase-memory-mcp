#include "test_framework.h"
#include "cbm.h"
#include "lsp/py_proof.h"
#include "resolve/binding.h"
#include "resolve/policy.h"
static const char PY_SERVICE[] = "class Service:\n    def go(self):\n        pass\n";
static int py_type_rows(const char *source, const char *library, int duplicate,
                        CBMResolvedKind kind) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMFileResult *lib = cbm_extract_file(library, (int)strlen(library), CBM_LANG_PYTHON, "p",
                                          "lib.py", 0, NULL, NULL);
    CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "p",
                                           "main.py", 0, NULL, NULL);
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
    cbm_resolve_binding_index_t *bindings =
        lib ? cbm_resolve_binding_build(&arena, lib, CBM_LANG_PYTHON, library, strlen(library))
            : NULL;
    defs[0].export_binding_proven =
        (cbm_resolve_binding_export_kind(bindings, defs[0].qualified_name) &
         CBM_RESOLVE_EXPORT_TYPE) != 0;
    bool facts =
        cbm_py_collect_source_facts(&arena, library, (int)strlen(library), "p.lib", defs, 2);
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
        if (row->kind == kind && cbm_resolve_admit(&proof) &&
            row->site_end_byte > row->site_start_byte)
            found++;
    }
    cbm_free_result(lib);
    cbm_free_result(file);
    cbm_arena_destroy(&arena);
    return found;
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

SUITE(resolver_py_types) {
    RUN_TEST(resolver_python_argument_references_precede_unknown_call_effects);
    RUN_TEST(resolver_python_constructor_types_propagate_to_aliases);
    RUN_TEST(resolver_python_class_instance_rebinding_does_not_prove_method);
    RUN_TEST(resolver_python_declared_receiver_alias_and_reference);
    RUN_TEST(resolver_python_receiver_reassignment_scope_and_effects_abstain);
    RUN_TEST(resolver_python_unproven_class_and_duplicate_domains_abstain);
    RUN_TEST(resolver_type_qn_iterator_preserves_competitors_and_rejects_tail);
}
