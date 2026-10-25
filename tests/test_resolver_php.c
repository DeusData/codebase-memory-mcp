#include "test_framework.h"
#include "cbm.h"
#include "lsp/php_proof.h"
#include "resolve/policy.h"
#include <string.h>

static const char PHP_SERVICE_SOURCE[] =
    "<?php\nnamespace App\\Services;\nclass Service {\n"
    " public function go() {}\n public static function build() {}\n}\n";

static int php_proof_rows(const char *source, const char *library, bool mapped, const char *target,
                          bool include_type) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMLSPDef defs[] = {{.qualified_name = "p.Service",
                         .graph_qualified_name = "p.Service",
                         .short_name = "Service",
                         .label = "Class",
                         .def_module_qn = "p",
                         .source_start_line = 3,
                         .declaration_identity = "lib:0"},
                        {.qualified_name = "p.Service.go",
                         .graph_qualified_name = "p.Service.go",
                         .short_name = "go",
                         .label = "Method",
                         .def_module_qn = "p",
                         .receiver_type = "p.Service",
                         .source_start_line = 4,
                         .declaration_identity = "lib:1"},
                        {.qualified_name = "p.Service.build",
                         .graph_qualified_name = "p.Service.build",
                         .short_name = "build",
                         .label = "Method",
                         .def_module_qn = "p",
                         .receiver_type = "p.Service",
                         .source_start_line = 5,
                         .declaration_identity = "lib:2"}};
    bool facts = cbm_php_collect_source_facts(&arena, library, (int)strlen(library), "p", defs, 3);
    CBMTypeRegistry registry;
    cbm_registry_init(&registry, &arena);
    cbm_php_register_lsp_defs(&arena, &arena, &registry, defs, 3);
    cbm_registry_finalize(&registry);
    TSParser *parser = ts_parser_new();
    extern const TSLanguage *tree_sitter_php_only(void);
    ts_parser_set_language(parser, tree_sitter_php_only());
    TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)strlen(source));
    ts_parser_delete(parser);
    CBMImport import = {.local_name = "Svc", .module_path = "App\\Services\\Service"};
    CBMImportArray imports = {.items = &import, .count = 1};
    const char *targets[] = {mapped ? "p.Service" : NULL};
    CBMPHPImportBindings bindings = {.imports = &imports, .targets = targets};
    CBMResolvedCallArray out = {0};
    if (facts && tree)
        cbm_php_proof_resolve(&arena, source, (int)strlen(source), "p.Main",
                              ts_tree_root_node(tree), &registry, &bindings, defs, 3, &out);
    int matched = 0;
    for (int i = 0; i < out.count; i++) {
        const CBMResolvedCall *row = &out.items[i];
        cbm_resolve_result_t proof = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                      .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                      .analysis = row->evidence.analysis,
                                      .rule = row->evidence.rule,
                                      .facts = row->evidence.facts,
                                      .target = row->callee_qn,
                                      .candidate_count = 1};
        if (cbm_resolve_admit(&proof) && row->site_end_byte > row->site_start_byte &&
            (include_type || row->kind == CBM_RESOLVED_INVOCATION) &&
            (!target || !strcmp(target, row->callee_qn)))
            matched++;
    }
    if (tree)
        ts_tree_delete(tree);
    cbm_arena_destroy(&arena);
    return matched;
}

TEST(resolver_php_declared_receiver_and_static_call_have_proof) {
    const char *source = "<?php\nuse App\\Services\\Service as Svc;\nclass Main {\n"
                         " public function run(Svc $value) { $value->go(); Svc::build(); }\n}\n";
    ASSERT_EQ(php_proof_rows(source, PHP_SERVICE_SOURCE, true, "p.Service.go", false), 1);
    ASSERT_EQ(php_proof_rows(source, PHP_SERVICE_SOURCE, true, "p.Service.build", false), 1);
    ASSERT_GT(php_proof_rows(source, PHP_SERVICE_SOURCE, true, "p.Service", true), 0);
    ASSERT_EQ(php_proof_rows(source, PHP_SERVICE_SOURCE, false, NULL, true), 0);
    PASS();
}

TEST(resolver_php_unknown_writes_references_and_symbol_effects_abstain) {
    const char *bodies[] = {"$value = unknown(); $value->go();",
                            "$value->go(); $value = other();",
                            "$alias =& $value; $alias = other(); $value->go();",
                            "update($value); $value->go();",
                            "extract($map); $value->go();",
                            "$value->{$name}();",
                            "$value->go(1);"};
    for (size_t i = 0; i < sizeof(bodies) / sizeof(bodies[0]); i++) {
        char source[1024];
        snprintf(source, sizeof(source),
                 "<?php\nuse App\\Services\\Service as Svc;\n"
                 "class Main { public function run(Svc $value) { %s } }\n",
                 bodies[i]);
        ASSERT_EQ(php_proof_rows(source, PHP_SERVICE_SOURCE, true, NULL, false), 0);
    }
    const char *by_ref = "<?php\nuse App\\Services\\Service as Svc;\n"
                         "class Main { public function run(Svc &$value) { $value->go(); } }\n";
    ASSERT_EQ(php_proof_rows(by_ref, PHP_SERVICE_SOURCE, true, NULL, false), 0);
    PASS();
}

TEST(resolver_php_private_magic_and_duplicate_method_domains_abstain) {
    const char *source = "<?php\nuse App\\Services\\Service as Svc;\n"
                         "class Main { public function run(Svc $value) { $value->go(); } }\n";
    const char *libraries[] = {"<?php\nnamespace App\\Services;\nclass Service {\n private "
                               "function go() {}\n public static function build() {}\n}\n",
                               "<?php\nnamespace App\\Services;\nclass Service {\n public function "
                               "__call($name,$args) {}\n public static function build() {}\n}\n",
                               "<?php\nnamespace App\\Services;\nclass Service {\n public function "
                               "go() {}\n public function GO() {}\n}\n",
                               "<?php\nnamespace App\\Services;\nclass Service {\n use "
                               "DynamicTrait;\n public function build() {}\n}\n"};
    for (size_t i = 0; i < sizeof(libraries) / sizeof(libraries[0]); i++)
        ASSERT_EQ(php_proof_rows(source, libraries[i], true, NULL, false), 0);
    PASS();
}

TEST(resolver_php_import_alias_does_not_escape_its_namespace_block) {
    const char *source = "<?php\nnamespace A;\nuse App\\Services\\Service as Svc;\n"
                         "class First {}\nnamespace B;\n"
                         "class Main { public function run(Svc $value) { $value->go(); } }\n";
    ASSERT_EQ(php_proof_rows(source, PHP_SERVICE_SOURCE, true, NULL, false), 0);
    PASS();
}

SUITE(resolver_php) {
    RUN_TEST(resolver_php_import_alias_does_not_escape_its_namespace_block);
    RUN_TEST(resolver_php_declared_receiver_and_static_call_have_proof);
    RUN_TEST(resolver_php_unknown_writes_references_and_symbol_effects_abstain);
    RUN_TEST(resolver_php_private_magic_and_duplicate_method_domains_abstain);
}
