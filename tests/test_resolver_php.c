#include "cbm.h"
#include "lsp/php_proof.h"
#include "pipeline/pipeline.h"
#include "resolve/policy.h"
#include "resolver_quality.h"
#include "store/store.h"
#include "test_framework.h"
#include "test_helpers.h"
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
    /* Production passes the import's own proof with its target. A target alone is
     * no predecessor, and the proof pass correctly refuses it. */
    cbm_resolve_proof_pool_t proofs;
    cbm_resolve_proof_pool_init_project(&proofs, &arena);
    cbm_resolve_proof_source_t proof_source = {.project = "p",
                                               .file = "main.php",
                                               .module = "p.Main",
                                               .scope = "p.Main",
                                               .content_identity = "source",
                                               .configuration_identity = "config",
                                               .source_origin = CBM_SOURCE_ORIGIN_RAW};
    cbm_resolve_proof_key_t proof_key = {.lookup_domain = "php.use",
                                         .lookup_key = "Svc",
                                         .binding_identity = "p.Service",
                                         .origin = CBM_SOURCE_ORIGIN_RAW,
                                         .detail = "IMPORTS"};
    cbm_resolve_result_t dependency = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                       .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                       .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                       .rule = CBM_RESOLVE_RULE_IMPORT_BINDING,
                                       .facts = CBM_RESOLVE_FACT_EXACT_TARGET |
                                                CBM_RESOLVE_FACT_IMPORT_BOUND |
                                                CBM_RESOLVE_FACT_LOOKUP_COMPLETE,
                                       .target = "p.Service",
                                       .candidate_count = 1,
                                       .reason = "php_use_binding"};
    dependency.proof = cbm_resolve_proof_leaf(&proofs, CBM_PROOF_SOURCE_FACT, &proof_source,
                                              &proof_key, &dependency);
    cbm_resolve_import_fact_t fact = {.local_name = "Svc",
                                      .module_path = "App\\Services\\Service",
                                      .dependency = dependency,
                                      .closed = true};
    cbm_resolve_import_snapshot_t snapshot = {
        .source = proof_source, .items = &fact, .count = 1, .complete = true};
    CBMPHPImportBindings bindings = {.imports = &imports, .targets = targets, .facts = &snapshot};
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

static const char PHP_DIRECT_SOURCE_0[] =
    "<?php\n"
    "namespace App;\n"
    "use App\\Services\\Service as Svc;\n"
    "use App\\Duplicates\\Duplicate as Dup;\n"
    "class Main {\n"
    " public function values(Svc $value, $x) { $value->take($x); "
    "$value->take(7); $value->take('text'); $value->take(true); "
    "$value->take(null); $value->pair($x, 1); }\n"
    " public function staticValue($x) { Svc::build($x); }\n"
    " public function zero(Svc $value) { $value->go(); }\n"
    " public function typedGap(Svc $value) { $value->typed(1); }\n"
    " public function defaultGap(Svc $value) { $value->defaulted(); }\n"
    " public function namedGap(Svc $value) { $value->take(value: 1); }\n"
    " public function referenceGap(Svc $value, $x) { $value->byref($x); }\n"
    " public function variadicGap(Svc $value) { $value->variadic(1); }\n"
    " public function receiverReferenceGap(Svc &$value) { $value->take(2); }\n"
    " public function argumentAliasGap(Svc $value) { $value->take($value); "
    "$value->go(); }\n"
    " public function invalidReference(Svc $value) { $value->byref(1); }\n"
    " public function invalidNamed(Svc $value) { $value->take(missing: 1); }\n"
    " public function invalidUnpack(Svc $value) { $value->take(...'text'); }\n"
    " public function invalidStatic() { Svc::take(1); }\n"
    " public function invalidPrivate(Svc $value) { $value->hidden(1); }\n"
    " public function invalidType(Svc $value) { $value->typed('text'); }\n"
    " public function invalidDomain(Dup $value) { $value->take(3); }\n"
    " public function unknownEffect(Svc $value) { $value = unknown(); "
    "$value->take(4); }\n"
    "}\n";

static const char PHP_DIRECT_SOURCE_1[] = "<?php\n"
                                          "namespace App\\Services;\n"
                                          "class Service {\n"
                                          " public function take($value) {}\n"
                                          " public function pair($left, $right) {}\n"
                                          " public static function build($value) {}\n"
                                          " public function typed(int $value) {}\n"
                                          " public function defaulted($value = 1) {}\n"
                                          " public function byref(&$value) {}\n"
                                          " public function variadic(...$values) {}\n"
                                          " private function hidden($value) {}\n"
                                          " public function go() {}\n"
                                          "}\n";

static const char PHP_DIRECT_SOURCE_2[] =
    "<?php\n"
    "namespace App;\n"
    "class Calc {\n"
    " public function helper($x) { return $x + 1; }\n"
    " private function hidden($x) { return $x; }\n"
    " public function run($y) { $this->helper($y); $this->hidden(2); }\n"
    " public static function invalidThis() { $this->helper(3); }\n"
    " public function localTypeGap(Calc $value) { $value->helper(4); }\n"
    "}\n";

static const char PHP_DIRECT_SOURCE_3[] =
    "<?php\n"
    "namespace App;\n"
    "class Derived extends Calc {\n"
    " public function inheritedGap($x) { $this->helper($x); }\n"
    "}\n";

static const char PHP_DIRECT_SOURCE_4[] = "<?php\n"
                                          "namespace App\\Duplicates;\n"
                                          "class Duplicate {\n"
                                          " public function take($value) {}\n"
                                          " public function TAKE($value) {}\n"
                                          "}\n";

static const char PHP_DIRECT_SOURCE_5[] = "<?php\n"
                                          "namespace Other;\n"
                                          "class Service {\n"
                                          " public function take($value) {}\n"
                                          "}\n";

static const RQ_Expected PHP_DIRECT_EXPECTED[] = {
    {.case_id = "untyped-variable",
     .site = {.file = "src/Main.php",
              .caller = "Main.values",
              .needle = "$value->take($x)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "src/Services/Service.php",
                .suffix = "Service.take",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = " public function take($value) {}"}},
    {.case_id = "integer-value",
     .site = {.file = "src/Main.php",
              .caller = "Main.values",
              .needle = "$value->take(7)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "src/Services/Service.php",
                .suffix = "Service.take",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = " public function take($value) {}"}},
    {.case_id = "string-value",
     .site = {.file = "src/Main.php",
              .caller = "Main.values",
              .needle = "$value->take('text')",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "src/Services/Service.php",
                .suffix = "Service.take",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = " public function take($value) {}"}},
    {.case_id = "boolean-value",
     .site = {.file = "src/Main.php",
              .caller = "Main.values",
              .needle = "$value->take(true)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "src/Services/Service.php",
                .suffix = "Service.take",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = " public function take($value) {}"}},
    {.case_id = "null-value",
     .site = {.file = "src/Main.php",
              .caller = "Main.values",
              .needle = "$value->take(null)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "src/Services/Service.php",
                .suffix = "Service.take",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = " public function take($value) {}"}},
    {.case_id = "fixed-two-arguments",
     .site = {.file = "src/Main.php",
              .caller = "Main.values",
              .needle = "$value->pair($x, 1)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "src/Services/Service.php",
                .suffix = "Service.pair",
                .label = "Method",
                .declaration_line = 5,
                .declaration_text = " public function pair($left, $right) {}"}},
    {.case_id = "static-untyped",
     .site = {.file = "src/Main.php",
              .caller = "Main.staticValue",
              .needle = "Svc::build($x)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "src/Services/Service.php",
                .suffix = "Service.build",
                .label = "Method",
                .declaration_line = 6,
                .declaration_text = " public static function build($value) {}"}},
    {.case_id = "preserve-zero-arguments",
     .site = {.file = "src/Main.php",
              .caller = "Main.zero",
              .needle = "$value->go()",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "src/Services/Service.php",
                .suffix = "Service.go",
                .label = "Method",
                .declaration_line = 12,
                .declaration_text = " public function go() {}"}},
    {.case_id = "lexical-public-this",
     .site = {.file = "src/Calc.php",
              .caller = "Calc.run",
              .needle = "$this->helper($y)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "src/Calc.php",
                .suffix = "Calc.helper",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = " public function helper($x) { return $x + 1; }"}},
    {.case_id = "lexical-private-this",
     .site = {.file = "src/Calc.php",
              .caller = "Calc.run",
              .needle = "$this->hidden(2)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "src/Calc.php",
                .suffix = "Calc.hidden",
                .label = "Method",
                .declaration_line = 5,
                .declaration_text = " private function hidden($x) { return $x; }"}},
    {.case_id = "typed-parameter",
     .site = {.file = "src/Main.php",
              .caller = "Main.typedGap",
              .needle = "$value->typed(1)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "src/Services/Service.php",
                .suffix = "Service.typed",
                .label = "Method",
                .declaration_line = 7,
                .declaration_text = " public function typed(int $value) {}"}},
    {.case_id = "default-parameter",
     .site = {.file = "src/Main.php",
              .caller = "Main.defaultGap",
              .needle = "$value->defaulted()",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "src/Services/Service.php",
                .suffix = "Service.defaulted",
                .label = "Method",
                .declaration_line = 8,
                .declaration_text = " public function defaulted($value = 1) {}"}},
    {.case_id = "named-parameter",
     .site = {.file = "src/Main.php",
              .caller = "Main.namedGap",
              .needle = "$value->take(value: 1)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "src/Services/Service.php",
                .suffix = "Service.take",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = " public function take($value) {}"}},
    {.case_id = "reference-parameter",
     .site = {.file = "src/Main.php",
              .caller = "Main.referenceGap",
              .needle = "$value->byref($x)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "src/Services/Service.php",
                .suffix = "Service.byref",
                .label = "Method",
                .declaration_line = 9,
                .declaration_text = " public function byref(&$value) {}"}},
    {.case_id = "variadic-parameter",
     .site = {.file = "src/Main.php",
              .caller = "Main.variadicGap",
              .needle = "$value->variadic(1)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "src/Services/Service.php",
                .suffix = "Service.variadic",
                .label = "Method",
                .declaration_line = 10,
                .declaration_text = " public function variadic(...$values) {}"}},
    {.case_id = "receiver-reference",
     .site = {.file = "src/Main.php",
              .caller = "Main.receiverReferenceGap",
              .needle = "$value->take(2)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "src/Services/Service.php",
                .suffix = "Service.take",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = " public function take($value) {}"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "php-receiver-reference",
     .why = "A receiver parameter passed by reference has no stable receiver "
            "fact.",
     .tried = "The annotation is genuine, but the whole-function alias audit "
              "retains this unresolved receiver."},
    {.case_id = "argument-alias-call",
     .site = {.file = "src/Main.php",
              .caller = "Main.argumentAliasGap",
              .needle = "$value->take($value)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "src/Services/Service.php",
                .suffix = "Service.take",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = " public function take($value) {}"}},
    {.case_id = "argument-alias-later",
     .site = {.file = "src/Main.php",
              .caller = "Main.argumentAliasGap",
              .needle = "$value->go()",
              .occurrence_index = 1},
     .relation = "CALLS",
     .target = {.file = "src/Services/Service.php",
                .suffix = "Service.go",
                .label = "Method",
                .declaration_line = 12,
                .declaration_text = " public function go() {}"}},
    {.case_id = "same-file-namespace-type",
     .site = {.file = "src/Calc.php",
              .caller = "Calc.localTypeGap",
              .needle = "$value->helper(4)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "src/Calc.php",
                .suffix = "Calc.helper",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = " public function helper($x) { return $x + 1; }"}},
    {.case_id = "inherited-this",
     .site = {.file = "src/Derived.php",
              .caller = "Derived.inheritedGap",
              .needle = "$this->helper($x)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .target = {.file = "src/Calc.php",
                .suffix = "Calc.helper",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = " public function helper($x) { return $x + 1; }"}},
};

static const RQ_Forbidden PHP_DIRECT_FORBIDDEN[] = {
    {.case_id = "literal-cannot-bind-reference",
     .site = {.file = "src/Main.php",
              .caller = "Main.invalidReference",
              .needle = "$value->byref(1)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "unknown-named-argument",
     .site = {.file = "src/Main.php",
              .caller = "Main.invalidNamed",
              .needle = "$value->take(missing: 1)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "noniterable-unpack",
     .site = {.file = "src/Main.php",
              .caller = "Main.invalidUnpack",
              .needle = "$value->take(...'text')",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "instance-method-called-statically",
     .site = {.file = "src/Main.php",
              .caller = "Main.invalidStatic",
              .needle = "Svc::take(1)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "private-outside-class",
     .site = {.file = "src/Main.php",
              .caller = "Main.invalidPrivate",
              .needle = "$value->hidden(1)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "incompatible-typed-literal",
     .site = {.file = "src/Main.php",
              .caller = "Main.invalidType",
              .needle = "$value->typed('text')",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "case-insensitive-duplicate-domain",
     .site = {.file = "src/Main.php",
              .caller = "Main.invalidDomain",
              .needle = "$value->take(3)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "unknown-function-replaces-receiver",
     .site = {.file = "src/Main.php",
              .caller = "Main.unknownEffect",
              .needle = "$value->take(4)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "this-in-static-method",
     .site = {.file = "src/Calc.php",
              .caller = "Calc.invalidThis",
              .needle = "$this->helper(3)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
};

TEST(resolver_php_untyped_arguments_and_lexical_this_require_proofs) {
    for (int driver = 0; driver < 2; driver++) {
        const char *root = th_mktempdir("cbm_php_direct");
        ASSERT_NOT_NULL(root);
        char repo[768], database[768];
        snprintf(repo, sizeof(repo), "%s/repo", root);
        snprintf(database, sizeof(database), "%s/index.db", root);
        const RQ_Source sources[] = {
            {.path = "src/Main.php", .text = PHP_DIRECT_SOURCE_0},
            {.path = "src/Services/Service.php", .text = PHP_DIRECT_SOURCE_1},
            {.path = "src/Calc.php", .text = PHP_DIRECT_SOURCE_2},
            {.path = "src/Derived.php", .text = PHP_DIRECT_SOURCE_3},
            {.path = "src/Duplicates/Duplicate.php", .text = PHP_DIRECT_SOURCE_4},
            {.path = "src/Decoy.php", .text = PHP_DIRECT_SOURCE_5},
        };
        bool ok = cbm_mkdir_p(repo, 0755) && cbm_mkdir_p(TH_PATH(repo, "src/Services"), 0755) &&
                  cbm_mkdir_p(TH_PATH(repo, "src/Duplicates"), 0755);
        for (size_t i = 0; ok && i < sizeof(sources) / sizeof(sources[0]); i++)
            ok = th_write_file(TH_PATH(repo, sources[i].path), sources[i].text) == 0;
        ok = ok && th_write_file(TH_PATH(repo, "composer.json"),
                                 "{\"autoload\": {\"psr-4\": {\"App\\\\\": \"src/\"}}}\n") == 0;
        for (int i = 0; ok && i < 55; i++) {
            char path[32];
            snprintf(path, sizeof(path), "Filler%02d.php", i);
            ok = th_write_file(TH_PATH(repo, path), "<?php\n// unrelated source file\n") == 0;
        }
        const char *old_workers = getenv("CBM_WORKERS");
        char *saved_workers = old_workers ? strdup(old_workers) : NULL;
        ok = ok && (!old_workers || saved_workers);
        cbm_setenv("CBM_WORKERS", driver ? "4" : "1", 1);
        cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
        ok = pipeline && cbm_pipeline_set_project_name(pipeline, "php-direct") &&
             cbm_pipeline_run(pipeline) == 0;
        cbm_pipeline_free(pipeline);
        if (saved_workers) {
            cbm_setenv("CBM_WORKERS", saved_workers, 1);
            free(saved_workers);
        } else
            cbm_unsetenv("CBM_WORKERS");
        cbm_store_t *store = ok ? cbm_store_open_path_existing(database) : NULL;
        RQ_Report report = {0};
        ok = store &&
             rq_check(
                 store, "php-direct", "direct-php-members", sources,
                 sizeof(sources) / sizeof(sources[0]), PHP_DIRECT_EXPECTED,
                 sizeof(PHP_DIRECT_EXPECTED) / sizeof(PHP_DIRECT_EXPECTED[0]), PHP_DIRECT_FORBIDDEN,
                 sizeof(PHP_DIRECT_FORBIDDEN) / sizeof(PHP_DIRECT_FORBIDDEN[0]), &report, stderr) &&
             ok;
        if (store)
            cbm_store_close(store);
        th_cleanup(root);
        ASSERT(ok);
        /* Nine former gaps are decided now by true rules: a declared parameter type
         * selects the class whatever the call's argument shape, within the signature's
         * arity, since PHP passes an object by handle a by-value argument cannot rebind
         * the caller's variable, an unqualified type name binds in its own namespace,
         * and $this reaches the parent chain. Only a by-reference receiver stays open. */
        ASSERT_EQ(report.genuine, 20);
        ASSERT_EQ(report.found, 19);
        ASSERT_EQ(report.missing, 1);
        ASSERT_EQ(report.required_missing + report.wrong, 0);
    }
    PASS();
}

/* Forms taken from koel (app/Services, app/Repositories, app/Models): promoted
 * constructor properties, inherited repository methods, global helper functions
 * called from a namespace, static builders returning their own class. Each
 * positive is a PHP rule; each negative is a same-named decoy that must not bind. */
static const char PHP_RULES_HELPERS[] =
    "<?php\n"
    "function image_storage_path(?string $name = null): string { return ''; }\n"
    "function both() { return 'global'; }\n"
    "if (!function_exists('polyfill')) {\n"
    "    function polyfill() { return 1; }\n"
    "}\n";
static const char PHP_RULES_FUNCTIONS[] = "<?php\n"
                                          "namespace App\\Services;\n"
                                          "function same_ns() { return 1; }\n"
                                          "function both() { return 'namespaced'; }\n"
                                          "function shadowed() { return 'decoy'; }\n";
static const char PHP_RULES_SUPPORT[] = "<?php\n"
                                        "namespace App\\Support;\n"
                                        "function local_fn() { return 1; }\n"
                                        "function helper() { return 2; }\n";
static const char PHP_RULES_OTHER_NS[] = "<?php\n"
                                         "namespace App\\Other;\n"
                                         "function only_in_other() { return 1; }\n";
static const char PHP_RULES_REPOSITORY[] = "<?php\n"
                                           "namespace App\\Repositories;\n"
                                           "abstract class Repository {\n"
                                           "    public function getOne($id) { return null; }\n"
                                           "    public static function make(): static { return new "
                                           "static(); }\n"
                                           "}\n";
static const char PHP_RULES_ALBUM_REPOSITORY[] =
    "<?php\n"
    "namespace App\\Repositories;\n"
    "class AlbumRepository extends Repository {\n"
    "    public function chain(): self { return $this; }\n"
    "}\n";
static const char PHP_RULES_ALBUM[] =
    "<?php\n"
    "namespace App\\Models;\n"
    "class Album {\n"
    "    public function isOwned() { return true; }\n"
    "    public static function query(): \\App\\Builders\\AlbumBuilder { return null; }\n"
    "}\n";
static const char PHP_RULES_BUILDER[] = "<?php\n"
                                        "namespace App\\Builders;\n"
                                        "class AlbumBuilder {\n"
                                        "    public function published(): self { return $this; }\n"
                                        "}\n";
static const char PHP_RULES_STATUS[] =
    "<?php\n"
    "namespace App\\Models;\n"
    "enum Status: string {\n"
    "    case Active = 'active';\n"
    "    public function label(): string { return $this->value; }\n"
    "}\n";
static const char PHP_RULES_TRAIT[] = "<?php\n"
                                      "namespace App\\Concerns;\n"
                                      "trait Loads {\n"
                                      "    public function fromTrait() { return 1; }\n"
                                      "}\n";
static const char PHP_RULES_BASE[] =
    "<?php\n"
    "namespace App\\Services;\n"
    "use Illuminate\\Database\\Eloquent\\Model;\n"
    "abstract class BaseService extends Model {\n"
    "    public function __construct() {}\n"
    "    public function inherited() { return 1; }\n"
    "}\n";
static const char PHP_RULES_STORE[] = "<?php\n"
                                      "namespace App\\Services;\n"
                                      "class Store {\n"
                                      "    public function get() { return 1; }\n"
                                      "    private function hidden() { return 2; }\n"
                                      "    public function byref(&$x) { return 3; }\n"
                                      "    public function save() { return 4; }\n"
                                      "    public function anything() { return 5; }\n"
                                      "    public function go() { return 6; }\n"
                                      "}\n";
static const char PHP_RULES_OTHER[] = "<?php\n"
                                      "namespace App\\Services;\n"
                                      "class Other {\n"
                                      "    public function get() { return 1; }\n"
                                      "}\n";
static const char PHP_RULES_BUILT[] = "<?php\n"
                                      "namespace App\\Services;\n"
                                      "class Built {\n"
                                      "    public function __construct() {}\n"
                                      "}\n";
static const char PHP_RULES_MAGIC[] = "<?php\n"
                                      "namespace App\\Services;\n"
                                      "class Magic {\n"
                                      "    public function __call($n, $a) { return null; }\n"
                                      "}\n";
static const char PHP_RULES_DUP_A[] = "<?php\n"
                                      "namespace App\\Services;\n"
                                      "class Dup {\n"
                                      "    public function go() { return 1; }\n"
                                      "}\n";
static const char PHP_RULES_DUP_B[] = "<?php\n"
                                      "namespace App\\Services;\n"
                                      "class Dup {\n"
                                      "    public function go() { return 2; }\n"
                                      "}\n";
static const char PHP_RULES_MAIN[] =
    "<?php\n"
    "namespace App\\Services;\n"
    "use App\\Models\\Album;\n"
    "use App\\Models\\Status;\n"
    "use App\\Concerns\\Loads;\n"
    "use App\\Repositories\\AlbumRepository;\n"
    "use App\\Support;\n"
    "use function App\\Support\\helper as imported_helper;\n"
    "use function Vendor\\Lib\\shadowed;\n"
    "class AlbumService extends BaseService {\n"
    "    use Loads;\n"
    "    public function __construct(private readonly AlbumRepository $albumRepository, private "
    "?Album $album = null) { parent::__construct(); }\n"
    "    public function viaPromoted() { return $this->albumRepository->getOne(1); }\n"
    "    public function viaInherited() { return $this->inherited(); }\n"
    "    public function viaSelf() { return self::build(); }\n"
    "    public function viaStatic() { return static::build(); }\n"
    "    public static function build() { return null; }\n"
    "    public function viaTrait() { return $this->fromTrait(); }\n"
    "    public function viaChain() { return AlbumRepository::make()->chain(); }\n"
    "    public function viaNew() { return new Built(); }\n"
    "    public function viaLocal() { $s = new Store(); return $s->get(); }\n"
    "    public function viaParam(Album $album) { return $album->isOwned(); }\n"
    "    public function viaNullsafe(?Album $album) { return $album?->isOwned(); }\n"
    "    public function viaProperty() { return $this->album->isOwned(); }\n"
    "    public function viaArrow() { return fn () => $this->own(); }\n"
    "    public function viaClosure(Album $album) { return function () use ($album) { return "
    "$album->isOwned(); }; }\n"
    "    public function viaReturnType() { return Album::query()->published(); }\n"
    "    public function viaEnum() { return Status::Active->label(); }\n"
    "    private function own() { return 1; }\n"
    "    public function viaCase(Store $s) { return $s->GET(); }\n"
    "    public function fnGlobal() { return image_storage_path(); }\n"
    "    public function fnImported() { return imported_helper(); }\n"
    "    public function fnFull() { return \\App\\Support\\local_fn(); }\n"
    "    public function fnAlias() { return Support\\local_fn(); }\n"
    "    public function fnCase() { return \\App\\Support\\LOCAL_FN(); }\n"
    "    public function fnNamespace() { return same_ns(); }\n"
    "    public function fnNamespaceWins() { return both(); }\n"
    "    public function noExternal() { return $this->save(); }\n"
    "    public function noReassign($c) { $x = new Store(); if ($c) { $x = new Other(); } return "
    "$x->get(); }\n"
    "    public function noEarly() { $x->get(); $x = new Store(); }\n"
    "    public function noExtract($m) { $x = new Store(); extract($m); return $x->get(); }\n"
    "    public function noRef() { $x = new Store(); $y =& $x; return $x->get(); }\n"
    "    public function noVarVar($n) { $x = new Store(); $$n = 1; return $x->get(); }\n"
    "    public function noPolyfill() { return polyfill(); }\n"
    "    public function noShadow() { return shadowed(); }\n"
    "    public function noOtherNamespace() { return only_in_other(); }\n"
    "    public function noUnion(Store|Other $x) { return $x->get(); }\n"
    "    public function noDoc() { /** @var Store $x */ $x = make_it(); return $x->get(); }\n"
    "    public function noPrivate(Store $s) { return $s->hidden(); }\n"
    "    public function noStaticInstance() { return Store::get(); }\n"
    "    public function noDuplicate(Dup $d) { return $d->go(); }\n"
    "    public function noMagic(Magic $m) { return $m->anything(); }\n"
    "    public function noByRefLiteral(Store $s) { return $s->byref(1); }\n"
    "    public function noArity(Store $s) { return $s->get(1); }\n"
    "    public function noAnonymous() { return new class { public function own2() { return "
    "$this->own(); } }; }\n"
    "    public function noStaticThis() { return static function () { return $this->own(); }; }\n"
    "}\n";

#define PHP_MAIN "app/Services/AlbumService.php"
#define PHP_RULE_AT(id, method, text, at, tfile, tsuffix, tlabel, tline, tdecl)               \
    {                                                                                          \
        .case_id = id,                                                                         \
        .site = {.file = PHP_MAIN,                                                             \
                 .caller = "AlbumService." method,                                             \
                 .needle = text,                                                               \
                 .occurrence_index = at},                                                      \
        .relation = "CALLS",                                                                   \
        .target = {.file = tfile,                                                              \
                   .suffix = tsuffix,                                                          \
                   .label = tlabel,                                                            \
                   .declaration_line = tline,                                                  \
                   .declaration_text = tdecl}                                                  \
    }
#define PHP_RULE(id, method, text, tfile, tsuffix, tlabel, tline, tdecl) \
    PHP_RULE_AT(id, method, text, 0, tfile, tsuffix, tlabel, tline, tdecl)
#define PHP_DENY(id, method, text, at)                                                         \
    {                                                                                          \
        .case_id = id,                                                                         \
        .site = {.file = PHP_MAIN,                                                             \
                 .caller = "AlbumService." method,                                             \
                 .needle = text,                                                               \
                 .occurrence_index = at},                                                      \
        .relation = "CALLS", .any_target = true                                                \
    }

static const RQ_Expected PHP_RULES_EXPECTED[] = {
    PHP_RULE("promoted-property-inherited-method", "viaPromoted",
             "$this->albumRepository->getOne(1)", "app/Repositories/Repository.php",
             "Repository.getOne", "Method", 4, "    public function getOne($id) { return null; }"),
    PHP_RULE("this-inherited-before-chain-leaves-project", "viaInherited", "$this->inherited()",
             "app/Services/BaseService.php", "BaseService.inherited", "Method", 6,
             "    public function inherited() { return 1; }"),
    PHP_RULE("self-static-member", "viaSelf", "self::build()", PHP_MAIN, "AlbumService.build",
             "Method", 17, "    public static function build() { return null; }"),
    PHP_RULE("static-keyword-member", "viaStatic", "static::build()", PHP_MAIN,
             "AlbumService.build", "Method", 17,
             "    public static function build() { return null; }"),
    PHP_RULE("trait-member", "viaTrait", "$this->fromTrait()", "app/Concerns/Loads.php",
             "Loads.fromTrait", "Method", 4, "    public function fromTrait() { return 1; }"),
    PHP_RULE("static-return-type-keeps-receiver", "viaChain", "AlbumRepository::make()->chain()",
             "app/Repositories/AlbumRepository.php", "AlbumRepository.chain", "Method", 4,
             "    public function chain(): self { return $this; }"),
    PHP_RULE("static-return-type-inherited-member", "viaChain", "AlbumRepository::make()",
             "app/Repositories/Repository.php", "Repository.make", "Method", 5,
             "    public static function make(): static { return new static(); }"),
    PHP_RULE("new-selects-constructor", "viaNew", "new Built()", "app/Services/Built.php",
             "Built.__construct", "Method", 4, "    public function __construct() {}"),
    PHP_RULE("single-new-assignment", "viaLocal", "$s->get()", "app/Services/Store.php",
             "Store.get", "Method", 4, "    public function get() { return 1; }"),
    PHP_RULE("typed-parameter", "viaParam", "$album->isOwned()", "app/Models/Album.php",
             "Album.isOwned", "Method", 4, "    public function isOwned() { return true; }"),
    PHP_RULE("nullable-parameter-nullsafe", "viaNullsafe", "$album?->isOwned()",
             "app/Models/Album.php", "Album.isOwned", "Method", 4,
             "    public function isOwned() { return true; }"),
    PHP_RULE("typed-promoted-property", "viaProperty", "$this->album->isOwned()",
             "app/Models/Album.php", "Album.isOwned", "Method", 4,
             "    public function isOwned() { return true; }"),
    PHP_RULE_AT("closure-by-value-capture", "viaClosure", "$album->isOwned()", 1,
                "app/Models/Album.php", "Album.isOwned", "Method", 4,
                "    public function isOwned() { return true; }"),
    PHP_RULE("arrow-function-binds-this", "viaArrow", "$this->own()", PHP_MAIN, "AlbumService.own",
             "Method", 29, "    private function own() { return 1; }"),
    PHP_RULE("declared-return-type", "viaReturnType", "Album::query()->published()",
             "app/Builders/AlbumBuilder.php", "AlbumBuilder.published", "Method", 4,
             "    public function published(): self { return $this; }"),
    PHP_RULE("declared-return-type-static", "viaReturnType", "Album::query()",
             "app/Models/Album.php", "Album.query", "Method", 5,
             "    public static function query(): \\App\\Builders\\AlbumBuilder { return null; }"),
    PHP_RULE("enum-case-member", "viaEnum", "Status::Active->label()", "app/Models/Status.php",
             "Status.label", "Method", 5,
             "    public function label(): string { return $this->value; }"),
    PHP_RULE("method-name-case-insensitive", "viaCase", "$s->GET()", "app/Services/Store.php",
             "Store.get", "Method", 4, "    public function get() { return 1; }"),
    PHP_RULE("global-fallback-from-namespace", "fnGlobal", "image_storage_path()",
             "app/Helpers.php", "Helpers.image_storage_path", "Function", 2,
             "function image_storage_path(?string $name = null): string { return ''; }"),
    PHP_RULE("use-function-alias", "fnImported", "imported_helper()", "app/Support/funcs.php",
             "funcs.helper", "Function", 4, "function helper() { return 2; }"),
    PHP_RULE("fully-qualified-function", "fnFull", "\\App\\Support\\local_fn()",
             "app/Support/funcs.php", "funcs.local_fn", "Function", 3,
             "function local_fn() { return 1; }"),
    PHP_RULE_AT("qualified-through-class-alias", "fnAlias", "Support\\local_fn()", 1,
                "app/Support/funcs.php", "funcs.local_fn", "Function", 3,
                "function local_fn() { return 1; }"),
    PHP_RULE("function-name-case-insensitive", "fnCase", "\\App\\Support\\LOCAL_FN()",
             "app/Support/funcs.php", "funcs.local_fn", "Function", 3,
             "function local_fn() { return 1; }"),
    PHP_RULE("same-namespace-other-file", "fnNamespace", "same_ns()", "app/Services/functions.php",
             "functions.same_ns", "Function", 3, "function same_ns() { return 1; }"),
    PHP_RULE("namespace-wins-over-global", "fnNamespaceWins", "both()",
             "app/Services/functions.php", "functions.both", "Function", 4,
             "function both() { return 'namespaced'; }"),
};

static const RQ_Forbidden PHP_RULES_FORBIDDEN[] = {
    PHP_DENY("chain-leaves-project-before-member", "noExternal", "$this->save()", 0),
    PHP_DENY("two-assigned-types", "noReassign", "$x->get()", 0),
    PHP_DENY("use-before-assignment", "noEarly", "$x->get()", 1),
    PHP_DENY("extract-writes-the-symbol-table", "noExtract", "$x->get()", 2),
    PHP_DENY("reference-alias", "noRef", "$x->get()", 3),
    PHP_DENY("variable-variable-write", "noVarVar", "$x->get()", 4),
    PHP_DENY("conditional-function-declaration", "noPolyfill", "polyfill()", 0),
    PHP_DENY("vendor-import-shadows-namespace-decoy", "noShadow", "shadowed()", 0),
    PHP_DENY("other-namespace-is-not-in-the-lookup", "noOtherNamespace", "only_in_other()", 0),
    PHP_DENY("union-type-has-no-single-target", "noUnion", "$x->get()", 5),
    PHP_DENY("docblock-is-not-enforced", "noDoc", "$x->get()", 6),
    PHP_DENY("private-outside-its-class", "noPrivate", "$s->hidden()", 0),
    PHP_DENY("instance-member-called-statically", "noStaticInstance", "Store::get()", 0),
    PHP_DENY("class-declared-twice", "noDuplicate", "$d->go()", 0),
    PHP_DENY("magic-call-has-no-declaration", "noMagic", "$m->anything()", 0),
    PHP_DENY("literal-cannot-bind-by-reference", "noByRefLiteral", "$s->byref(1)", 0),
    PHP_DENY("too-many-arguments", "noArity", "$s->get(1)", 0),
    PHP_DENY("anonymous-class-has-its-own-this", "noAnonymous", "$this->own()", 1),
    PHP_DENY("static-closure-has-no-this", "noStaticThis", "$this->own()", 2),
};

TEST(resolver_php_source_rules_bind_real_forms_and_refuse_decoys) {
    for (int driver = 0; driver < 2; driver++) {
        const char *root = th_mktempdir("cbm_php_rules");
        ASSERT_NOT_NULL(root);
        char repo[768], database[768];
        snprintf(repo, sizeof(repo), "%s/repo", root);
        snprintf(database, sizeof(database), "%s/index.db", root);
        const RQ_Source sources[] = {
            {.path = PHP_MAIN, .text = PHP_RULES_MAIN},
            {.path = "app/Helpers.php", .text = PHP_RULES_HELPERS},
            {.path = "app/Services/functions.php", .text = PHP_RULES_FUNCTIONS},
            {.path = "app/Support/funcs.php", .text = PHP_RULES_SUPPORT},
            {.path = "app/Other/funcs.php", .text = PHP_RULES_OTHER_NS},
            {.path = "app/Repositories/Repository.php", .text = PHP_RULES_REPOSITORY},
            {.path = "app/Repositories/AlbumRepository.php", .text = PHP_RULES_ALBUM_REPOSITORY},
            {.path = "app/Models/Album.php", .text = PHP_RULES_ALBUM},
            {.path = "app/Models/Status.php", .text = PHP_RULES_STATUS},
            {.path = "app/Builders/AlbumBuilder.php", .text = PHP_RULES_BUILDER},
            {.path = "app/Concerns/Loads.php", .text = PHP_RULES_TRAIT},
            {.path = "app/Services/BaseService.php", .text = PHP_RULES_BASE},
            {.path = "app/Services/Store.php", .text = PHP_RULES_STORE},
            {.path = "app/Services/Other.php", .text = PHP_RULES_OTHER},
            {.path = "app/Services/Built.php", .text = PHP_RULES_BUILT},
            {.path = "app/Services/Magic.php", .text = PHP_RULES_MAGIC},
            {.path = "app/Services/DupA.php", .text = PHP_RULES_DUP_A},
            {.path = "app/Services/DupB.php", .text = PHP_RULES_DUP_B},
        };
        bool ok = cbm_mkdir_p(repo, 0755);
        for (size_t i = 0; ok && i < sizeof(sources) / sizeof(sources[0]); i++) {
            char directory[1024];
            snprintf(directory, sizeof(directory), "%s/%s", repo, sources[i].path);
            *strrchr(directory, '/') = '\0';
            ok = cbm_mkdir_p(directory, 0755) &&
                 th_write_file(TH_PATH(repo, sources[i].path), sources[i].text) == 0;
        }
        for (int i = 0; ok && i < 55; i++) {
            char path[32];
            snprintf(path, sizeof(path), "Filler%02d.php", i);
            ok = th_write_file(TH_PATH(repo, path), "<?php\n// unrelated source file\n") == 0;
        }
        const char *old_workers = getenv("CBM_WORKERS");
        char *saved_workers = old_workers ? strdup(old_workers) : NULL;
        cbm_setenv("CBM_WORKERS", driver ? "4" : "1", 1);
        cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
        ok = pipeline && cbm_pipeline_set_project_name(pipeline, "php-rules") &&
             cbm_pipeline_run(pipeline) == 0;
        cbm_pipeline_free(pipeline);
        if (saved_workers) {
            cbm_setenv("CBM_WORKERS", saved_workers, 1);
            free(saved_workers);
        } else
            cbm_unsetenv("CBM_WORKERS");
        cbm_store_t *store = ok ? cbm_store_open_path_existing(database) : NULL;
        RQ_Report report = {0};
        ok = store &&
             rq_check(store, "php-rules", "php-source-rules", sources,
                      sizeof(sources) / sizeof(sources[0]), PHP_RULES_EXPECTED,
                      sizeof(PHP_RULES_EXPECTED) / sizeof(PHP_RULES_EXPECTED[0]),
                      PHP_RULES_FORBIDDEN,
                      sizeof(PHP_RULES_FORBIDDEN) / sizeof(PHP_RULES_FORBIDDEN[0]), &report,
                      stderr) &&
             ok;
        if (store)
            cbm_store_close(store);
        th_cleanup(root);
        ASSERT(ok);
        ASSERT_EQ(report.missing + report.wrong, 0);
    }
    PASS();
}

SUITE(resolver_php) {
    RUN_TEST(resolver_php_source_rules_bind_real_forms_and_refuse_decoys);
    RUN_TEST(resolver_php_untyped_arguments_and_lexical_this_require_proofs);
    RUN_TEST(resolver_php_import_alias_does_not_escape_its_namespace_block);
    RUN_TEST(resolver_php_declared_receiver_and_static_call_have_proof);
    RUN_TEST(resolver_php_unknown_writes_references_and_symbol_effects_abstain);
    RUN_TEST(resolver_php_private_magic_and_duplicate_method_domains_abstain);
}
