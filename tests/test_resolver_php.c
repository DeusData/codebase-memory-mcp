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
                .declaration_text = " public function typed(int $value) {}"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "php-typed-parameter",
     .why = "Typed parameter applicability and PHP coercion modes are not "
            "audited.",
     .tried = "An exact integer literal is valid; the bridge admits only "
              "audited untyped by-value parameters."},
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
                .declaration_text = " public function defaulted($value = 1) {}"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "php-default-parameter",
     .why = "Defaulted parameter applicability remains unsupported.",
     .tried = "The zero-argument call is genuine, but the fixed-arity "
              "declaration fact excludes defaults."},
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
                .declaration_text = " public function take($value) {}"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "php-named-parameter",
     .why = "Named argument matching is not audited.",
     .tried = "The actual parameter name is value; only positional arguments "
              "have a complete binding proof."},
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
                .declaration_text = " public function byref(&$value) {}"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "php-reference-parameter",
     .why = "Reference binding and effects remain unsupported.",
     .tried = "A variable can bind by reference, but the source flag "
              "deliberately excludes reference parameters."},
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
                .declaration_text = " public function variadic(...$values) {}"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "php-variadic-parameter",
     .why = "Variadic applicability remains unsupported.",
     .tried = "The single positional value is valid; variadic declarations are "
              "not fixed-arity facts."},
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
                .declaration_text = " public function take($value) {}"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "php-argument-alias-call",
     .why = "A receiver also used as an argument is conservatively invalidated.",
     .tried = "The by-value target is genuine; retaining whole-function argument "
              "alias invalidation avoids borrowing stale receiver facts."},
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
                .declaration_text = " public function go() {}"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "php-argument-alias-later",
     .why = "A receiver used as an argument has no retained later fact.",
     .tried = "This later zero-argument call is genuine, but alias-effect "
              "handling remains conservative."},
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
                .declaration_text = " public function helper($x) { return $x + 1; }"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "php-same-file-namespace-type",
     .why = "Ordinary local type names need namespace binding evidence.",
     .tried = "The same-file Calc annotation is genuine; only lexical this and "
              "proven imported type names are bound."},
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
                .declaration_text = " public function helper($x) { return $x + 1; }"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "php-inherited-this",
     .why = "Inherited lexical this lookup remains unsupported.",
     .tried = "The base helper is genuine, but the new lexical proof requires "
              "a direct class domain without inheritance."},
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
        ASSERT_EQ(report.genuine, 20);
        ASSERT_EQ(report.found, 10);
        ASSERT_EQ(report.missing, 10);
        ASSERT_EQ(report.required_missing + report.wrong, 0);
    }
    PASS();
}

SUITE(resolver_php) {
    RUN_TEST(resolver_php_untyped_arguments_and_lexical_this_require_proofs);
    RUN_TEST(resolver_php_import_alias_does_not_escape_its_namespace_block);
    RUN_TEST(resolver_php_declared_receiver_and_static_call_have_proof);
    RUN_TEST(resolver_php_unknown_writes_references_and_symbol_effects_abstain);
    RUN_TEST(resolver_php_private_magic_and_duplicate_method_domains_abstain);
}
