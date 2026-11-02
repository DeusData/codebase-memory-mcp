#include "test_framework.h"
#include "test_helpers.h"
#include "pipeline/pipeline.h"
#include "store/store.h"
#include "sqlite3.h"
#include "cbm.h"
#include "lsp/rust_proof.h"
#include "lsp/rust_lsp.h"
#include "resolve/binding.h"
#include "resolve/policy.h"
#include "resolver_quality.h"
static const char RUST_SERVICE[] = "pub struct Service;\nimpl Service {\n pub fn go(&self) {}\n}\n";
static int rust_type_rows(const char *source, const char *library, int duplicate,
                          CBMResolvedKind kind) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMFileResult *lib = cbm_extract_file(library, (int)strlen(library), CBM_LANG_RUST, "p",
                                          "lib.rs", 0, NULL, NULL);
    CBMFileResult *file =
        cbm_extract_file(source, (int)strlen(source), CBM_LANG_RUST, "p", "main.rs", 0, NULL, NULL);
    CBMLSPDef defs[3] = {{.qualified_name = "p.lib.Service",
                          .graph_qualified_name = "p.lib.Service",
                          .short_name = "Service",
                          .label = "Struct",
                          .source_start_line = 1,
                          .declaration_identity = "lib:0",
                          .def_module_qn = "p.lib",
                          .lang = CBM_LANG_RUST},
                         {.qualified_name = "p.lib.Service.go",
                          .graph_qualified_name = "p.lib.Service.go",
                          .short_name = "go",
                          .label = "Method",
                          .receiver_type = "p.lib.Service",
                          .source_start_line = 3,
                          .declaration_identity = "lib:1",
                          .def_module_qn = "p.lib",
                          .lang = CBM_LANG_RUST}};
    bool facts =
        cbm_rust_collect_source_facts(&arena, library, (int)strlen(library), "p.lib", defs, 2);
    int count = 2;
    if (duplicate) {
        defs[2] = defs[duplicate == 1 ? 0 : 1];
        defs[2].declaration_identity = "other:0";
        count = 3;
    }
    CBMTypeRegistry *registry = cbm_rust_build_cross_registry(&arena, defs, count);
    const char *names[] = {"Svc"}, *targets[] = {"p.lib.Service"};
    CBMResolvedCallArray out = {0};
    if (facts && registry && file)
        cbm_rust_proof_resolve(&arena, source, (int)strlen(source), registry, file, names, targets,
                               1, &out);
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

TEST(resolver_rust_shared_reference_and_alias) {
    const char *source = "use crate::lib::Service as Svc;\npub fn run(value: &Svc) { value.go(); "
                         "let alias=value; alias.go(); }\n";
    ASSERT_EQ(rust_type_rows(source, RUST_SERVICE, 0, CBM_RESOLVED_INVOCATION), 2);
    ASSERT_EQ(rust_type_rows(source, RUST_SERVICE, 0, CBM_RESOLVED_TYPE_REFERENCE), 1);
    PASS();
}
TEST(resolver_rust_receiver_shadow_and_unsupported_flow_abstain) {
    const char *sources[] = {
        ("use crate::lib::Service as Svc;\npub fn run(value: &Svc) { let value=other; value.go(); "
         "}\n"),
        ("use crate::lib::Service as Svc;\npub fn run(value: &Svc) { value=other; value.go(); }\n"),
        ("use crate::lib::Service as Svc;\npub fn run(value: &Svc) { value.go(1); }\n"),
        ("use crate::lib::Service as Svc;\npub fn run(value: &mut Svc) { value.go(); }\n"),
        ("use crate::lib::Service as Svc;\npub fn run(value: Svc) { value.go(); }\n"),
        ("use crate::lib::Service as Svc;\npub fn run<Svc>(value: &Svc) { value.go(); }\n"),
        ("use crate::lib::Service as Svc;\npub fn run(value: &Svc) { unknown!(); value.go(); }\n"),
        ("use crate::lib::Service as Svc;\npub fn run(value: &Svc) { if cond { value=other; } "
         "value.go(); }\n")};
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        int count = rust_type_rows(sources[i], RUST_SERVICE, 0, CBM_RESOLVED_INVOCATION);
        if (count)
            fprintf(stderr, "Rust unexpected proof %zu: %d\n", i, count);
        ASSERT_EQ(count, 0);
    }
    PASS();
}
TEST(resolver_rust_visibility_traits_attributes_and_duplicates_abstain) {
    const char *source =
        "use crate::lib::Service as Svc;\npub fn run(value: &Svc) { value.go(); }\n";
    const char *libraries[] = {
        ("struct Service;\nimpl Service {\n pub fn go(&self) {}\n}\n"),
        ("pub struct Service;\nimpl Service {\n fn go(&self) {}\n}\n"),
        ("pub struct Service;\nimpl Other for Service {\n fn go(&self) {}\n}\n"),
        ("pub struct Service;\nimpl Service {\n pub fn go(&mut self) {}\n}\n"),
        ("pub struct Service;\nimpl Service {\n pub fn go<T>(&self) {}\n}\n"),
        ("#[cfg(feature=\"other\")]\npub struct Service;\nimpl Service { pub fn go(&self) {} }\n")};
    for (size_t i = 0; i < sizeof(libraries) / sizeof(libraries[0]); i++)
        ASSERT_EQ(rust_type_rows(source, libraries[i], 0, CBM_RESOLVED_INVOCATION), 0);
    ASSERT_EQ(rust_type_rows(source, RUST_SERVICE, 1, CBM_RESOLVED_INVOCATION), 0);
    ASSERT_EQ(rust_type_rows(source, RUST_SERVICE, 2, CBM_RESOLVED_INVOCATION), 0);
    PASS();
}

typedef struct {
    const char *path, *text;
} rust_proof_file_t;

static int rust_proof_count(const rust_proof_file_t *files, size_t count, const char *sql,
                            const char *target, const char *name, const char *label) {
    const char *root = th_mktempdir("cbm_rust_proof");
    if (!root)
        return -1;
    char repo[768], database[768];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    bool ok = cbm_mkdir_p(repo, 0755) && cbm_mkdir_p(TH_PATH(repo, "src"), 0755);
    for (size_t i = 0; ok && i < count; i++)
        ok = th_write_file(TH_PATH(repo, files[i].path), files[i].text) == 0;
    cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
    int rc = pipeline && cbm_pipeline_set_project_name(pipeline, "python-proof")
                 ? cbm_pipeline_run(pipeline)
                 : -1;
    cbm_pipeline_free(pipeline);
    cbm_store_t *store = rc == 0 ? cbm_store_open_path_existing(database) : NULL;
    sqlite3_stmt *statement = NULL;
    int edges = -1;
    if (store &&
        sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &statement, NULL) == SQLITE_OK) {
        const char *parameters[] = {target, name, label};
        for (int i = 0; i < sqlite3_bind_parameter_count(statement); i++)
            sqlite3_bind_text(statement, i + 1, parameters[i], -1, SQLITE_STATIC);
        if (sqlite3_step(statement) == SQLITE_ROW)
            edges = sqlite3_column_int(statement, 0);
    }
    sqlite3_finalize(statement);
    if (store)
        cbm_store_close(store);
    th_cleanup(root);
    return edges;
}

static int rust_proof_edges(const rust_proof_file_t *files, size_t count, const char *target,
                            const char *name, const char *label) {
    return rust_proof_count(
        files, count,
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
        "JOIN nodes t ON t.id=e.target_id WHERE e.type='CALLS' AND s.file_path='src/main.rs' "
        "AND t.file_path=?1 AND t.name=?2 AND t.label=?3",
        target, name, label);
}

/* Check the exact symbol the import donated, before a language resolver can
 * conceal incorrect binding evidence by rejecting a later call. */
static int rust_proof_bindings(const rust_proof_file_t *files, size_t count, const char *target,
                               const char *name, const char *label) {
    return rust_proof_count(
        files, count,
        "SELECT count(*) FROM nodes s, json_each(s.properties, '$.rust_import_bindings') b "
        "JOIN nodes t ON t.qualified_name=json_extract(b.value, '$.target') "
        "WHERE s.label='File' AND s.file_path='src/main.rs' "
        "AND t.file_path=?1 AND t.name=?2 AND t.label=?3",
        target, name, label);
}

static int rust_proof_imports(const rust_proof_file_t *files, size_t count, const char *target) {
    return rust_proof_count(files, count,
                            "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
                            "JOIN nodes t ON t.id=e.target_id WHERE e.type='IMPORTS' "
                            "AND s.label='File' AND s.file_path='src/main.rs' "
                            "AND t.label='File' AND t.file_path=?1",
                            target, NULL, NULL);
}

TEST(resolver_rust_import_alias_preserves_binding_name) {
    const char *source = "use crate::service::Service as Svc;\n";
    CBMFileResult *file =
        cbm_extract_file(source, (int)strlen(source), CBM_LANG_RUST, "p", "main.rs", 0, NULL, NULL);
    ASSERT_NOT_NULL(file);
    bool exact = file->imports.count == 1 && !strcmp(file->imports.items[0].local_name, "Svc");
    cbm_free_result(file);
    ASSERT(exact);
    PASS();
}

TEST(resolver_rust_pipeline_keeps_exact_method_and_rejects_module_alias) {
    const rust_proof_file_t files[] = {
        {"src/service.rs", RUST_SERVICE},
        {"src/decoy.rs", RUST_SERVICE},
        {"src/main.rs", "mod service; mod decoy;\nuse crate::service::Service as Svc;\npub fn "
                        "run(value: &Svc) { value.go(); }\nfn main() {}\n"}};
    ASSERT_EQ(rust_proof_edges(files, 3, "src/service.rs", "go", "Method"), 1);
    ASSERT_EQ(rust_proof_edges(files, 3, "src/decoy.rs", "go", "Method"), 0);
    const rust_proof_file_t module[] = {
        {"src/service.rs", RUST_SERVICE},
        {"src/main.rs", "mod service;\nuse crate::service as Svc;\npub fn run(value: &Svc) { "
                        "value.go(); }\nfn main() {}\n"}};
    ASSERT_EQ(rust_proof_edges(module, 2, "src/service.rs", "go", "Method"), 0);
    PASS();
}
TEST(resolver_rust_declared_module_and_use_keep_exact_target) {
    const rust_proof_file_t files[] = {{"src/actual.rs", "pub fn target() {}\n"},
                                       {"src/decoy.rs", "pub fn target() {}\n"},
                                       {"src/main.rs",
                                        "mod actual;\nuse crate::actual::target;\n"
                                        "fn caller() { target(); }\nfn main() {}\n"}};
    ASSERT(rust_proof_imports(files, 3, "src/actual.rs") > 0);
    ASSERT_EQ(rust_proof_imports(files, 3, "src/decoy.rs"), 0);
    ASSERT_EQ(rust_proof_bindings(files, 3, "src/actual.rs", "target", "Function"), 1);
    ASSERT_EQ(rust_proof_bindings(files, 3, "src/decoy.rs", "target", "Function"), 0);
    ASSERT_EQ(rust_proof_edges(files, 3, "src/actual.rs", "target", "Function"), 1);
    ASSERT_EQ(rust_proof_edges(files, 3, "src/decoy.rs", "target", "Function"), 0);
    PASS();
}

TEST(resolver_rust_inline_module_does_not_bind_top_level_namesake) {
    const char *sources[] = {"pub struct Service;\n"
                             "mod inner { pub struct Service; }\n"
                             "use crate::inner::Service as Svc;\n"
                             "fn consume(value: &Svc) {}\nfn main() {}\n",
                             "pub struct Service;\n"
                             "mod inner { pub struct Service; use self::Service as Svc;\n"
                             "pub fn consume(value: &Svc) {} }\nfn main() {}\n"};
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        const rust_proof_file_t files[] = {{"src/main.rs", sources[i]}};
        int bindings = rust_proof_count(
            files, 1,
            "SELECT count(*) FROM nodes s, json_each(s.properties, '$.rust_import_bindings') b "
            "JOIN nodes t ON t.qualified_name=json_extract(b.value, '$.target') "
            "WHERE s.label='File' AND s.file_path='src/main.rs' AND t.start_line=1 "
            "AND t.file_path=?1 AND t.name=?2 AND t.label=?3",
            "src/main.rs", "Service", "Struct");
        if (bindings)
            fprintf(stderr, "Rust inline import binding %zu: %d\n", i, bindings);
        ASSERT_EQ(bindings, 0);
    }
    PASS();
}

/* An attribute or macro next to a written item does not void that item: rustc rejects a
 * second definition of one name in one namespace, so the written item is what the path
 * names. This test used to pin abstention for these forms; that abstention is the gate
 * that voided real files. A name only a macro could have produced stays unproved. */
TEST(resolver_rust_attributes_and_macros_keep_an_explicit_item) {
    const char *libraries[] = {"#[cfg(feature=\"alternate\")]\npub fn target() {}\n",
                               "#![cfg(feature=\"alternate\")]\npub fn target() {}\n",
                               "#[transform]\npub fn target() {}\n",
                               "macro_rules! influence { () => {} }\npub fn target() {}\n",
                               "influence!();\npub fn target() {}\n"};
    for (size_t i = 0; i < sizeof(libraries) / sizeof(libraries[0]); i++) {
        const rust_proof_file_t files[] = {{"src/actual.rs", libraries[i]},
                                           {"src/main.rs",
                                            "mod actual;\nuse crate::actual::target;\n"
                                            "fn caller() { target(); }\nfn main() {}\n"}};
        ASSERT_EQ(rust_proof_bindings(files, 2, "src/actual.rs", "target", "Function"), 1);
        ASSERT_EQ(rust_proof_edges(files, 2, "src/actual.rs", "target", "Function"), 1);
    }
    const rust_proof_file_t generated[] = {{"src/actual.rs", "generate_target!();\n"},
                                           {"src/main.rs",
                                            "mod actual;\nuse crate::actual::target;\n"
                                            "fn caller() { target(); }\nfn main() {}\n"}};
    ASSERT_EQ(rust_proof_bindings(generated, 2, "src/actual.rs", "target", "Function"), 0);
    ASSERT_EQ(rust_proof_edges(generated, 2, "src/actual.rs", "target", "Function"), 0);
    /* The use compiled, so the module's macro made the name: the file is still a dependency. */
    ASSERT(rust_proof_imports(generated, 2, "src/actual.rs") > 0);
    const rust_proof_file_t absent[] = {{"src/actual.rs", "pub fn other() {}\n"},
                                        {"src/main.rs",
                                         "mod actual;\nuse crate::actual::target;\nfn main() {}\n"}};
    /* Without a macro nothing can supply the leaf. Only `mod actual;` depends on the file. */
    ASSERT_EQ(rust_proof_imports(absent, 2, "src/actual.rs"), 1);
    PASS();
}

static const char RUST_PRIMITIVE_MAIN[] =
    "mod service;\n"
    "mod decoy;\n"
    "mod shadow;\n"
    "use crate::service::Service as Svc;\n"
    "pub fn run(value: &Svc, x: i32, b: bool) {\n"
    " value.take(x);\n"
    " value.take(7);\n"
    " value.take(7i32);\n"
    " value.flag(b);\n"
    " value.flag(true);\n"
    " let alias=x;\n"
    " value.take(alias);\n"
    " value.small(-128);\n"
    " value.big(1u64);\n"
    "}\n"
    "pub fn bad(value: &Svc, wide: i64) {\n"
    " value.take(wide); value.take(7i64); value.small(128); value.big(-1); value.flag(1); "
    "value.hidden(1);\n"
    "}\n"
    "pub fn gaps(value: &Svc) { value.float(1.0f32); let local=7; value.take(local); "
    "value.generic(1); }\n"
    "pub fn owned(value: Svc) { value.take(1); }\n"
    "pub fn mutable(value: &mut Svc) { value.take(2); }\n"
    "pub fn nested(value: &Svc, x: i32) { { let x=1i64; value.take(x); } }\n"
    "struct Calc { base: i32 }\n"
    "impl Calc {\n"
    " fn helper(&self, x: i32) -> i32 { self.base + x }\n"
    " fn run(&self, y: i32) -> i32 { self.helper(y) }\n"
    "}\n"
    "fn local(value: &Calc, x: i32) { value.helper(x); }\n"
    "fn owned_local() { let c=Calc { base: 1 }; c.run(2); }\n"
    "fn main() {}\n";

static const char RUST_PRIMITIVE_SERVICE[] = "pub struct Service;\n"
                                             "impl Service {\n"
                                             " pub fn take(&self, value: i32) {}\n"
                                             " pub fn flag(&self, value: bool) {}\n"
                                             " pub fn small(&self, value: i8) {}\n"
                                             " pub fn big(&self, value: u64) {}\n"
                                             " pub fn float(&self, value: f32) {}\n"
                                             " pub fn generic<T>(&self, value: T) {}\n"
                                             " fn hidden(&self, value: i32) {}\n"
                                             "}\n";

static const char RUST_PRIMITIVE_DECOY[] = "pub struct Service;\n"
                                           "impl Service {\n"
                                           " pub fn take(&self, value: i32) {}\n"
                                           "}\n";

static const char RUST_PRIMITIVE_SHADOW[] =
    "use crate::service::Service;\n"
    "type i32 = i64;\n"
    "pub fn shadow(value: &Service, x: i32) { value.take(x); }\n";

static const RQ_Expected RUST_PRIMITIVE_EXPECTED[] = {
    {.case_id = "primitive-parameter",
     .site = {.file = "src/main.rs", .caller = "run", .needle = "value.take(x)"},
     .relation = "CALLS",
     .target = {.file = "src/service.rs",
                .suffix = "Service.take",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " pub fn take(&self, value: i32) {}"}},
    {.case_id = "contextual-int-literal",
     .site = {.file = "src/main.rs", .caller = "run", .needle = "value.take(7)"},
     .relation = "CALLS",
     .target = {.file = "src/service.rs",
                .suffix = "Service.take",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " pub fn take(&self, value: i32) {}"}},
    {.case_id = "explicit-int-literal",
     .site = {.file = "src/main.rs", .caller = "run", .needle = "value.take(7i32)"},
     .relation = "CALLS",
     .target = {.file = "src/service.rs",
                .suffix = "Service.take",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " pub fn take(&self, value: i32) {}"}},
    {.case_id = "boolean-parameter",
     .site = {.file = "src/main.rs", .caller = "run", .needle = "value.flag(b)"},
     .relation = "CALLS",
     .target = {.file = "src/service.rs",
                .suffix = "Service.flag",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = " pub fn flag(&self, value: bool) {}"}},
    {.case_id = "boolean-literal",
     .site = {.file = "src/main.rs", .caller = "run", .needle = "value.flag(true)"},
     .relation = "CALLS",
     .target = {.file = "src/service.rs",
                .suffix = "Service.flag",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = " pub fn flag(&self, value: bool) {}"}},
    {.case_id = "typed-parameter-alias",
     .site = {.file = "src/main.rs", .caller = "run", .needle = "value.take(alias)"},
     .relation = "CALLS",
     .target = {.file = "src/service.rs",
                .suffix = "Service.take",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " pub fn take(&self, value: i32) {}"}},
    {.case_id = "contextual-signed-boundary",
     .site = {.file = "src/main.rs", .caller = "run", .needle = "value.small(-128)"},
     .relation = "CALLS",
     .target = {.file = "src/service.rs",
                .suffix = "Service.small",
                .label = "Method",
                .declaration_line = 5,
                .declaration_text = " pub fn small(&self, value: i8) {}"}},
    {.case_id = "explicit-unsigned-literal",
     .site = {.file = "src/main.rs", .caller = "run", .needle = "value.big(1u64)"},
     .relation = "CALLS",
     .target = {.file = "src/service.rs",
                .suffix = "Service.big",
                .label = "Method",
                .declaration_line = 6,
                .declaration_text = " pub fn big(&self, value: u64) {}"}},
    {.case_id = "private-lexical-self",
     .site = {.file = "src/main.rs", .caller = "Calc.run", .needle = "self.helper(y)"},
     .relation = "CALLS",
     .target = {.file = "src/main.rs",
                .suffix = "Calc.helper",
                .label = "Method",
                .declaration_line = 25,
                .declaration_text = " fn helper(&self, x: i32) -> i32 { self.base + x }"}},
    {.case_id = "private-same-module-reference",
     .site = {.file = "src/main.rs", .caller = "local", .needle = "value.helper(x)"},
     .relation = "CALLS",
     .target = {.file = "src/main.rs",
                .suffix = "Calc.helper",
                .label = "Method",
                .declaration_line = 25,
                .declaration_text = " fn helper(&self, x: i32) -> i32 { self.base + x }"}},
    {.case_id = "float-literal",
     .site = {.file = "src/main.rs", .caller = "gaps", .needle = "value.float(1.0f32)"},
     .relation = "CALLS",
     .target = {.file = "src/service.rs",
                .suffix = "Service.float",
                .label = "Method",
                .declaration_line = 7,
                .declaration_text = " pub fn float(&self, value: f32) {}"}},
    {.case_id = "local-literal-inference",
     .site = {.file = "src/main.rs", .caller = "gaps", .needle = "value.take(local)"},
     .relation = "CALLS",
     .target = {.file = "src/service.rs",
                .suffix = "Service.take",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " pub fn take(&self, value: i32) {}"}},
    {.case_id = "generic-inference",
     .site = {.file = "src/main.rs", .caller = "gaps", .needle = "value.generic(1)"},
     .relation = "CALLS",
     .target = {.file = "src/service.rs",
                .suffix = "Service.generic",
                .label = "Method",
                .declaration_line = 8,
                .declaration_text = " pub fn generic<T>(&self, value: T) {}"}},
    {.case_id = "owned-receiver",
     .site = {.file = "src/main.rs", .caller = "owned", .needle = "value.take(1)"},
     .relation = "CALLS",
     .target = {.file = "src/service.rs",
                .suffix = "Service.take",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " pub fn take(&self, value: i32) {}"}},
    {.case_id = "mutable-receiver",
     .site = {.file = "src/main.rs", .caller = "mutable", .needle = "value.take(2)"},
     .relation = "CALLS",
     .target = {.file = "src/service.rs",
                .suffix = "Service.take",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " pub fn take(&self, value: i32) {}"}},
    {.case_id = "constructor-receiver",
     .site = {.file = "src/main.rs", .caller = "owned_local", .needle = "c.run(2)"},
     .relation = "CALLS",
     .target = {.file = "src/main.rs",
                .suffix = "Calc.run",
                .label = "Method",
                .declaration_line = 26,
                .declaration_text = " fn run(&self, y: i32) -> i32 { self.helper(y) }"}},
};

static const RQ_Forbidden RUST_PRIMITIVE_FORBIDDEN[] = {
    {.case_id = "no-numeric-widening",
     .site = {.file = "src/main.rs",
              .caller = "bad",
              .needle = "value.take(wide)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "wrong-explicit-suffix",
     .site = {.file = "src/main.rs",
              .caller = "bad",
              .needle = "value.take(7i64)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "out-of-range-contextual-literal",
     .site = {.file = "src/main.rs",
              .caller = "bad",
              .needle = "value.small(128)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "negative-unsigned",
     .site =
         {.file = "src/main.rs", .caller = "bad", .needle = "value.big(-1)", .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "integer-is-not-boolean",
     .site =
         {.file = "src/main.rs", .caller = "bad", .needle = "value.flag(1)", .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "private-method-outside-module",
     .site = {.file = "src/main.rs",
              .caller = "bad",
              .needle = "value.hidden(1)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "nested-shadow-not-outer-type",
     .site = {.file = "src/main.rs",
              .caller = "nested",
              .needle = "value.take(x)",
              .occurrence_index = 1},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "primitive-name-shadowed",
     .site = {.file = "src/shadow.rs",
              .caller = "shadow",
              .needle = "value.take(x)",
              .occurrence_index = 0},
     .relation = "CALLS",
     .any_target = true},
};

TEST(resolver_rust_primitive_arguments_and_private_self_require_proofs) {
    for (int driver = 0; driver < 2; driver++) {
        const char *root = th_mktempdir("cbm_rust_primitive");
        ASSERT_NOT_NULL(root);
        char repo[768], database[768];
        snprintf(repo, sizeof(repo), "%s/repo", root);
        snprintf(database, sizeof(database), "%s/index.db", root);
        const RQ_Source sources[] = {{.path = "src/main.rs", .text = RUST_PRIMITIVE_MAIN},
                                     {.path = "src/service.rs", .text = RUST_PRIMITIVE_SERVICE},
                                     {.path = "src/decoy.rs", .text = RUST_PRIMITIVE_DECOY},
                                     {.path = "src/shadow.rs", .text = RUST_PRIMITIVE_SHADOW}};
        bool ok = cbm_mkdir_p(repo, 0755) && cbm_mkdir_p(TH_PATH(repo, "src"), 0755);
        for (size_t i = 0; ok && i < sizeof(sources) / sizeof(sources[0]); i++)
            ok = th_write_file(TH_PATH(repo, sources[i].path), sources[i].text) == 0;
        for (int i = 0; ok && i < 55; i++) {
            char path[32];
            snprintf(path, sizeof(path), "Filler%02d.rs", i);
            ok = th_write_file(TH_PATH(repo, path), "// unrelated source file\n") == 0;
        }
        const char *old_workers = getenv("CBM_WORKERS");
        char *saved_workers = old_workers ? strdup(old_workers) : NULL;
        ok = ok && (!old_workers || saved_workers);
        cbm_setenv("CBM_WORKERS", driver ? "4" : "1", 1);
        cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
        ok = pipeline && cbm_pipeline_set_project_name(pipeline, "rust-direct") &&
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
             rq_check(store, "rust-direct", "primitive-rust-members", sources,
                      sizeof(sources) / sizeof(sources[0]), RUST_PRIMITIVE_EXPECTED,
                      sizeof(RUST_PRIMITIVE_EXPECTED) / sizeof(RUST_PRIMITIVE_EXPECTED[0]),
                      RUST_PRIMITIVE_FORBIDDEN,
                      sizeof(RUST_PRIMITIVE_FORBIDDEN) / sizeof(RUST_PRIMITIVE_FORBIDDEN[0]),
                      &report, stderr) &&
             ok;
        if (store)
            cbm_store_close(store);
        th_cleanup(root);
        ASSERT(ok);
        ASSERT_EQ(report.genuine, 16);
        ASSERT_EQ(report.found, 16);
        ASSERT_EQ(report.missing, 0);
        ASSERT_EQ(report.required_missing + report.wrong, 0);
    }
    PASS();
}

TEST(resolver_rust_local_impl_preserves_import_domain) {
    const char *sources[] = {RUST_PRIMITIVE_MAIN,
                             "use crate::service::Service as Svc;\nstruct Local<T>(T);\n"
                             "impl<T> Local<T> { fn local(&self, value: T) {} }\n",
                             "use crate::service::Service as Svc;\nstruct Local;\n"
                             "impl Local { generate!(); }\n",
                             "use crate::service::Service as Svc;\nstruct Local;\n"
                             "#[cfg(feature=\"alternate\")] impl Local { fn local(&self) {} }\n",
                             "use crate::service::Service as Svc;\nstruct Local;\n"
                             "trait Dynamic { fn dynamic(&self); }\n"
                             "impl Dynamic for Local { fn dynamic(&self) {} }\n",
                             "use crate::service::Service as Svc;\nstruct Local;\n"
                             "impl Local { fn local(&self) where Self: Sized {} }\n"};
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        const char *source = sources[i];
        CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_RUST,
                                               "rust-domain", "src/main.rs", 0, NULL, NULL);
        ASSERT_NOT_NULL(file);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *bindings =
            cbm_resolve_binding_build(&arena, file, CBM_LANG_RUST, source, strlen(source));
        bool imported = cbm_resolve_binding_import(bindings, "Svc");
        bool parsed = !file->has_error && !file->parse_incomplete && !file->parse_unusable;
        cbm_arena_destroy(&arena);
        cbm_free_result(file);
        ASSERT(parsed);
        ASSERT_EQ(imported, i == 0);
    }
    PASS();
}

/* ── Rust's own name rules, on the forms real crates use ───────────── */

/* Both drivers: one worker (sequential route) and four with enough files to take
 * the parallel route. The occurrence outcome must not depend on the route. */
static cbm_store_t *rust_workspace_index(const rust_proof_file_t *files, size_t count, int driver,
                                         char **root_out) {
    const char *root = th_mktempdir("cbm_rust_names");
    if (!root)
        return NULL;
    char repo[768], database[768];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    bool ok = cbm_mkdir_p(repo, 0755);
    for (size_t i = 0; ok && i < count; i++) {
        char path[1024];
        snprintf(path, sizeof(path), "%s/%s", repo, files[i].path);
        char *slash = strrchr(path, '/');
        *slash = '\0';
        ok = cbm_mkdir_p(path, 0755);
        *slash = '/';
        ok = ok && th_write_file(path, files[i].text) == 0;
    }
    for (int i = 0; ok && driver && i < 55; i++) {
        char path[1024];
        snprintf(path, sizeof(path), "%s/Filler%02d.rs", repo, i);
        ok = th_write_file(path, "// unrelated source file\n") == 0;
    }
    const char *old_workers = getenv("CBM_WORKERS");
    char *saved_workers = old_workers ? strdup(old_workers) : NULL;
    ok = ok && (!old_workers || saved_workers);
    cbm_setenv("CBM_WORKERS", driver ? "4" : "1", 1);
    cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
    ok = pipeline && cbm_pipeline_set_project_name(pipeline, "rs") && cbm_pipeline_run(pipeline) == 0;
    cbm_pipeline_free(pipeline);
    if (saved_workers) {
        cbm_setenv("CBM_WORKERS", saved_workers, 1);
        free(saved_workers);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }
    cbm_store_t *store = ok ? cbm_store_open_path_existing(database) : NULL;
    *root_out = strdup(root);
    return store;
}

static void rust_workspace_done(cbm_store_t *store, char *root) {
    if (store)
        cbm_store_close(store);
    if (root)
        th_cleanup(root);
    free(root);
}

/* Exact endpoints: the project is `rs`, so an endpoint is rs.<path>.<item>. */
static int rust_edge(cbm_store_t *store, const char *relation, const char *source, const char *target) {
    sqlite3_stmt *statement = NULL;
    int count = -1;
    const char *sql = "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
                      "JOIN nodes t ON t.id=e.target_id WHERE e.type=?1 "
                      "AND s.qualified_name='rs.'||?2 AND t.qualified_name='rs.'||?3";
    if (store && sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &statement, NULL) == SQLITE_OK) {
        sqlite3_bind_text(statement, 1, relation, -1, SQLITE_STATIC);
        sqlite3_bind_text(statement, 2, source, -1, SQLITE_STATIC);
        sqlite3_bind_text(statement, 3, target, -1, SQLITE_STATIC);
        if (sqlite3_step(statement) == SQLITE_ROW)
            count = sqlite3_column_int(statement, 0);
    }
    sqlite3_finalize(statement);
    return count;
}

static int rust_edges_from(cbm_store_t *store, const char *relation, const char *source) {
    sqlite3_stmt *statement = NULL;
    int count = -1;
    const char *sql = "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
                      "WHERE e.type=?1 AND s.qualified_name='rs.'||?2";
    if (store && sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &statement, NULL) == SQLITE_OK) {
        sqlite3_bind_text(statement, 1, relation, -1, SQLITE_STATIC);
        sqlite3_bind_text(statement, 2, source, -1, SQLITE_STATIC);
        if (sqlite3_step(statement) == SQLITE_ROW)
            count = sqlite3_column_int(statement, 0);
    }
    sqlite3_finalize(statement);
    return count;
}

/* The outcome the occurrence recorded, whether or not an edge came of it. */
static bool rust_occurrence_reason(cbm_store_t *store, const char *relation, const char *spelling,
                                   const char *reason) {
    sqlite3_stmt *statement = NULL;
    bool found = false;
    const char *sql = "SELECT count(*) FROM resolver_occurrences WHERE relation=?1 AND spelling=?2 "
                      "AND reason=?3 AND emitted=0";
    if (store && sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &statement, NULL) == SQLITE_OK) {
        sqlite3_bind_text(statement, 1, relation, -1, SQLITE_STATIC);
        sqlite3_bind_text(statement, 2, spelling, -1, SQLITE_STATIC);
        sqlite3_bind_text(statement, 3, reason, -1, SQLITE_STATIC);
        found = sqlite3_step(statement) == SQLITE_ROW && sqlite3_column_int(statement, 0) > 0;
    }
    sqlite3_finalize(statement);
    return found;
}

#define RUST_ROUTES(store, root, files)                                                          \
    for (int driver = 0; driver < 2; driver++)                                                   \
        for (char *root = NULL, *once = (char *)1; once; once = NULL)                            \
            for (cbm_store_t *store = rust_workspace_index(files, sizeof(files) / sizeof(files[0]), \
                                                           driver, &root);                       \
                 once; rust_workspace_done(store, root), once = NULL)

/* One grouped import, a derive and a test module used to void the whole file. */
TEST(resolver_rust_same_module_item_ignores_the_files_other_syntax) {
    static const rust_proof_file_t files[] = {
        {"src/other.rs", "pub fn local() -> i32 { 9 }\n"},
        {"src/main.rs",
         "mod other;\n"
         "use std::path::{Path, PathBuf};\n"
         "use other::*;\n"
         "#[derive(Debug, Clone)]\n"
         "pub struct Config { pub root: PathBuf }\n"
         "macro_rules! noop { () => {} }\n"
         "noop!();\n"
         "fn local() -> i32 { 1 }\n"
         "fn caller() -> i32 { local() }\n"
         "fn shadowed() -> i32 { let local = |x: i32| x; local(1) }\n"
         "#[cfg(test)]\n"
         "mod tests { use super::*; #[test] fn inner() { caller(); } }\n"
         "fn main() { caller(); shadowed(); }\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        /* A local item shadows the glob: it cannot be other::local. */
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.main.local"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.other.local"), 0);
        /* A closure binding hides the item for the rest of its block. */
        ASSERT_EQ(rust_edges_from(store, "CALLS", "src.main.shadowed"), 0);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.main", "src.main.caller"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.inner", "src.main.caller"), 1);
    }
    PASS();
}

/* serde_json::{json, Value} used to become one binding literally named `{json, Value}`. */
TEST(resolver_rust_use_tree_binds_one_name_per_leaf) {
    static const rust_proof_file_t files[] = {
        {"Cargo.toml", "[package]\nname = \"demo\"\nedition = \"2021\"\n[dependencies]\nserde_json = \"1\"\n"},
        {"src/lib.rs", "pub mod a;\npub mod glob_me;\n"},
        {"src/a/mod.rs", "pub mod e;\npub fn b() {}\npub fn c() {}\n"},
        {"src/a/e.rs", "pub fn f() {}\npub fn g() {}\n"},
        {"src/glob_me.rs", "pub fn from_glob() {}\n"},
        {"src/main.rs",
         "use demo::a::{b, c as renamed, e::{self, f}};\n"
         "use demo::glob_me::*;\n"
         "use serde_json::{json, Value};\n"
         "use std::collections::HashMap as Map;\n"
         "fn caller(_v: Value, _m: Map<i32, i32>) {\n"
         "    b();\n"
         "    renamed();\n"
         "    f();\n"
         "    e::g();\n"
         "    from_glob();\n"
         "    let _ = json!(1);\n"
         "}\n"
         "fn main() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.a.mod.b"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.a.mod.c"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.a.e.f"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.a.e.g"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.glob_me.from_glob"), 1);
        ASSERT(rust_edge(store, "IMPORTS", "src.main.rs.__file__", "src.a.mod.rs.__file__") >= 1);
        ASSERT(rust_edge(store, "IMPORTS", "src.main.rs.__file__", "src.a.e.rs.__file__") >= 1);
        ASSERT(rust_edge(store, "IMPORTS", "src.main.rs.__file__", "src.glob_me.rs.__file__") >= 1);
        /* The renamed leaf binds its alias, not the original name. */
        ASSERT(rust_occurrence_reason(store, "IMPORTS", "serde_json::Value",
                                      "rust_external_crate_import"));
        ASSERT(rust_occurrence_reason(store, "IMPORTS", "std::collections::HashMap",
                                      "rust_external_crate_import"));
    }
    PASS();
}

/* Cargo decides crate names: lib-core is `lib_core`, and only a dependency is reachable. */
TEST(resolver_rust_workspace_dependency_names_come_from_cargo) {
    static const rust_proof_file_t files[] = {
        {"Cargo.toml",
         "[workspace]\nmembers = [\"app\", \"lib-core\", \"unrelated\"]\n"
         "[workspace.package]\nedition = \"2021\"\n"
         "[workspace.dependencies]\nlib-core = { path = \"lib-core\" }\n"},
        {"lib-core/Cargo.toml", "[package]\nname = \"lib-core\"\nedition.workspace = true\n"},
        {"named-lib/Cargo.toml", "[package]\nname = \"named-lib\"\nedition = \"2021\"\n[lib]\nname = \"short\"\n"},
        {"named-lib/src/lib.rs", "pub fn in_named() {}\n"},
        {"lib-core/src/lib.rs", "pub mod store;\npub use store::Store;\n"},
        {"lib-core/src/store.rs",
         "pub struct Store;\nimpl Store {\n pub fn new() -> Store { Store }\n pub fn get(&self) -> i32 { 1 }\n}\n"},
        {"unrelated/Cargo.toml", "[package]\nname = \"unrelated\"\nedition = \"2021\"\n"},
        {"unrelated/src/lib.rs", "pub struct Store;\nimpl Store { pub fn new() -> Store { Store } }\n"},
        {"app/Cargo.toml",
         "[package]\nname = \"app\"\nedition = \"2021\"\n[dependencies]\nlib-core.workspace = true\n"
         "renamed = { path = \"../unrelated\", package = \"unrelated\" }\n"
         "named-lib = { path = \"../named-lib\" }\n"},
        {"app/src/main.rs",
         "use lib_core::Store;\n"
         "fn caller() -> i32 { let s = Store::new(); s.get() }\n"
         "fn renamed_dep() { renamed::Store::new(); }\n"
         "fn lib_name_wins() { short::in_named(); }\n"
         "fn package_name_is_not_it() { named_lib::in_named(); }\n"
         "fn not_a_dependency() { unrelated::Store::new(); }\n"
         "fn main() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "CALLS", "app.src.main.caller", "lib-core.src.store.Store.new"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "app.src.main.caller", "lib-core.src.store.Store.get"), 1);
        ASSERT(rust_edge(store, "IMPORTS", "app.src.main.rs.__file__", "lib-core.src.lib.rs.__file__") >= 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "app.src.main.renamed_dep", "unrelated.src.lib.Store.new"), 1);
        /* Code names a dependency by its [lib] name, not its package name. */
        ASSERT_EQ(rust_edge(store, "CALLS", "app.src.main.lib_name_wins", "named-lib.src.lib.in_named"), 1);
        ASSERT_EQ(rust_edges_from(store, "CALLS", "app.src.main.package_name_is_not_it"), 0);
        /* A crate the manifest does not list is no name in this crate. */
        ASSERT_EQ(rust_edges_from(store, "CALLS", "app.src.main.not_a_dependency"), 0);
    }
    PASS();
}

TEST(resolver_rust_inline_module_constant_is_a_declaration) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "pub mod main_key { pub const CREATED_AT: &str = \"created-at\"; }\n"
         "fn caller() -> &'static str { main_key::CREATED_AT }\n"
         "fn main() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "USAGE", "src.main.caller", "src.main.CREATED_AT"), 1);
    }
    PASS();
}

TEST(resolver_rust_module_layout_and_relative_paths) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "mod flat;\nmod nested;\n#[path = \"odd/place.rs\"]\nmod moved;\nmod both;\n"
         "fn caller() {\n"
         "    flat::f();\n    nested::deep::d();\n    moved::m();\n    both::b();\n"
         "}\nfn main() {}\n"},
        {"src/flat.rs", "pub fn f() {}\n"},
        {"src/nested/mod.rs", "pub mod deep;\npub(crate) fn up() {}\n"},
        {"src/nested/deep.rs", "pub fn d() { super::up(); self::same(); crate::flat::f(); }\nfn same() {}\n"},
        {"src/odd/place.rs", "pub fn m() {}\n"},
        /* rustc rejects both files existing (E0761): neither is the module. */
        {"src/both.rs", "pub fn b() {}\n"},
        {"src/both/mod.rs", "pub fn b() {}\n"},
        /* No `mod` reaches this file: its crate:: means nothing known. */
        {"src/dead.rs", "pub fn live() { own(); crate::flat::f(); }\nfn own() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.flat.f"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.nested.deep.d"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.odd.place.m"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.nested.deep.d", "src.nested.mod.up"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.nested.deep.d", "src.nested.deep.same"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.nested.deep.d", "src.flat.f"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.both.b"), 0);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.both.mod.b"), 0);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.dead.live", "src.dead.own"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.dead.live", "src.flat.f"), 0);
    }
    PASS();
}

TEST(resolver_rust_visibility_blocks_private_items_and_reexports_pass) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "mod inner;\nmod facade;\n"
         "fn caller() {\n"
         "    inner::open();\n"
         "    inner::hidden();\n"
         "    facade::renamed();\n"
         "    inner::Thing::new().visible();\n"
         "    inner::Thing::new().private_method();\n"
         "}\nfn main() {}\n"},
        {"src/inner.rs",
         "pub fn open() {}\nfn hidden() {}\n"
         "pub struct Thing;\n"
         "impl Thing {\n pub fn new() -> Thing { Thing }\n pub fn visible(&self) {}\n fn private_method(&self) {}\n}\n"},
        {"src/facade.rs", "pub use crate::inner::open as renamed;\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.inner.open"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.inner.hidden"), 0);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.inner.Thing.visible"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.inner.Thing.private_method"), 0);
    }
    PASS();
}

TEST(resolver_rust_methods_follow_the_language_probe) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "mod model;\nmod greet;\nmod quiet;\n"
         "use std::sync::Arc;\n"
         "use model::{Guard, Store};\n"
         "use greet::Greet;\n"
         "fn typed(s: &Store) { s.inherent(); s.hello(); s.defaulted(); s.silent(); }\n"
         "fn constructed() { let s = Store::new(); s.inherent(); }\n"
         "fn through_arc(s: Arc<Store>) { s.inherent(); }\n"
         "fn through_box() { let s = Box::new(Store::new()); s.inherent(); }\n"
         "fn through_deref(g: Guard<Store>) { g.inherent(); }\n"
         "fn through_field(g: &Holder) { g.store.inherent(); }\n"
         "struct Holder { store: Store }\n"
         "fn through_return() { Store::new().me().inherent(); }\n"
         "fn through_option(o: Option<Store>) { o.unwrap().inherent(); }\n"
         "fn through_iter(v: Vec<Store>) { for s in &v { s.inherent(); } v.iter().for_each(|s| s.inherent()); }\n"
         "async fn through_await() { model::make().await.inherent(); }\n"
         "fn bound<T: Greet>(t: T) { t.hello(); }\n"
         "fn trait_object(t: &dyn Greet) { t.hello(); }\n"
         "fn unknown(x: u32, y: Unknown) { x.inherent(); y.inherent(); }\n"
         "fn shadowed(s: &Store) { let s = 5; s.inherent(); }\n"
         "fn main() {}\n"},
        {"src/model.rs",
         "pub struct Store;\n"
         "impl Store {\n pub fn new() -> Self { Self }\n pub fn me(&self) -> &Self { self }\n pub fn inherent(&self) {}\n}\n"
         "impl crate::greet::Greet for Store { fn hello(&self) {} }\n"
         "impl crate::quiet::Quiet for Store { fn silent(&self) {} }\n"
         "pub async fn make() -> Store { Store }\n"
         "pub struct Guard<D> { inner: D }\n"
         "impl<D> std::ops::Deref for Guard<D> { type Target = D; fn deref(&self) -> &D { &self.inner } }\n"},
        {"src/greet.rs", "pub trait Greet {\n fn hello(&self);\n fn defaulted(&self) {}\n}\n"},
        {"src/quiet.rs", "pub trait Quiet { fn silent(&self); }\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        const char *inherent = "src.model.Store.inherent";
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.typed", inherent), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.constructed", "src.model.Store.new"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.constructed", inherent), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.through_arc", inherent), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.through_box", inherent), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.through_deref", inherent), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.through_field", inherent), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.through_return", inherent), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.through_option", inherent), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.through_iter", inherent), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.through_await", inherent), 1);
        /* A trait in scope: its impl's method, or its default body when not overridden. */
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.typed", "src.model.Store.hello"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.typed", "src.greet.Greet.defaulted"), 1);
        /* The receiver's type is only known through a bound: the trait's declaration. */
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.bound", "src.greet.Greet.hello"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.trait_object", "src.greet.Greet.hello"), 1);
        /* Quiet is not imported into main.rs, so `silent` is not callable there. */
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.typed", "src.model.Store.silent"), 0);
        /* No receiver type, no method: not a project method named `inherent`. */
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.unknown", inherent), 0);
        ASSERT_EQ(rust_edges_from(store, "CALLS", "src.main.shadowed"), 0);
    }
    PASS();
}

/* impl Foo<A> { fn new(x) } and impl Foo<B> { fn new() } share one QN, so one graph node:
 * neither signature can veto the other's call. An unfixed parameter is unknown to an
 * outside caller, even when it has bounds: its bound's method may not be the one called. */
TEST(resolver_rust_generic_signatures_at_a_use) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "struct A;\nstruct B;\n"
         "impl A { fn from_a(&self) {} }\n"
         "trait Tr { fn act(&self); }\n"
         "impl Tr for A { fn act(&self) {} }\n"
         "struct Foo<T> { value: T }\n"
         "impl Foo<A> { fn new(x: i32) -> Self { todo!() } }\n"
         "impl Foo<B> { fn new() -> Self { todo!() } }\n"
         "type FooA = Foo<A>;\n"
         "fn twins() { FooA::new(1); }\n"
         "struct Holder<T: Tr> { held: T }\n"
         "impl<T: Tr> Holder<T> { fn get(&self) -> &T { &self.held } fn new() -> Self { todo!() } }\n"
         "fn fixed(h: Holder<A>) { h.get().from_a(); h.held.from_a(); }\n"
         "fn unfixed() { let h = Holder::new(); h.get().act(); }\n"
         "impl<T: Tr> Holder<T> { fn wrap(held: T) -> Self { Holder { held } } }\n"
         "fn identity<V>(v: V) -> V { v }\n"
         "fn from_argument(a: A, other: A) { let h = Holder::wrap(a); h.held.from_a();\n"
         "    identity(other).from_a(); }\n"
         "fn inside<T: Tr>(h: Holder<T>) { h.get().act(); }\n"
         "fn block_local() {\n"
         "    struct Local { private: i32 }\n"
         "    impl Local { fn own(&self) -> i32 { self.private } }\n"
         "    let l = Local { private: 1 };\n"
         "    l.own();\n"
         "}\n"
         "fn main() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.twins", "src.main.Foo.new"), 1);
        /* The argument fixes T, so the walk continues on A. */
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.fixed", "src.main.Holder.get"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.fixed", "src.main.A.from_a"), 1);
        /* The value passed fixes T for `Self` and for the result. */
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.from_argument", "src.main.A.from_a"), 1);
        /* Nothing fixes T at this use: `act` on it could be anyone's. */
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.unfixed", "src.main.Tr.act"), 0);
        /* Inside the generic scope T is known by its bound. */
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.inside", "src.main.Tr.act"), 1);
        /* A block's own private members are visible in that block, so the walk selects
         * own. The graph has no node for a block-level struct, and the outcome says so. */
        ASSERT(rust_occurrence_reason(store, "CALLS", "l.own", "rust_declaration_not_materialized"));
    }
    PASS();
}

/* `<Foo as Tr>::bar` names Foo's impl of Tr, or Tr's default when Foo has none. */
TEST(resolver_rust_qualified_trait_path) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "trait Tr { fn bar(); fn dflt() {} }\n"
         "struct Foo;\nstruct Other;\n"
         "impl Tr for Foo { fn bar() {} }\n"
         "impl Tr for Other { fn bar() {} }\n"
         "impl Foo { fn inherent() {} }\n"
         "fn caller() { <Foo as Tr>::bar(); <Foo as Tr>::dflt(); <Foo>::inherent(); }\n"
         "fn main() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.main.Foo.bar"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.main.Other.bar"), 0);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.main.Tr.dflt"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.main.Foo.inherent"), 1);
    }
    PASS();
}

/* make_enum_progress! { enum X { .. } } inside a fn declares X for that block,
 * hiding a module-level X. Real form: milli's upgrade steps. */
TEST(resolver_rust_macro_declared_item_hides_module_name_in_its_block) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "macro_rules! progress { (enum $n:ident { $($v:ident),* }) => { enum $n { $($v),* } } }\n"
         "struct Cleanup;\n"
         "impl Cleanup { fn step() {} }\n"
         "fn declares() {\n"
         "    progress! { enum Cleanup { Step } };\n"
         "    let _ = Cleanup::Step;\n"
         "}\n"
         "fn plain() { Cleanup::step(); let _ = Cleanup; }\n"
         "fn main() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "USAGE", "src.main.declares", "src.main.Cleanup"), 0);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.plain", "src.main.Cleanup.step"), 1);
        ASSERT_EQ(rust_edge(store, "USAGE", "src.main.plain", "src.main.Cleanup"), 1);
    }
    PASS();
}

/* A closure's parameters are what the callee's signature says it passes. */
TEST(resolver_rust_closure_parameters_come_from_the_callee_signature) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "struct Settings;\n"
         "impl Settings { fn set(&mut self) {} }\n"
         "struct Holder;\n"
         "impl Holder {\n"
         " fn update(&self, f: impl Fn(&mut Settings)) {}\n"
         " fn generic<F: FnOnce(&mut Settings)>(&self, f: F) {}\n"
         " fn bound_in_where<F>(&self, f: F) where F: FnMut(&mut Settings) {}\n"
         " fn no_signature<F>(&self, f: F) {}\n"
         "}\n"
         "fn free(f: impl FnOnce(Settings)) {}\n"
         "fn through_impl(h: Holder) { h.update(|s| s.set()); }\n"
         "fn through_generic(h: Holder) { h.generic(|s| s.set()); }\n"
         "fn through_where(h: Holder) { h.bound_in_where(|s| s.set()); }\n"
         "fn through_free() { free(|mut s| s.set()); }\n"
         "fn untyped(h: Holder) { h.no_signature(|s| s.set()); }\n"
         "fn main() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        const char *set = "src.main.Settings.set";
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.through_impl", set), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.through_generic", set), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.through_where", set), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.through_free", set), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.untyped", set), 0);
    }
    PASS();
}

/* A blanket impl serves a type only if that type meets its project bounds. */
TEST(resolver_rust_blanket_impl_needs_its_bounds_met) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "trait Marker {}\n"
         "trait Extra { fn extra(&self) {} }\n"
         "impl<T: Marker> Extra for T {}\n"
         "struct Marked;\nimpl Marker for Marked {}\n"
         "struct Plain;\n"
         "fn meets(m: Marked) { m.extra(); }\n"
         "fn misses(p: Plain) { p.extra(); }\n"
         "fn main() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.meets", "src.main.Extra.extra"), 1);
        ASSERT_EQ(rust_edges_from(store, "CALLS", "src.main.misses"), 0);
    }
    PASS();
}

/* A project trait on Result serves a crate's Result<T> alias of it too. */
TEST(resolver_rust_project_trait_on_external_type) {
    static const rust_proof_file_t files[] = {
        {"Cargo.toml", "[package]\nname = \"demo\"\nedition = \"2021\"\n[dependencies]\nheed = \"1\"\n"},
        {"src/main.rs",
         "trait WithIndex<T> { fn with_index(self, i: usize) -> Option<T>; }\n"
         "impl<T, E> WithIndex<T> for Result<T, E> { fn with_index(self, i: usize) -> Option<T> { self.ok() } }\n"
         "trait Shout { fn shout(&self) -> String; }\n"
         "impl Shout for String { fn shout(&self) -> String { self.clone() } }\n"
         "fn from_alias(r: heed::Result<u32>) { r.with_index(1); }\n"
         "fn on_string(s: String) { s.shout(); }\n"
         "fn other_method(s: String) { s.len(); }\n"
         "fn main() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.from_alias", "src.main.Result.with_index"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.on_string", "src.main.String.shout"), 1);
        ASSERT_EQ(rust_edges_from(store, "CALLS", "src.main.other_method"), 0);
    }
    PASS();
}

/* No work cap decides graph content: every call in a big file keeps its edge. A
 * per-file proof pool with a node limit used to fail the sites past its limit. */
TEST(resolver_rust_big_file_keeps_every_edge) {
    enum { CALLERS = 4000 };
    size_t capacity = (size_t)CALLERS * 48 + 128;
    char *text = malloc(capacity);
    ASSERT_NOT_NULL(text);
    size_t used = (size_t)snprintf(text, capacity, "fn helper() {}\nfn main() {}\n");
    for (int i = 0; i < CALLERS; i++)
        used += (size_t)snprintf(text + used, capacity - used, "fn caller%d() { helper(); }\n", i);
    const rust_proof_file_t files[] = {{"src/main.rs", text}};
    char *root = NULL;
    cbm_store_t *store = rust_workspace_index(files, 1, 0, &root);
    ASSERT_NOT_NULL(store);
    sqlite3_stmt *statement = NULL;
    int edges = -1;
    if (sqlite3_prepare_v2(cbm_store_get_db(store),
                           "SELECT count(*) FROM edges WHERE type='CALLS'", -1, &statement,
                           NULL) == SQLITE_OK &&
        sqlite3_step(statement) == SQLITE_ROW)
        edges = sqlite3_column_int(statement, 0);
    sqlite3_finalize(statement);
    rust_workspace_done(store, root);
    free(text);
    ASSERT_EQ(edges, CALLERS);
    PASS();
}

/* assert_eq!, println!, vec! and friends take ordinary expressions of the caller.
 * A macro we cannot read keeps its tokens unresolved: they may mean anything. */
TEST(resolver_rust_std_macro_arguments_are_expressions) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "macro_rules! custom { ($($t:tt)*) => {} }\n"
         "struct Store;\n"
         "impl Store { fn get(&self) -> i32 { 1 } }\n"
         "const LIMIT: i32 = 3;\n"
         "fn helper(x: i32) -> i32 { x }\n"
         "fn asserted(s: Store) {\n"
         "    assert_eq!(helper(s.get()), 1, \"message {}\", LIMIT);\n"
         "}\n"
         "fn named_argument() { let helper = 1; println!(\"{helper}\", helper = helper); }\n"
         "fn opaque() { custom!(helper(1), LIMIT); }\n"
         "fn main() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.asserted", "src.main.helper"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.asserted", "src.main.Store.get"), 1);
        ASSERT_EQ(rust_edge(store, "USAGE", "src.main.asserted", "src.main.LIMIT"), 1);
        ASSERT_EQ(rust_edge(store, "USAGE", "src.main.named_argument", "src.main.helper"), 0);
        ASSERT_EQ(rust_edges_from(store, "CALLS", "src.main.opaque"), 0);
        ASSERT_EQ(rust_edge(store, "USAGE", "src.main.opaque", "src.main.LIMIT"), 0);
    }
    PASS();
}

/* thiserror's #[from] writes From impls the source never shows. `Error::from` on an
 * unknown value must not land on the one impl that happens to be written out. */
TEST(resolver_rust_generic_trait_impl_is_chosen_by_the_value_passed) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "mod hidden;\nmod explicit;\n"
         "use hidden::{Error, Plain};\n"
         "use explicit::{Two, A, B};\n"
         "fn unknown_value(x: Unknown) { let _ = Error::from(x); }\n"
         "fn known_plain(p: Plain) { let _ = Error::from(p); }\n"
         "fn known_other(s: String) { let _ = Error::from(s); }\n"
         "fn picks_a(a: A) { let _ = Two::from(a); }\n"
         "fn picks_b(b: B) { let _ = Two::from(b); }\n"
         "fn main() {}\n"},
        {"src/hidden.rs",
         "pub struct Plain;\n"
         "#[derive(Debug, thiserror::Error)]\n"
         "pub enum Error { #[error(\"io\")] Io(#[from] std::io::Error), #[error(\"plain\")] Plain }\n"
         "impl From<Plain> for Error { fn from(_: Plain) -> Self { Error::Plain } }\n"},
        {"src/explicit.rs", "pub mod for_a;\npub mod for_b;\npub struct Two;\npub struct A;\npub struct B;\n"},
        {"src/explicit/for_a.rs", "use super::{A, Two};\nimpl From<A> for Two { fn from(_: A) -> Two { Two } }\n"},
        {"src/explicit/for_b.rs", "use super::{B, Two};\nimpl From<B> for Two { fn from(_: B) -> Two { Two } }\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        const char *explicit_from = "src.hidden.Error.from";
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.unknown_value", explicit_from), 0);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.known_other", explicit_from), 0);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.known_plain", explicit_from), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.picks_a", "src.explicit.for_a.Two.from"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.picks_a", "src.explicit.for_b.Two.from"), 0);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.picks_b", "src.explicit.for_b.Two.from"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.picks_b", "src.explicit.for_a.Two.from"), 0);
    }
    PASS();
}

/* cfg twins of one name are one declaration; cfg-gated imports of one name are two. */
TEST(resolver_rust_cfg_twins_are_one_identity_and_cfg_imports_are_not) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "mod one;\nmod two;\n"
         "#[cfg(unix)]\nfn twin() {}\n#[cfg(not(unix))]\nfn twin() {}\n"
         "#[cfg(feature = \"a\")]\nuse one::chosen;\n#[cfg(not(feature = \"a\"))]\nuse two::chosen;\n"
         "fn caller() { twin(); }\n"
         "fn gated() { chosen(); }\n"
         "fn main() {}\n"},
        {"src/one.rs", "pub fn chosen() {}\n"},
        {"src/two.rs", "pub fn chosen() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.main.twin"), 1);
        ASSERT_EQ(rust_edges_from(store, "CALLS", "src.main.gated"), 0);
    }
    PASS();
}

/* The graph has one node per QN, so a QN two scopes claim names neither faithfully. */
TEST(resolver_rust_qn_collisions_withhold_the_edge) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "fn helper() {}\n"
         "mod inner { pub fn helper() {} }\n"
         "fn caller() { helper(); inner::helper(); }\n"
         "pub struct Store { inner: i32, plain: i32 }\n"
         "impl Store {\n"
         " pub fn inner(&self) -> i32 { self.inner + self.plain }\n"
         "}\n"
         "fn getter(s: &Store) -> i32 { s.inner() }\n"
         "fn main() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edges_from(store, "CALLS", "src.main.caller"), 0);
        /* The method keeps its node, so the call is exact... */
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.getter", "src.main.Store.inner"), 1);
        /* ...the same-named field has none, and a field read is no method reference. */
        ASSERT_EQ(rust_edge(store, "USAGE", "src.main.Store.inner", "src.main.Store.inner"), 0);
        ASSERT_EQ(rust_edge(store, "USAGE", "src.main.Store.inner", "src.main.Store.plain"), 1);
    }
    PASS();
}

static int rust_occurrences_like(cbm_store_t *store, const char *relation, const char *pattern) {
    sqlite3_stmt *statement = NULL;
    int count = -1;
    const char *sql = "SELECT count(*) FROM resolver_occurrences WHERE relation=?1 AND spelling LIKE ?2";
    if (store && sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &statement, NULL) == SQLITE_OK) {
        sqlite3_bind_text(statement, 1, relation, -1, SQLITE_STATIC);
        sqlite3_bind_text(statement, 2, pattern, -1, SQLITE_STATIC);
        if (sqlite3_step(statement) == SQLITE_ROW)
            count = sqlite3_column_int(statement, 0);
    }
    sqlite3_finalize(statement);
    return count;
}

/* async-openai keeps retired names commented out inside its use lists. */
TEST(resolver_rust_use_list_comment_is_not_a_leaf) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "mod kept;\n"
         "use kept::{\n    //gone, also_gone,\n    here,\n    /* gone_too */\n};\n"
         "fn caller() { here(); }\n"
         "fn main() {}\n"},
        {"src/kept.rs", "pub fn here() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.caller", "src.kept.here"), 1);
        ASSERT_EQ(rust_occurrences_like(store, "IMPORTS", "%gone%"), 0);
    }
    PASS();
}

/* A use takes every namespace it may see: routes-macros keeps a private `mod path`
 * beside its exported `pub fn path`, and `pub use routes_macros::path` compiles. */
TEST(resolver_rust_use_takes_the_namespace_it_may_see) {
    static const rust_proof_file_t files[] = {
        {"Cargo.toml", "[workspace]\nmembers = [\"m\", \"app\"]\n"},
        {"m/Cargo.toml", "[package]\nname = \"m\"\nedition = \"2021\"\n"},
        {"m/src/lib.rs", "mod path;\nmod hidden;\npub fn path() {}\n"},
        {"m/src/path.rs", "pub fn helper() {}\n"},
        {"m/src/hidden.rs", "pub fn helper() {}\n"},
        {"app/Cargo.toml",
         "[package]\nname = \"app\"\nedition = \"2021\"\n[dependencies]\nm = { path = \"../m\" }\n"},
        {"app/src/lib.rs", "pub mod visible;\npub mod blocked;\n"},
        {"app/src/visible.rs", "pub use m::path;\n"},
        {"app/src/blocked.rs", "pub use m::hidden;\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT(rust_edge(store, "IMPORTS", "app.src.visible.rs.__file__", "m.src.lib.rs.__file__") >=
               1);
        /* Private in the only namespace that holds it: still not visible. */
        ASSERT_EQ(rust_edges_from(store, "IMPORTS", "app.src.blocked.rs.__file__"), 0);
        ASSERT(rust_occurrence_reason(store, "IMPORTS", "m::hidden", "rust_use_path_not_visible"));
    }
    PASS();
}

/* Two uses of one name in one namespace do not compile, so `use thiserror::Error` beside
 * `use crate::Error` is the derive macro. Under cfg they really can name two things. */
TEST(resolver_rust_external_use_beside_a_project_use_is_another_namespace) {
    static const rust_proof_file_t files[] = {
        {"Cargo.toml",
         "[package]\nname = \"demo\"\nedition = \"2021\"\n"
         "[dependencies]\nthiserror = \"1\"\nother = \"1\"\n"},
        {"src/main.rs", "mod error;\nmod user;\nmod gated;\nfn main() {}\n"},
        {"src/error.rs", "pub struct Error;\nimpl Error { pub fn build() -> Error { Error } }\n"},
        {"src/user.rs",
         "use thiserror::Error;\nuse crate::error::Error;\n"
         "#[derive(Error, Debug)]\npub enum Local { A }\n"
         "fn caller() { Error::build(); }\n"},
        {"src/gated.rs",
         "#[cfg(feature = \"x\")]\nuse other::Error;\n"
         "#[cfg(not(feature = \"x\"))]\nuse crate::error::Error;\n"
         "fn gated() { Error::build(); }\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.user.caller", "src.error.Error.build"), 1);
        ASSERT_EQ(rust_edges_from(store, "CALLS", "src.gated.gated"), 0);
    }
    PASS();
}

/* `pub use self::steps::SearchStep` in milli's lib.rs names an enum make_enum_progress!
 * declares: the import is real, its module is known, the item itself stays unseen. */
TEST(resolver_rust_reexport_of_a_macro_declared_item_keeps_its_module) {
    static const rust_proof_file_t files[] = {
        {"Cargo.toml", "[package]\nname = \"demo\"\nedition = \"2021\"\n"},
        {"src/lib.rs",
         "pub mod steps;\npub mod only;\npub mod absent;\npub mod user;\n"
         "pub use self::steps::Step;\n"},
        {"src/steps.rs", "make_steps! { pub enum Step { One } }\n"},
        {"src/only.rs", "use crate::Step;\n"},
        {"src/absent.rs", "use crate::Absent;\n"},
        {"src/user.rs", "use crate::Step;\npub fn taker(step: Step) { let _ = step; }\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "IMPORTS", "src.only.rs.__file__", "src.lib.rs.__file__"), 1);
        /* No macro can supply it and nothing re-exports it. */
        ASSERT_EQ(rust_edges_from(store, "IMPORTS", "src.absent.rs.__file__"), 0);
        ASSERT(rust_occurrence_reason(store, "IMPORTS", "crate::Absent", "rust_use_path_unresolved"));
        ASSERT(rust_occurrence_reason(store, "USAGE", "Step", "rust_macro_declared_item"));
    }
    PASS();
}

/* A file no crate root reaches has no dependency names to check a path against. */
TEST(resolver_rust_unreached_file_reports_its_crate_as_unknown) {
    static const rust_proof_file_t files[] = {
        {"Cargo.toml", "[package]\nname = \"demo\"\nedition = \"2021\"\n"},
        {"src/lib.rs", "pub fn root() {}\n"},
        {"src/dead.rs", "use serde::Serialize;\nfn a() {}\nfn b() { a(); }\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT(rust_occurrence_reason(store, "IMPORTS", "serde::Serialize", "rust_crate_root_unknown"));
        /* Its own items still resolve. */
        ASSERT_EQ(rust_edge(store, "CALLS", "src.dead.b", "src.dead.a"), 1);
    }
    PASS();
}

/* FromIterator: `_` is the iterator's element. A map takes pairs, and Result<C, E> takes
 * items of that wrapper. Nothing fills it when the element itself is not known. */
TEST(resolver_rust_collect_fills_the_placeholder_from_the_iterator) {
    static const rust_proof_file_t files[] = {
        {"Cargo.toml",
         "[package]\nname = \"demo\"\nedition = \"2021\"\n"
         "[dependencies]\nbumpalo = \"3\"\nhashbrown = \"0.15\"\nother_crate = \"1\"\n"},
        {"src/main.rs",
         "use std::collections::HashMap;\n"
         "pub struct Thing;\nimpl Thing { pub fn run(&self) {} }\n"
         "pub struct Other;\nimpl Other { pub fn run(&self) {} }\n"
         "type Res<T> = std::result::Result<T, String>;\n"
         "fn make() -> Vec<Thing> { Vec::new() }\n"
         "fn results() -> Vec<Res<Thing>> { Vec::new() }\n"
         "fn pairs() -> Vec<(i32, Other)> { Vec::new() }\n"
         "fn annotated() { let all: Vec<_> = make().into_iter().collect(); for t in all { t.run(); } }\n"
         "fn turbofish() { let all = make().into_iter().collect::<Vec<_>>(); for t in all { t.run(); } }\n"
         "fn fallible() {\n"
         "    let all = results().into_iter().collect::<Res<Vec<_>>>().unwrap();\n"
         "    for t in all { t.run(); }\n"
         "}\n"
         "fn keyed() {\n"
         "    let all: HashMap<_, _> = pairs().into_iter().collect();\n"
         "    for (_, v) in all { v.run(); }\n"
         "}\n"
         "fn unknown_source() { let all: Vec<_> = other_crate::items().collect(); for t in all { t.run(); } }\n"
         "fn boxed() -> Res<Box<dyn Iterator<Item = Res<Thing>> + 'static>> { todo!() }\n"
         "fn through_a_box() {\n"
         "    let mut all = boxed().unwrap().collect::<Res<Vec<_>>>().unwrap();\n"
         "    all.pop().unwrap().run();\n"
         "}\n"
         "fn sorted(mut all: Vec<Thing>) { all.sort_by_key(|item| item.run()); }\n"
         "fn compared(mut all: Vec<Thing>) { all.sort_by(|a, b| { a.run(); b.run(); todo!() }); }\n"
         "fn folded(all: Vec<Other>) { all.into_iter().fold(0, |_sum, item| { item.run(); 0 }); }\n"
         "fn best(all: Vec<Other>) { all.iter().max_by_key(|item| item.run()); }\n"
         "fn visit(pick: &impl Fn(&Thing) -> bool) { todo!() }\n"
         "fn by_reference() { visit(&|item| { item.run(); true }); }\n"
         "fn bump_seq(all: bumpalo::collections::Vec<'_, Thing>) { for t in all { t.run(); } }\n"
         "fn third_party(all: hashbrown::HashMap<i32, Other>) {\n"
         "    for (_, v) in all.iter() { v.run(); }\n"
         "    all.get(&1).unwrap().run();\n"
         "}\n"
         "fn lookalike(all: other_crate::HashMap<i32, Other>) { all.get(&1).unwrap().run(); }\n"
         "fn main() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.annotated", "src.main.Thing.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.turbofish", "src.main.Thing.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.fallible", "src.main.Thing.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.keyed", "src.main.Other.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.keyed", "src.main.Thing.run"), 0);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.unknown_source", "src.main.Thing.run"), 0);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.unknown_source", "src.main.Other.run"), 0);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.through_a_box", "src.main.Thing.run"), 1);
        /* The element reaches the closure through the std signature of each method. */
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.sorted", "src.main.Thing.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.compared", "src.main.Thing.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.folded", "src.main.Other.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.folded", "src.main.Thing.run"), 0);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.best", "src.main.Other.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.by_reference", "src.main.Thing.run"), 1);
        /* Named third-party containers read like std; an unlisted lookalike does not. */
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.bump_seq", "src.main.Thing.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.third_party", "src.main.Other.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.lookalike", "src.main.Other.run"), 0);
        /* The placeholder is not a name that failed to resolve. */
        ASSERT(rust_occurrence_reason(store, "USAGE", "_", "rust_inferred_type"));
    }
    PASS();
}

/* `let index; ... index = make()?;` from meilisearch's process_batch: Rust infers the binding
 * from its first assignment. An annotated or initialized binding already has its type, and a
 * binding shadowed by a later `let` is a different binding. */
TEST(resolver_rust_deferred_let_takes_its_type_from_the_assignment) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "pub struct Thing;\nimpl Thing { pub fn run(&self) {} }\n"
         "pub struct Other;\nimpl Other { pub fn run(&self) {} }\n"
         "fn branches(flag: bool) {\n"
         "    let chosen;\n"
         "    if flag { chosen = Thing; } else { chosen = Thing; }\n"
         "    chosen.run();\n"
         "}\n"
         "fn unassigned() {\n"
         "    let never_set;\n"
         "    never_set.run();\n"
         "}\n"
         "fn from_unknown() {\n"
         "    let value;\n"
         "    value = other_crate::make();\n"
         "    value.run();\n"
         "}\n"
         "fn shadowed() {\n"
         "    let first;\n"
         "    first = Thing;\n"
         "    let first = Other;\n"
         "    first.run();\n"
         "}\n"
         "fn main() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.branches", "src.main.Thing.run"), 1);
        ASSERT_EQ(rust_edges_from(store, "CALLS", "src.main.unassigned"), 0);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.from_unknown", "src.main.Thing.run"), 0);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.shadowed", "src.main.Other.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.shadowed", "src.main.Thing.run"), 0);
    }
    PASS();
}

/* meilitool's `Index::new(..).with_context(..)?`: the trait is named, so its signature is known.
 * The same method through another crate's Context says nothing about the result. */
TEST(resolver_rust_anyhow_context_keeps_the_success_type) {
    static const rust_proof_file_t files[] = {
        {"Cargo.toml",
         "[package]\nname = \"demo\"\nedition = \"2021\"\n"
         "[dependencies]\nanyhow = \"1\"\nother = \"1\"\n"},
        {"src/main.rs",
         "mod a;\nmod b;\nmod thing;\nfn main() {}\n"},
        {"src/thing.rs",
         "pub struct Thing;\nimpl Thing { pub fn run(&self) {}\n"
         " pub fn make() -> std::io::Result<Thing> { Ok(Thing) } }\n"},
        {"src/a.rs",
         "use anyhow::Context;\nuse crate::thing::Thing;\n"
         "fn result() -> anyhow::Result<()> {\n"
         "    let t = Thing::make().with_context(|| \"opening\")?;\n"
         "    t.run();\n"
         "    Ok(())\n"
         "}\n"},
        {"src/b.rs",
         "use other::Context;\nuse crate::thing::Thing;\n"
         "fn lookalike() -> anyhow::Result<()> {\n"
         "    let t = Thing::make().with_context(|| \"opening\")?;\n"
         "    t.run();\n"
         "    Ok(())\n"
         "}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.a.result", "src.thing.Thing.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.b.lookalike", "src.thing.Thing.run"), 0);
    }
    PASS();
}

/* `index: &Index` used to record one span around `&Index`, which proved nothing and was
 * withheld, and `Option<Foo>` recorded only Option. Each named type is its own use. */
TEST(resolver_rust_every_named_type_in_a_signature_is_a_use) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "pub struct Alpha;\npub struct Beta;\npub struct Gamma;\npub struct Delta;\n"
         "fn wrapped(a: &Alpha, b: Option<Beta>) -> Result<Vec<Gamma>, String> { todo!() }\n"
         "fn local() { let held: Vec<Delta> = Vec::new(); drop(held); }\n"
         "fn bound(it: impl Iterator<Item = Alpha>) { drop(it); }\n"
         "fn generic<Beta>(value: Beta) { drop(value); }\n"
         "fn main() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "USES_TYPE", "src.main.wrapped", "src.main.Alpha"), 1);
        ASSERT_EQ(rust_edge(store, "USES_TYPE", "src.main.wrapped", "src.main.Beta"), 1);
        ASSERT_EQ(rust_edge(store, "USES_TYPE", "src.main.wrapped", "src.main.Gamma"), 1);
        ASSERT_EQ(rust_edge(store, "USES_TYPE", "src.main.local", "src.main.Delta"), 1);
        ASSERT_EQ(rust_edge(store, "USES_TYPE", "src.main.bound", "src.main.Alpha"), 1);
        /* `Item` names a member of the trait; it is no use of a type in scope. */
        ASSERT_EQ(rust_occurrences_like(store, "USES_TYPE", "Item"), 0);
        /* The parameter hides the struct of the same name. */
        ASSERT_EQ(rust_edge(store, "USES_TYPE", "src.main.generic", "src.main.Beta"), 0);
        ASSERT_EQ(rust_occurrences_like(store, "USES_TYPE", "%&%"), 0);
    }
    PASS();
}

/* `Self::Named { inner } => inner.go()` from meilisearch's Details: Self is the impl's type,
 * so the variant's fields give the pattern its types. `Self::Output` is a member with no node. */
TEST(resolver_rust_self_variant_path_binds_its_fields) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "pub struct Inner;\nimpl Inner { pub fn go(&self) {} }\n"
         "pub struct Decoy;\nimpl Decoy { pub fn go(&self) {} }\n"
         "pub enum Kind { Named { inner: Inner }, Other { inner: Decoy } }\n"
         "impl Kind {\n"
         "    pub fn act(&self) {\n"
         "        match self { Self::Named { inner } => inner.go(), Self::Other { .. } => {} }\n"
         "    }\n"
         "    pub fn make(i: Inner) -> Self { Self::Named { inner: i } }\n"
         "}\n"
         "impl std::ops::Add for Kind {\n"
         "    type Output = Kind;\n"
         "    fn add(self, _o: Kind) -> Self::Output { self }\n"
         "}\n"
         "fn main() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.Kind.act", "src.main.Inner.go"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.Kind.act", "src.main.Decoy.go"), 0);
        ASSERT(!rust_occurrence_reason(store, "USAGE", "Named", "rust_struct_path_unsupported"));
        ASSERT(!rust_occurrence_reason(store, "USAGE", "Named", "rust_pattern_path_unsupported"));
        ASSERT(rust_occurrence_reason(store, "USES_TYPE", "Self::Output", "rust_associated_type"));
    }
    PASS();
}

/* meilisearch's `use std::ops::Bound::*;` in a function body hid every outer name from the walk,
 * because an external glob might bind anything. A standard enum's variants are a closed list. */
TEST(resolver_rust_glob_of_a_std_enum_binds_only_its_variants) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "pub struct Local;\nimpl Local { pub fn make() {} }\n"
         "fn helper() {}\n"
         "fn Included(_v: i32) {}\n"
         "fn closed() {\n"
         "    use std::ops::Bound::*;\n"
         "    let _low = Included(1);\n"
         "    Local::make();\n"
         "    helper();\n"
         "}\n"
         "fn outer() { Included(2); }\n"
         "fn open_glob() {\n"
         "    use other_crate::stuff::*;\n"
         "    helper();\n"
         "}\n"
         "fn main() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.closed", "src.main.Local.make"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.closed", "src.main.helper"), 1);
        /* The variant the glob does bind hides the module's function in that block. */
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.closed", "src.main.Included"), 0);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.outer", "src.main.Included"), 1);
        /* Another crate's glob stays open: it may bind helper. */
        ASSERT_EQ(rust_edges_from(store, "CALLS", "src.main.open_glob"), 0);
        ASSERT(rust_occurrence_reason(store, "CALLS", "helper", "rust_glob_domain_open"));
    }
    PASS();
}

/* Standard signatures that carry the element type through a chain. */
TEST(resolver_rust_std_signatures_carry_the_element) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "use std::collections::HashMap;\n"
         "use std::collections::hash_map::Entry;\n"
         "#[derive(Default)]\npub struct Thing;\nimpl Thing { pub fn run(&self) {} }\n"
         "pub struct Other;\nimpl Other { pub fn run(&self) {} }\n"
         "fn entry_default(mut m: HashMap<i32, Thing>) { m.entry(1).or_default().run(); }\n"
         "fn entry_with(mut m: HashMap<i32, Other>) { m.entry(1).or_insert_with(|| Other).run(); }\n"
         "fn occupied(mut m: HashMap<i32, Other>) {\n"
         "    match m.entry(1) {\n"
         "        Entry::Occupied(mut e) => e.get_mut().run(),\n"
         "        Entry::Vacant(v) => v.insert(Other).run(),\n"
         "    }\n"
         "}\n"
         "fn best(all: Vec<Thing>) { all.into_iter().max_by_key(|_| 0).unwrap().run(); }\n"
         "fn flat(all: Vec<Vec<Other>>) { for x in all.into_iter().flatten() { x.run(); } }\n"
         "fn zipped(a: Vec<Thing>, b: Vec<Other>) {\n"
         "    for (x, y) in a.into_iter().zip(b) { x.run(); y.run(); }\n"
         "}\n"
         "fn transposed(v: Option<Result<Thing, String>>) -> Result<(), String> {\n"
         "    v.transpose()?.unwrap().run();\n"
         "    Ok(())\n"
         "}\n"
         "fn windows(all: Vec<Other>) { for w in all.windows(2) { w[0].run(); } }\n"
         "fn first_pair(m: std::collections::BTreeMap<i32, Thing>) {\n"
         "    let (_, v) = m.first_key_value().unwrap();\n"
         "    v.run();\n"
         "}\n"
         "fn main() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.entry_default", "src.main.Thing.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.entry_default", "src.main.Other.run"), 0);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.entry_with", "src.main.Other.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.occupied", "src.main.Other.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.best", "src.main.Thing.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.flat", "src.main.Other.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.zipped", "src.main.Thing.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.zipped", "src.main.Other.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.transposed", "src.main.Thing.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.windows", "src.main.Other.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.first_pair", "src.main.Thing.run"), 1);
    }
    PASS();
}

/* `_` is an anonymous token, so counting named children moved `v` in `(_, v)` onto the first
 * element's type: the call landed on a method of the wrong type. */
TEST(resolver_rust_underscore_holds_its_position) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "pub struct Thing;\nimpl Thing { pub fn run(&self) {} }\n"
         "pub struct Other;\nimpl Other { pub fn run(&self) {} }\n"
         "pub struct Pair(pub Thing, pub Other);\n"
         "fn tuple(pairs: Vec<(Thing, Other)>) { for (_, v) in pairs { v.run(); } }\n"
         "fn tuple_struct(p: Pair) { let Pair(_, v) = p; v.run(); }\n"
         "fn visit(_f: impl Fn(Thing, Other)) {}\n"
         "fn closure() { visit(|_, v| v.run()); }\n"
         "fn main() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.tuple", "src.main.Other.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.tuple", "src.main.Thing.run"), 0);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.tuple_struct", "src.main.Other.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.tuple_struct", "src.main.Thing.run"), 0);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.closure", "src.main.Other.run"), 1);
        ASSERT_EQ(rust_edge(store, "CALLS", "src.main.closure", "src.main.Thing.run"), 0);
    }
    PASS();
}

/* matches! takes a pattern: Kind::One names the enum, and a bare `helper` is a new binding
 * even when a function of that name is in scope. */
TEST(resolver_rust_matches_macro_reads_a_pattern) {
    static const rust_proof_file_t files[] = {
        {"src/main.rs",
         "pub enum Kind { One, Two(i32) }\n"
         "pub fn helper() {}\n"
         "pub fn make() -> Kind { Kind::One }\n"
         "fn check() -> bool { let k = make(); matches!(k, Kind::One | Kind::Two(_)) }\n"
         "fn binder(k: &Option<i32>) -> bool { matches!(k, Some(helper) if *helper > 0) }\n"
         "fn main() {}\n"}};
    RUST_ROUTES(store, root, files) {
        ASSERT_NOT_NULL(store);
        ASSERT_EQ(rust_edge(store, "USAGE", "src.main.check", "src.main.Kind"), 1);
        ASSERT_EQ(rust_edge(store, "USAGE", "src.main.binder", "src.main.helper"), 0);
    }
    PASS();
}

SUITE(resolver_rust) {
    RUN_TEST(resolver_rust_same_module_item_ignores_the_files_other_syntax);
    RUN_TEST(resolver_rust_use_tree_binds_one_name_per_leaf);
    RUN_TEST(resolver_rust_workspace_dependency_names_come_from_cargo);
    RUN_TEST(resolver_rust_inline_module_constant_is_a_declaration);
    RUN_TEST(resolver_rust_module_layout_and_relative_paths);
    RUN_TEST(resolver_rust_visibility_blocks_private_items_and_reexports_pass);
    RUN_TEST(resolver_rust_methods_follow_the_language_probe);
    RUN_TEST(resolver_rust_macro_declared_item_hides_module_name_in_its_block);
    RUN_TEST(resolver_rust_std_macro_arguments_are_expressions);
    RUN_TEST(resolver_rust_big_file_keeps_every_edge);
    RUN_TEST(resolver_rust_qualified_trait_path);
    RUN_TEST(resolver_rust_generic_signatures_at_a_use);
    RUN_TEST(resolver_rust_closure_parameters_come_from_the_callee_signature);
    RUN_TEST(resolver_rust_blanket_impl_needs_its_bounds_met);
    RUN_TEST(resolver_rust_project_trait_on_external_type);
    RUN_TEST(resolver_rust_generic_trait_impl_is_chosen_by_the_value_passed);
    RUN_TEST(resolver_rust_cfg_twins_are_one_identity_and_cfg_imports_are_not);
    RUN_TEST(resolver_rust_qn_collisions_withhold_the_edge);
    RUN_TEST(resolver_rust_use_list_comment_is_not_a_leaf);
    RUN_TEST(resolver_rust_use_takes_the_namespace_it_may_see);
    RUN_TEST(resolver_rust_external_use_beside_a_project_use_is_another_namespace);
    RUN_TEST(resolver_rust_reexport_of_a_macro_declared_item_keeps_its_module);
    RUN_TEST(resolver_rust_unreached_file_reports_its_crate_as_unknown);
    RUN_TEST(resolver_rust_collect_fills_the_placeholder_from_the_iterator);
    RUN_TEST(resolver_rust_deferred_let_takes_its_type_from_the_assignment);
    RUN_TEST(resolver_rust_anyhow_context_keeps_the_success_type);
    RUN_TEST(resolver_rust_every_named_type_in_a_signature_is_a_use);
    RUN_TEST(resolver_rust_self_variant_path_binds_its_fields);
    RUN_TEST(resolver_rust_glob_of_a_std_enum_binds_only_its_variants);
    RUN_TEST(resolver_rust_std_signatures_carry_the_element);
    RUN_TEST(resolver_rust_underscore_holds_its_position);
    RUN_TEST(resolver_rust_matches_macro_reads_a_pattern);
    RUN_TEST(resolver_rust_local_impl_preserves_import_domain);
    RUN_TEST(resolver_rust_primitive_arguments_and_private_self_require_proofs);
    RUN_TEST(resolver_rust_declared_module_and_use_keep_exact_target);
    RUN_TEST(resolver_rust_inline_module_does_not_bind_top_level_namesake);
    RUN_TEST(resolver_rust_attributes_and_macros_keep_an_explicit_item);
    RUN_TEST(resolver_rust_import_alias_preserves_binding_name);
    RUN_TEST(resolver_rust_shared_reference_and_alias);
    RUN_TEST(resolver_rust_receiver_shadow_and_unsupported_flow_abstain);
    RUN_TEST(resolver_rust_visibility_traits_attributes_and_duplicates_abstain);
    RUN_TEST(resolver_rust_pipeline_keeps_exact_method_and_rejects_module_alias);
}
