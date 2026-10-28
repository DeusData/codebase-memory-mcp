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

TEST(resolver_rust_attributes_and_macros_do_not_donate_import_binding) {
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
        int bindings = rust_proof_bindings(files, 2, "src/actual.rs", "target", "Function");
        if (bindings)
            fprintf(stderr, "Rust unsupported import binding %zu: %d\n", i, bindings);
        ASSERT_EQ(bindings, 0);
        ASSERT_EQ(rust_proof_edges(files, 2, "src/actual.rs", "target", "Function"), 0);
    }
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
                .declaration_text = " pub fn float(&self, value: f32) {}"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "rust-float-literal",
     .why = "floating literal representation and range are not audited",
     .tried = "explicit f32 suffix is genuine but only primitive parameter facts and bounded "
              "integer/bool literals are proved"},
    {.case_id = "local-literal-inference",
     .site = {.file = "src/main.rs", .caller = "gaps", .needle = "value.take(local)"},
     .relation = "CALLS",
     .target = {.file = "src/service.rs",
                .suffix = "Service.take",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " pub fn take(&self, value: i32) {}"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "rust-local-literal-inference",
     .why = "local literal type inference is not modeled",
     .tried =
         "the source infers i32 from the call; parameter-to-local copying is proved separately"},
    {.case_id = "generic-inference",
     .site = {.file = "src/main.rs", .caller = "gaps", .needle = "value.generic(1)"},
     .relation = "CALLS",
     .target = {.file = "src/service.rs",
                .suffix = "Service.generic",
                .label = "Method",
                .declaration_line = 8,
                .declaration_text = " pub fn generic<T>(&self, value: T) {}"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "rust-generic-inference",
     .why = "generic source signatures and inference remain unsupported",
     .tried = "the compiler infers an integer T; the bridge retains the complete unproved method "
              "domain"},
    {.case_id = "owned-receiver",
     .site = {.file = "src/main.rs", .caller = "owned", .needle = "value.take(1)"},
     .relation = "CALLS",
     .target = {.file = "src/service.rs",
                .suffix = "Service.take",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " pub fn take(&self, value: i32) {}"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "rust-owned-receiver",
     .why = "owned receiver autoref is not part of the audited reference candidate domain",
     .tried = "the valid owned Service can borrow as &Service; only explicit shared-reference "
              "facts are currently accepted"},
    {.case_id = "mutable-receiver",
     .site = {.file = "src/main.rs", .caller = "mutable", .needle = "value.take(2)"},
     .relation = "CALLS",
     .target = {.file = "src/service.rs",
                .suffix = "Service.take",
                .label = "Method",
                .declaration_line = 3,
                .declaration_text = " pub fn take(&self, value: i32) {}"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "rust-mutable-receiver",
     .why = "mutable receiver reborrow/autoref remains unsupported",
     .tried = "a mutable reference can call an &self method, but the current type fact "
              "deliberately preserves that missing adaptation"},
    {.case_id = "constructor-receiver",
     .site = {.file = "src/main.rs", .caller = "owned_local", .needle = "c.run(2)"},
     .relation = "CALLS",
     .target = {.file = "src/main.rs",
                .suffix = "Calc.run",
                .label = "Method",
                .declaration_line = 26,
                .declaration_text = " fn run(&self, y: i32) -> i32 { self.helper(y) }"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "rust-constructor-receiver",
     .why = "struct construction and owned-receiver autoref are not proved",
     .tried = "the same-file private self call is required; this distinct constructor path remains "
              "genuine quality debt"},
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
        ASSERT_EQ(report.found, 10);
        ASSERT_EQ(report.missing, 6);
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

SUITE(resolver_rust) {
    RUN_TEST(resolver_rust_local_impl_preserves_import_domain);
    RUN_TEST(resolver_rust_primitive_arguments_and_private_self_require_proofs);
    RUN_TEST(resolver_rust_declared_module_and_use_keep_exact_target);
    RUN_TEST(resolver_rust_inline_module_does_not_bind_top_level_namesake);
    RUN_TEST(resolver_rust_attributes_and_macros_do_not_donate_import_binding);
    RUN_TEST(resolver_rust_import_alias_preserves_binding_name);
    RUN_TEST(resolver_rust_shared_reference_and_alias);
    RUN_TEST(resolver_rust_receiver_shadow_and_unsupported_flow_abstain);
    RUN_TEST(resolver_rust_visibility_traits_attributes_and_duplicates_abstain);
    RUN_TEST(resolver_rust_pipeline_keeps_exact_method_and_rejects_module_alias);
}
