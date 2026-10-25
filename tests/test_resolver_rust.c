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

SUITE(resolver_rust) {
    RUN_TEST(resolver_rust_declared_module_and_use_keep_exact_target);
    RUN_TEST(resolver_rust_inline_module_does_not_bind_top_level_namesake);
    RUN_TEST(resolver_rust_attributes_and_macros_do_not_donate_import_binding);
    RUN_TEST(resolver_rust_import_alias_preserves_binding_name);
    RUN_TEST(resolver_rust_shared_reference_and_alias);
    RUN_TEST(resolver_rust_receiver_shadow_and_unsupported_flow_abstain);
    RUN_TEST(resolver_rust_visibility_traits_attributes_and_duplicates_abstain);
    RUN_TEST(resolver_rust_pipeline_keeps_exact_method_and_rejects_module_alias);
}
