#include "test_framework.h"
#include "test_helpers.h"
#include "lsp/c_lsp.h"
#include "lsp/py_lsp.h"
#include "resolve/binding.h"
#include "result_spill.h"
#include "pipeline/lsp_resolve.h"
#include "pipeline/shared_resolver.h"
#include "resolve/policy.h"
#include "resolve/capabilities.h"
#include "resolve/proof.h"
#include "resolve/snapshot.h"
#include "store/store_resolver.h"

TEST(resolver_target_mapping_requires_exact_identity) {
    cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp");
    ASSERT_NOT_NULL(gb);
    cbm_gbuf_upsert_node(gb, "Method", "run", "proj.other.Worker.run", "other.ts", 1, 1, "{}");
    const cbm_gbuf_node_t *wrong =
        cbm_pipeline_lsp_target_node(gb, "proj", "vendor.Worker.run", true);
    const cbm_gbuf_node_t *exact =
        cbm_pipeline_lsp_target_node(gb, "proj", "other.Worker.run", true);
    bool rejected = wrong == NULL;
    bool retained = exact != NULL;
    cbm_gbuf_free(gb);
    ASSERT(rejected);
    ASSERT(retained);
    PASS();
}

TEST(resolver_unknown_receiver_never_uses_unique_field) {
    cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp");
    cbm_registry_t *reg = cbm_registry_new();
    ASSERT_NOT_NULL(gb);
    ASSERT_NOT_NULL(reg);
    cbm_gbuf_upsert_node(gb, "Field", "close", "proj.File.close", "file.h", 1, 1, "{}");
    cbm_registry_add(reg, "close", "proj.File.close", "Field");
    cbm_resolution_t res = {0};
    const cbm_gbuf_node_t *field = cbm_pipeline_c_member_call_resolve(
        NULL, NULL, gb, reg, "proj", "proj.main", "unknown->close", true, &res);
    bool rejected = field == NULL;
    cbm_registry_free(reg);
    cbm_gbuf_free(gb);
    ASSERT(rejected);
    PASS();
}

TEST(resolver_aggregate_field_rows_cannot_choose_occurrence_owner) {
    cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp");
    ASSERT_NOT_NULL(gb);
    cbm_gbuf_upsert_node(gb, "Field", "close", "proj.A.close", "a.h", 1, 1, "{}");
    cbm_gbuf_upsert_node(gb, "Field", "close", "proj.B.close", "b.h", 1, 1, "{}");
    CBMResolvedCall rows[] = {{.caller_qn = "proj.main",
                               .callee_qn = "proj.A.close",
                               .strategy = "lsp_field_access",
                               .confidence = 0.95f,
                               .kind = CBM_RESOLVED_FIELD_REFERENCE},
                              {.caller_qn = "proj.main",
                               .callee_qn = "proj.B.close",
                               .strategy = "lsp_field_access",
                               .confidence = 0.95f,
                               .kind = CBM_RESOLVED_FIELD_REFERENCE}};
    CBMResolvedCallArray array = {.items = rows, .count = 2};
    bool typed = false;
    const cbm_gbuf_node_t *field = cbm_pipeline_c_member_call_field(
        &array, NULL, gb, "proj", "proj.main", "receiver.close", &typed);
    bool rejected = field == NULL;
    cbm_gbuf_free(gb);
    ASSERT(rejected);
    ASSERT(typed);
    PASS();
}

static CBMResolvedCall resolver_field_row(const char *target, uint32_t start, uint32_t end) {
    return (CBMResolvedCall){
        .caller_qn = "proj.main",
        .callee_qn = target,
        .kind = CBM_RESOLVED_FIELD_REFERENCE,
        .site_start_byte = start,
        .site_end_byte = end,
        .evidence = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                     .rule = CBM_RESOLVE_RULE_TYPE_MEMBER,
                     .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                              CBM_RESOLVE_FACT_RECEIVER_TYPED | CBM_RESOLVE_FACT_OCCURRENCE_EXACT}};
}

TEST(resolver_field_occurrences_keep_distinct_receivers_and_origins) {
    CBMResolvedCall rows[] = {resolver_field_row("proj.A.close", 10, 15),
                              resolver_field_row("proj.B.close", 20, 25)};
    CBMResolvedCallArray array = {.items = rows, .count = 2};
    cbm_pipeline_lsp_field_index_t index = {0};
    ASSERT(cbm_pipeline_lsp_field_index_build(&array, &index));
    cbm_pipeline_lsp_field_cursor_t first = cbm_pipeline_lsp_field_cursor_at(
        &array, &index, "proj.main", "close", 10, 15, CBM_SOURCE_ORIGIN_RAW);
    cbm_pipeline_lsp_field_cursor_t second = cbm_pipeline_lsp_field_cursor_at(
        &array, &index, "proj.main", "close", 20, 25, CBM_SOURCE_ORIGIN_RAW);
    cbm_pipeline_lsp_field_cursor_t wrong_origin = cbm_pipeline_lsp_field_cursor_at(
        &array, &index, "proj.main", "close", 10, 15, CBM_SOURCE_ORIGIN_PREPROCESSED);
    cbm_pipeline_lsp_field_cursor_t unknown = cbm_pipeline_lsp_field_cursor_at(
        &array, &index, "proj.main", "close", 30, 35, CBM_SOURCE_ORIGIN_RAW);
    const CBMResolvedCall *a = cbm_pipeline_lsp_field_next(&first);
    const CBMResolvedCall *b = cbm_pipeline_lsp_field_next(&second);
    bool correct =
        a && b && !strcmp(a->callee_qn, "proj.A.close") && !strcmp(b->callee_qn, "proj.B.close");
    bool rejected =
        !cbm_pipeline_lsp_field_next(&wrong_origin) && !cbm_pipeline_lsp_field_next(&unknown);
    cbm_pipeline_lsp_field_index_free(&index);
    ASSERT(correct);
    ASSERT(rejected);
    PASS();
}

TEST(resolver_field_occurrence_conflicts_are_not_ranked) {
    CBMResolvedCall rows[] = {resolver_field_row("proj.A.close", 10, 15),
                              resolver_field_row("proj.B.close", 10, 15)};
    CBMResolvedCallArray array = {.items = rows, .count = 2};
    cbm_pipeline_lsp_field_index_t index = {0};
    ASSERT(cbm_pipeline_lsp_field_index_build(&array, &index));
    cbm_pipeline_lsp_field_cursor_t owners = cbm_pipeline_lsp_field_cursor_at(
        &array, &index, "proj.main", "close", 10, 15, CBM_SOURCE_ORIGIN_RAW);
    bool rejected = cbm_pipeline_lsp_field_next(&owners) == NULL;
    cbm_pipeline_lsp_field_index_free(&index);
    ASSERT(owners.typed);
    ASSERT(rejected);
    PASS();
}

TEST(resolver_c_fields_and_writes_keep_exact_occurrence_proof) {
    const char *source = "struct A { int close; }; struct B { int close; };\n"
                         "void run(struct A *a, struct B *b, struct Missing *u) {\n"
                         "a->close = 1; b->close = 2; u->close = 3; }\n";
    CBMFileResult *result =
        cbm_extract_file(source, (int)strlen(source), CBM_LANG_C, "proj", "main.c", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    uint32_t first = (uint32_t)(strstr(source, "a->close") - source) + 3;
    uint32_t second = (uint32_t)(strstr(source, "b->close") - source) + 3;
    uint32_t unknown = (uint32_t)(strstr(source, "u->close") - source) + 3;
    bool first_found = false, second_found = false, unknown_found = false;
    bool first_write = false, second_write = false;
    for (int i = 0; i < result->resolved_calls.count; i++) {
        const CBMResolvedCall *row = &result->resolved_calls.items[i];
        if (row->source_origin != CBM_SOURCE_ORIGIN_RAW || !cbm_pipeline_lsp_field_row_proven(row))
            continue;
        if (row->site_start_byte == first && row->site_end_byte == first + 5 &&
            !strcmp(row->callee_qn, "proj.main.A.close"))
            first_found = true;
        if (row->site_start_byte == second && row->site_end_byte == second + 5 &&
            !strcmp(row->callee_qn, "proj.main.B.close"))
            second_found = true;
        if (row->site_start_byte == unknown)
            unknown_found = true;
    }
    for (int i = 0; i < result->rw.count; i++) {
        const CBMReadWrite *write = &result->rw.items[i];
        if (write->source_origin != CBM_SOURCE_ORIGIN_RAW || !write->is_member_access)
            continue;
        if (write->site_start_byte == first && write->site_end_byte == first + 5)
            first_write = true;
        if (write->site_start_byte == second && write->site_end_byte == second + 5)
            second_write = true;
    }
    cbm_free_result(result);
    ASSERT(first_found);
    ASSERT(second_found);
    ASSERT(first_write);
    ASSERT(second_write);
    ASSERT(!unknown_found);
    PASS();
}

TEST(resolver_policy_rejects_incomplete_and_uncertain_evidence) {
    cbm_resolve_result_t r = {
        .binding = CBM_RESOLVE_BINDING_UNIQUE,
        .domain = CBM_RESOLVE_DOMAIN_PROJECT,
        .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
        .rule = CBM_RESOLVE_RULE_TYPE_MEMBER,
        .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                 CBM_RESOLVE_FACT_RECEIVER_TYPED | CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
        .target = "proj.Type.member",
        .candidate_count = 1};
    ASSERT(cbm_resolve_admit(&r));
    r.facts &= ~CBM_RESOLVE_FACT_RECEIVER_TYPED;
    ASSERT(!cbm_resolve_admit(&r));
    r.facts |= CBM_RESOLVE_FACT_RECEIVER_TYPED;
    r.analysis = CBM_RESOLVE_ANALYSIS_LIMITED;
    ASSERT(!cbm_resolve_admit(&r));
    r.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE;
    r.binding = CBM_RESOLVE_BINDING_AMBIGUOUS;
    ASSERT(!cbm_resolve_admit(&r));
    r.binding = CBM_RESOLVE_BINDING_UNIQUE;
    r.domain = CBM_RESOLVE_DOMAIN_EXTERNAL;
    ASSERT(!cbm_resolve_admit(&r));
    PASS();
}

TEST(resolver_outcomes_preserve_occurrences_without_edges) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    cbm_resolve_counts_t counts = {0};
    bool present = true;
    ASSERT_EQ(cbm_store_resolver_summary(store, "proj", &counts, &present), CBM_STORE_OK);
    ASSERT(!present);
    cbm_resolve_record_t rows[] = {
        {.file_path = "main.rb",
         .scope = "proj.main",
         .relation = "CALLS",
         .spelling = "run",
         .start_byte = 10,
         .end_byte = 15,
         .result = {.reason = "unsupported_binding"}},
        {.file_path = "main.rb",
         .scope = "proj.main",
         .relation = "CALLS",
         .spelling = "run",
         .start_byte = 20,
         .end_byte = 25,
         .result = {.binding = CBM_RESOLVE_BINDING_AMBIGUOUS, .reason = "ambiguous_binding"}}};
    ASSERT_EQ(cbm_store_resolver_replace(store, "proj", rows, 2), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_resolver_summary(store, "proj", &counts, &present), CBM_STORE_OK);
    ASSERT(present);
    ASSERT_EQ(counts.occurrences, 2);
    ASSERT_EQ(counts.emitted, 0);
    ASSERT_EQ(counts.ambiguous, 1);
    ASSERT_EQ(counts.unresolved, 1);
    /* A falsely claimed edge aborts the replacement and retains both old rows. */
    rows[0].emitted = true;
    ASSERT_EQ(cbm_store_resolver_replace(store, "proj", rows, 1), CBM_STORE_ERR);
    ASSERT_EQ(cbm_store_resolver_summary(store, "proj", &counts, &present), CBM_STORE_OK);
    ASSERT_EQ(counts.occurrences, 2);
    cbm_store_close(store);
    PASS();
}

TEST(resolver_record_requires_exact_source_occurrence) {
    const char *source = "function caller(){ target(); }\nfunction target() {}\n";
    CBMFileResult *parsed = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT,
                                             "proj", "main.ts", 0, NULL, NULL);
    ASSERT_NOT_NULL(parsed);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &arena, 0, 0, 0);
    uint32_t start = (uint32_t)(strstr(source, "target();") - source);
    cbm_resolve_record_t row = {
        .file_path = "main.ts",
        .scope = "proj.main.caller",
        .relation = "CALLS",
        .spelling = "target",
        .start_byte = start,
        .end_byte = start + 6,
        .result = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                   .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                   .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                   .rule = CBM_RESOLVE_RULE_SCOPE_BINDING,
                   .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                            CBM_RESOLVE_FACT_SCOPE_BOUND | CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
                   .target = "proj.main.target",
                   .candidate_count = 1},
        .emitted = true};
    /* Flags alone remain insufficient. This fixed source has one hoisted
     * declaration and no competing binding in caller's lexical lookup. */
    ASSERT_FALSE(cbm_resolve_record_admit(&row));
    cbm_resolve_proof_source_t captured =
        cbm_result_proof_source(parsed, CBM_SOURCE_ORIGIN_RAW, row.scope);
    cbm_resolve_proof_key_t key = {.lookup_domain = row.scope,
                                   .lookup_key = row.spelling,
                                   .lookup_closed = true,
                                   .declaration_identity = row.result.target,
                                   .binding_identity = row.result.target,
                                   .start_byte = row.start_byte,
                                   .end_byte = row.end_byte,
                                   .detail = "CALLS"};
    const cbm_resolve_proof_t *audit =
        cbm_resolve_proof_leaf(&pool, CBM_PROOF_ADAPTER_DOMAIN, &captured, &key, &row.result);
    ASSERT_NOT_NULL(audit);
    row.result.proof = cbm_resolve_proof_derive(&pool, CBM_PROOF_REGISTRY_LOOKUP, &captured, &key,
                                                &row.result, &audit, 1, true);
    ASSERT_NOT_NULL(row.result.proof);
    row.content_identity = captured.content_identity;
    row.configuration_identity = captured.configuration_identity;
    cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp");
    ASSERT_NOT_NULL(gb);
    ASSERT(cbm_gbuf_record_resolution(gb, &row));
    ASSERT(cbm_gbuf_resolutions_own_shared(gb));
    size_t count = 0;
    const cbm_resolve_record_t *sealed = cbm_gbuf_resolutions(gb, &count);
    ASSERT_EQ(count, 1);
    row = sealed[0];
    ASSERT(cbm_resolve_record_admit(&row));
    row.end_byte = 0;
    ASSERT_FALSE(cbm_resolve_record_admit(&row));
    row.end_byte = start + 6;
    row.result.facts &= ~CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
    ASSERT_FALSE(cbm_resolve_record_admit(&row));
    row.result.facts |= CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
    row.source_origin = 2;
    ASSERT_FALSE(cbm_resolve_record_admit(&row));
    row.source_origin = 0;
    row.scope = "";
    ASSERT_FALSE(cbm_resolve_record_admit(&row));
    row.scope = "proj.main.caller";
    row.relation = "UNKNOWN_RELATION";
    ASSERT_FALSE(cbm_resolve_record_admit(&row));
    cbm_gbuf_free(gb);
    cbm_free_result(parsed);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_store_rejects_proof_without_occurrence_and_keeps_generation) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    cbm_resolve_record_t old = {.file_path = "main.ts",
                                .scope = "proj.main.caller",
                                .relation = "CALLS",
                                .result = {.reason = "unsupported_binding"}};
    ASSERT_EQ(cbm_store_resolver_replace(store, "proj", &old, 1), CBM_STORE_OK);
    cbm_resolve_record_t missing = old;
    missing.result = (cbm_resolve_result_t){
        .binding = CBM_RESOLVE_BINDING_UNIQUE,
        .domain = CBM_RESOLVE_DOMAIN_PROJECT,
        .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
        .rule = CBM_RESOLVE_RULE_SCOPE_BINDING,
        .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                 CBM_RESOLVE_FACT_SCOPE_BOUND | CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
        .target = "proj.lib.target",
        .candidate_count = 1};
    missing.emitted = true;
    ASSERT_EQ(cbm_store_resolver_replace(store, "proj", &missing, 1), CBM_STORE_ERR);
    cbm_resolve_counts_t counts;
    bool present = false;
    ASSERT_EQ(cbm_store_resolver_summary(store, "proj", &counts, &present), CBM_STORE_OK);
    ASSERT(present);
    ASSERT_EQ(counts.occurrences, 1);
    ASSERT_EQ(counts.emitted, 0);
    cbm_store_close(store);
    PASS();
}

static const char resolver_adapter_source[] = "target";

typedef struct {
    cbm_pipeline_ctx_t ctx;
    CBMArena arena;
    cbm_shared_resolver_adapter_t adapter;
    cbm_resolve_proof_source_t source;
    const cbm_resolve_proof_t *occurrence;
    int64_t owner, target;
} resolver_adapter_fixture_t;

static bool resolver_adapter_setup(resolver_adapter_fixture_t *f) {
    *f = (resolver_adapter_fixture_t){0};
    cbm_arena_init_lazy(&f->arena, 4096);
    f->ctx.project_name = "proj";
    f->ctx.gbuf = cbm_gbuf_new("proj", "/tmp");
    if (!f->ctx.gbuf)
        return false;
    f->owner = cbm_gbuf_upsert_node(f->ctx.gbuf, "Variable", "block", "proj.main.block", "main.tf",
                                    1, 1, "{}");
    f->target = cbm_gbuf_upsert_node(f->ctx.gbuf, "Variable", "target", "proj.lib.target", "lib.tf",
                                     1, 1, "{}");
    CBMFileResult identity = {0};
    if (!cbm_result_capture_raw_source(&identity, resolver_adapter_source, 6, CBM_LANG_HCL, NULL,
                                       NULL))
        return false;
    cbm_resolve_file_t file = {.file_path = "main.tf",
                               .module = "proj.main",
                               .raw_content_identity = identity.source_content_id[0],
                               .configuration_identity = identity.source_configuration_id,
                               .raw_length = 6,
                               .language = CBM_LANG_HCL,
                               .complete = true};
    if (!cbm_gbuf_record_resolver_file(f->ctx.gbuf, &file) ||
        !cbm_shared_resolver_adapter_init(&f->adapter, &f->ctx, &f->arena) ||
        !cbm_shared_resolver_adapter_capture(&f->adapter, "main.tf", CBM_LANG_HCL,
                                             resolver_adapter_source, 6, &f->source))
        return false;
    f->source.scope = "proj.main.block";
    f->occurrence = cbm_shared_resolver_adapter_occurrence(
        &f->adapter, &f->source, resolver_adapter_source, 6, 0, 6, "target", "USAGE");
    return f->occurrence != NULL;
}

static void resolver_adapter_cleanup(resolver_adapter_fixture_t *f) {
    cbm_shared_resolver_adapter_dispose(&f->adapter);
    cbm_arena_destroy(&f->arena);
    cbm_gbuf_free(f->ctx.gbuf);
}

static const cbm_resolve_proof_t *resolver_adapter_lookup(resolver_adapter_fixture_t *f,
                                                          bool closed, bool resolved) {
    cbm_resolve_proof_key_t key = f->occurrence->value.key;
    key.lookup_domain = "terraform:module";
    key.binding_identity = resolved ? "proj.lib.target" : "";
    key.lookup_closed = closed;
    cbm_resolve_result_t result = {
        .binding = resolved ? CBM_RESOLVE_BINDING_UNIQUE : CBM_RESOLVE_BINDING_UNRESOLVED,
        .domain = CBM_RESOLVE_DOMAIN_PROJECT,
        .analysis = closed ? CBM_RESOLVE_ANALYSIS_COMPLETE : CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
        .rule = CBM_RESOLVE_RULE_LANGUAGE_BINDING,
        .facts = resolved ? CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                                CBM_RESOLVE_FACT_LANGUAGE_BOUND
                          : 0,
        .target = key.binding_identity,
        .candidate_count = resolved ? 1 : 0,
        .reason = "fixture_independent_closed_declaration_lookup"};
    return cbm_resolve_proof_leaf(&f->adapter.proofs, CBM_PROOF_ADAPTER_DOMAIN, &f->source, &key,
                                  &result);
}

TEST(resolver_adapter_source_registration_precedes_capture_and_is_idempotent) {
    cbm_pipeline_ctx_t ctx = {.project_name = "proj"};
    ctx.gbuf = cbm_gbuf_new("proj", "/tmp");
    ASSERT_NOT_NULL(ctx.gbuf);
    CBMFileResult result = {.module_qn = "proj.main"};
    ASSERT(cbm_result_capture_raw_source(&result, resolver_adapter_source, 6, CBM_LANG_HCL, NULL,
                                         NULL));
    CBMFileResult *cache[] = {&result};
    cbm_file_info_t files[] = {{.rel_path = "main.tf", .language = CBM_LANG_HCL}};
    ASSERT(cbm_shared_resolver_capture_sources(&ctx, files, 1, cache));
    ASSERT(cbm_shared_resolver_capture_sources(&ctx, files, 1, cache));
    size_t count = 0;
    const cbm_resolve_file_t *snapshots = cbm_gbuf_resolver_files(ctx.gbuf, &count);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(snapshots[0].configuration_identity, result.source_configuration_id);
    ASSERT_STR_EQ(snapshots[0].raw_content_identity, result.source_content_id[0]);
    CBMArena arena;
    cbm_arena_init_lazy(&arena, 4096);
    cbm_shared_resolver_adapter_t adapter;
    cbm_resolve_proof_source_t source;
    ASSERT(cbm_shared_resolver_adapter_init(&adapter, &ctx, &arena));
    ASSERT(cbm_shared_resolver_adapter_capture(&adapter, "main.tf", CBM_LANG_HCL,
                                               resolver_adapter_source, 6, &source));
    ASSERT_STR_EQ(source.configuration_identity, result.source_configuration_id);
    result.source_configuration_id[0] = result.source_configuration_id[0] == '0' ? '1' : '0';
    ASSERT_FALSE(cbm_shared_resolver_capture_sources(&ctx, files, 1, cache));
    snapshots = cbm_gbuf_resolver_files(ctx.gbuf, &count);
    ASSERT_EQ(count, 1);
    ASSERT(strcmp(snapshots[0].configuration_identity, result.source_configuration_id));
    cbm_shared_resolver_adapter_dispose(&adapter);
    cbm_arena_destroy(&arena);
    cbm_gbuf_free(ctx.gbuf);
    PASS();
}

/* Fixed extraction facts exercise snapshot identity independently of an AST
 * adapter's supported grammar. Their strings deliberately remain mutable. */
typedef struct {
    cbm_pipeline_ctx_t ctx;
    CBMFileResult result;
    CBMFileResult *cache[1];
    cbm_file_info_t files[1];
    CBMDefinition definition;
    CBMImport import;
    CBMTypeAssign assignment;
    CBMFieldType field;
    const char *exports[2];
} resolver_source_fixture_t;

static bool resolver_source_fixture(resolver_source_fixture_t *f, bool extraction_error,
                                    bool incomplete_parse) {
    *f = (resolver_source_fixture_t){0};
    f->ctx.project_name = "proj";
    f->ctx.gbuf = cbm_gbuf_new("proj", "/tmp");
    f->definition = (CBMDefinition){.qualified_name = "proj.main.target",
                                    .name = "target",
                                    .label = "Function",
                                    .file_path = "main.py"};
    f->import = (CBMImport){.local_name = "item", .module_path = "library", .member = "item"};
    f->assignment = (CBMTypeAssign){
        .var_name = "value", .type_name = "Widget", .enclosing_func_qn = "proj.main.target"};
    f->field =
        (CBMFieldType){.class_qn = "proj.main.Widget", .field_name = "member", .type_text = "str"};
    f->exports[0] = "target";
    f->result = (CBMFileResult){.module_qn = "proj.main",
                                .source_project = "proj",
                                .source_file = "main.py",
                                .has_error = extraction_error,
                                .parse_incomplete = incomplete_parse,
                                .defs = {.items = &f->definition, .count = 1},
                                .imports = {.items = &f->import, .count = 1},
                                .type_assigns = {.items = &f->assignment, .count = 1},
                                .field_types = {.items = &f->field, .count = 1},
                                .exports = f->exports};
    f->cache[0] = &f->result;
    f->files[0] = (cbm_file_info_t){.rel_path = "main.py", .language = CBM_LANG_PYTHON};
    return f->ctx.gbuf &&
           cbm_result_capture_raw_source(&f->result, resolver_adapter_source, 6, CBM_LANG_PYTHON,
                                         NULL, NULL) &&
           cbm_shared_resolver_capture_sources(&f->ctx, f->files, 1, f->cache);
}

TEST(resolver_source_snapshot_survives_c_and_python_lsp_failure) {
    void (*failures[])(CBMFileResult *, CBMLSPStatus) = {cbm_c_lsp_record_failure,
                                                         cbm_py_lsp_record_failure};
    for (size_t i = 0; i < sizeof(failures) / sizeof(failures[0]); i++) {
        resolver_source_fixture_t f;
        ASSERT(resolver_source_fixture(&f, false, false));
        size_t count = 0;
        const cbm_resolve_file_t *before = cbm_gbuf_resolver_files(f.ctx.gbuf, &count);
        ASSERT_EQ(count, 1);
        ASSERT(before[0].complete);
        const char *digest = before[0].extracted_summary;
        failures[i](&f.result, CBM_LSP_DEPTH_EXCEEDED);
        ASSERT(f.result.has_error);
        ASSERT(f.result.lsp_skipped);
        ASSERT(f.result.lsp_failed);
        ASSERT(f.result.resolver_extraction_state.captured);
        ASSERT_FALSE(f.result.resolver_extraction_state.has_error);
        ASSERT(cbm_shared_resolver_capture_sources(&f.ctx, f.files, 1, f.cache));
        const cbm_resolve_file_t *after = cbm_gbuf_resolver_files(f.ctx.gbuf, &count);
        ASSERT_EQ(count, 1);
        ASSERT(after[0].complete);
        ASSERT_STR_EQ(after[0].extracted_summary, digest);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *bindings = cbm_resolve_binding_build(
            &arena, &f.result, CBM_LANG_PYTHON, resolver_adapter_source, 6);
        ASSERT_NOT_NULL(bindings);
        ASSERT_STR_EQ(cbm_resolve_binding_status(bindings), "incomplete_binding_source");
        cbm_arena_destroy(&arena);
        cbm_gbuf_free(f.ctx.gbuf);
    }
    PASS();
}

TEST(resolver_source_snapshot_rejects_changed_extraction_data_after_lsp_failure) {
    resolver_source_fixture_t f;
    ASSERT(resolver_source_fixture(&f, false, false));
    /* A broad budget-skip flag does not explain a new extraction error. */
    f.result.has_error = true;
    f.result.lsp_skipped = true;
    ASSERT_FALSE(cbm_shared_resolver_capture_sources(&f.ctx, f.files, 1, f.cache));
    f.result.has_error = false;
    f.result.lsp_skipped = false;
    cbm_py_lsp_record_failure(&f.result, CBM_LSP_DEPTH_EXCEEDED);
    ASSERT(cbm_shared_resolver_capture_sources(&f.ctx, f.files, 1, f.cache));
    const char **fields[] = {&f.definition.qualified_name, &f.import.module_path,
                             &f.assignment.type_name, &f.field.type_text, &f.exports[0]};
    for (size_t i = 0; i < sizeof(fields) / sizeof(fields[0]); i++) {
        const char *original = *fields[i];
        *fields[i] = "changed";
        ASSERT_FALSE(cbm_shared_resolver_capture_sources(&f.ctx, f.files, 1, f.cache));
        *fields[i] = original;
        ASSERT(cbm_shared_resolver_capture_sources(&f.ctx, f.files, 1, f.cache));
    }
    bool *flags[] = {&f.result.parse_incomplete, &f.result.parse_unusable,
                     &f.result.walk_truncated};
    for (size_t i = 0; i < sizeof(flags) / sizeof(flags[0]); i++) {
        *flags[i] = true;
        ASSERT_FALSE(cbm_shared_resolver_capture_sources(&f.ctx, f.files, 1, f.cache));
        *flags[i] = false;
    }
    char *identities[] = {f.result.source_content_id[CBM_SOURCE_ORIGIN_RAW],
                          f.result.source_configuration_id};
    for (size_t i = 0; i < sizeof(identities) / sizeof(identities[0]); i++) {
        char original = identities[i][0];
        identities[i][0] = original == '0' ? '1' : '0';
        ASSERT_FALSE(cbm_shared_resolver_capture_sources(&f.ctx, f.files, 1, f.cache));
        identities[i][0] = original;
    }
    f.result.source_content_length[CBM_SOURCE_ORIGIN_RAW]++;
    ASSERT_FALSE(cbm_shared_resolver_capture_sources(&f.ctx, f.files, 1, f.cache));
    f.result.source_content_length[CBM_SOURCE_ORIGIN_RAW]--;
    ASSERT(cbm_shared_resolver_capture_sources(&f.ctx, f.files, 1, f.cache));
    size_t count = 0;
    (void)cbm_gbuf_resolver_files(f.ctx.gbuf, &count);
    ASSERT_EQ(count, 1);
    cbm_gbuf_free(f.ctx.gbuf);
    PASS();
}

TEST(resolver_source_snapshot_preserves_initial_extraction_failure) {
    for (int parse = 0; parse < 2; parse++) {
        resolver_source_fixture_t f;
        ASSERT(resolver_source_fixture(&f, !parse, parse));
        cbm_py_lsp_record_failure(&f.result, CBM_LSP_DEPTH_EXCEEDED);
        ASSERT(cbm_shared_resolver_capture_sources(&f.ctx, f.files, 1, f.cache));
        size_t count = 0;
        const cbm_resolve_file_t *snapshot = cbm_gbuf_resolver_files(f.ctx.gbuf, &count);
        ASSERT_EQ(count, 1);
        ASSERT_FALSE(snapshot[0].complete);
        if (parse)
            f.result.parse_incomplete = false;
        else
            f.result.has_error = false;
        ASSERT_FALSE(cbm_shared_resolver_capture_sources(&f.ctx, f.files, 1, f.cache));
        cbm_gbuf_free(f.ctx.gbuf);
    }
    PASS();
}

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
TEST(resolver_source_snapshot_spill_roundtrip_and_failed_load) {
    const char *source = "def run():\n    return 1\n";
    CBMFileResult *parsed = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "proj",
                                             "main.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(parsed);
    ASSERT_FALSE(parsed->has_error);
    const char *directory = th_mktempdir("cbm_resolver_source_spill");
    ASSERT_NOT_NULL(directory);
    cbm_pipeline_ctx_t ctx = {.project_name = "proj"};
    ctx.gbuf = cbm_gbuf_new("proj", directory);
    ASSERT_NOT_NULL(ctx.gbuf);
    ctx.spill = cbm_result_spill_open(directory, 1, 2);
    ASSERT_NOT_NULL(ctx.spill);
    CBMFileResult *cache[] = {parsed, NULL};
    cbm_file_info_t files[] = {{.rel_path = "main.py", .language = CBM_LANG_PYTHON},
                               {.rel_path = "skipped.py", .language = CBM_LANG_PYTHON}};
    ASSERT(cbm_shared_resolver_capture_sources(&ctx, files, 2, cache));
    cbm_py_lsp_record_failure(parsed, CBM_LSP_DEPTH_EXCEEDED);
    cbm_result_compact(parsed);
    ASSERT_EQ(parsed->arena.nblocks, 1);
    ASSERT(cbm_result_spill_park(ctx.spill, 0, 0, parsed));
    cache[0] = NULL;
    parsed = cbm_result_spill_load(ctx.spill, 0);
    ASSERT_NOT_NULL(parsed);
    ASSERT(parsed->resolver_extraction_state.captured);
    ASSERT_FALSE(parsed->resolver_extraction_state.has_error);
    ASSERT(parsed->lsp_failed);
    ASSERT(parsed->has_error);
    cbm_free_result(parsed);
    ASSERT(cbm_shared_resolver_capture_sources(&ctx, files, 2, cache));
    size_t count = 0;
    const cbm_resolve_file_t *snapshot = cbm_gbuf_resolver_files(ctx.gbuf, &count);
    ASSERT_EQ(count, 1);
    ASSERT(snapshot[0].complete);
    /* Fail one acquisition at a real populated slot, then prove its data is
     * still readable; an intentionally empty second slot remains non-fatal. */
    ctx.test_fault_mask = CBM_PIPELINE_TEST_FAULT_RESOLVER_SPILL_NULL;
    ASSERT_FALSE(cbm_shared_resolver_capture_sources(&ctx, files, 2, cache));
    (void)cbm_gbuf_resolver_files(ctx.gbuf, &count);
    ASSERT_EQ(count, 1);
    ctx.test_fault_mask = 0;
    ASSERT(cbm_shared_resolver_capture_sources(&ctx, files, 2, cache));
    cbm_result_spill_close(ctx.spill);
    cbm_gbuf_free(ctx.gbuf);
    th_cleanup(directory);
    PASS();
}
#endif

TEST(resolver_lsp_failure_rejects_preexisting_semantic_proof) {
    const char *source = "def target(): pass\ndef caller(): target()\n";
    CBMFileResult *parsed = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "proj",
                                             "main.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(parsed);
    ASSERT_FALSE(parsed->has_error);
    parsed->source = cbm_arena_strdup(&parsed->arena, source);
    parsed->source_len = (int)strlen(source);
    cbm_pipeline_ctx_t ctx = {.project_name = "proj", .repo_path = "/tmp"};
    ctx.gbuf = cbm_gbuf_new("proj", "/tmp");
    ctx.registry = cbm_registry_new();
    ASSERT_NOT_NULL(ctx.gbuf);
    ASSERT_NOT_NULL(ctx.registry);
    cbm_gbuf_upsert_node(ctx.gbuf, "File", "main.py", "proj.main.__file__", "main.py", 1, 2, "{}");
    const char *caller = NULL, *target = NULL;
    for (int i = 0; i < parsed->defs.count; i++) {
        const CBMDefinition *definition = &parsed->defs.items[i];
        cbm_gbuf_upsert_node(ctx.gbuf, definition->label, definition->name,
                             definition->qualified_name, "main.py", definition->start_line,
                             definition->end_line, "{}");
        cbm_registry_add(ctx.registry, definition->name, definition->qualified_name,
                         definition->label);
        if (definition->name && !strcmp(definition->name, "caller"))
            caller = definition->qualified_name;
        if (definition->name && !strcmp(definition->name, "target"))
            target = definition->qualified_name;
    }
    ASSERT_NOT_NULL(caller);
    ASSERT_NOT_NULL(target);
    CBMFileResult *cache[] = {parsed};
    cbm_file_info_t files[] = {{.rel_path = "main.py", .language = CBM_LANG_PYTHON}};
    cbm_shared_resolver_t *shared = cbm_shared_resolver_build(&ctx, files, 1, cache);
    ASSERT_NOT_NULL(shared);
    /* Invocation carriers span the call expression, including its arguments.
     * Prove the fixture's exact site before attaching the synthetic LSP row. */
    ASSERT_EQ(parsed->calls.count, 1);
    const CBMCall *call = &parsed->calls.items[0];
    uint32_t start = (uint32_t)(strstr(source, "target()\n") - source);
    uint32_t end = start + (uint32_t)strlen("target()");
    ASSERT_EQ(call->site_start_byte, start);
    ASSERT_EQ(call->site_end_byte, end);
    ASSERT_STR_EQ(call->callee_name, "target");
    ASSERT_STR_EQ(call->enclosing_func_qn, caller);
    ASSERT_EQ(call->source_origin, CBM_SOURCE_ORIGIN_RAW);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &parsed->arena, 0, 0, 0);
    cbm_resolve_proof_source_t captured =
        cbm_result_proof_source(parsed, CBM_SOURCE_ORIGIN_RAW, caller);
    cbm_resolve_proof_key_t key = {.lookup_domain = caller,
                                   .lookup_key = "target",
                                   .lookup_closed = true,
                                   .declaration_identity = target,
                                   .binding_identity = target,
                                   .start_byte = start,
                                   .end_byte = end,
                                   .detail = "CALLS"};
    cbm_resolve_result_t outcome = {
        .binding = CBM_RESOLVE_BINDING_UNIQUE,
        .domain = CBM_RESOLVE_DOMAIN_PROJECT,
        .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
        .rule = CBM_RESOLVE_RULE_SCOPE_BINDING,
        .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                 CBM_RESOLVE_FACT_SCOPE_BOUND | CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
        .target = target,
        .candidate_count = 1};
    const cbm_resolve_proof_t *domain =
        cbm_resolve_proof_leaf(&pool, CBM_PROOF_ADAPTER_DOMAIN, &captured, &key, &outcome);
    ASSERT_NOT_NULL(domain);
    outcome.proof = cbm_resolve_proof_derive(&pool, CBM_PROOF_REGISTRY_LOOKUP, &captured, &key,
                                             &outcome, &domain, 1, true);
    ASSERT_NOT_NULL(outcome.proof);
    parsed->resolved_calls = (CBMResolvedCallArray){0};
    cbm_resolvedcall_push(&parsed->resolved_calls, &parsed->arena,
                          (CBMResolvedCall){.caller_qn = caller,
                                            .callee_qn = target,
                                            .kind = CBM_RESOLVED_INVOCATION,
                                            .site_start_byte = start,
                                            .site_end_byte = end,
                                            .source_origin = CBM_SOURCE_ORIGIN_RAW,
                                            .evidence = {.analysis = outcome.analysis,
                                                         .rule = outcome.rule,
                                                         .facts = outcome.facts,
                                                         .proof = outcome.proof}});
    cbm_resolver_file_t input = {.ctx = &ctx,
                                 .registry = ctx.registry,
                                 .graph = ctx.gbuf,
                                 .output = ctx.gbuf,
                                 .result = parsed,
                                 .rel_path = "main.py",
                                 .module_qn = parsed->module_qn,
                                 .language = CBM_LANG_PYTHON,
                                 .source = source,
                                 .source_len = strlen(source)};
    ASSERT(cbm_shared_resolver_file(shared, &input));
    /* Per-file candidates are admitted only after the normal coordinator
     * seals their occurrence proofs and projects the admitted relationships. */
    ASSERT(cbm_gbuf_resolutions_own_shared(ctx.gbuf));
    ASSERT(cbm_shared_resolver_project(ctx.gbuf));
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(ctx.gbuf, "CALLS"), 1);
    size_t count = 0;
    const cbm_resolve_record_t *records = cbm_gbuf_resolutions(ctx.gbuf, &count);
    bool admitted = false;
    for (size_t i = 0; i < count; i++)
        if (records[i].start_byte == start && !strcmp(records[i].relation, "CALLS")) {
            ASSERT_EQ(records[i].end_byte, end);
            ASSERT_STR_EQ(records[i].result.reason, "semantic_binding_evidence");
            ASSERT_STR_EQ(records[i].result.target, target);
            admitted |= records[i].emitted && cbm_resolve_record_admit(&records[i]);
        }
    ASSERT(admitted);
    cbm_py_lsp_record_failure(parsed, CBM_LSP_DEPTH_EXCEEDED);
    ASSERT(cbm_shared_resolver_capture_sources(&ctx, files, 1, cache));
    cbm_gbuf_t *failed = cbm_gbuf_new("proj", "/tmp");
    ASSERT_NOT_NULL(failed);
    input.output = failed;
    ASSERT(cbm_shared_resolver_file(shared, &input));
    ASSERT(cbm_gbuf_resolutions_own_shared(failed));
    ASSERT(cbm_shared_resolver_project(failed));
    records = cbm_gbuf_resolutions(failed, &count);
    bool retained = false;
    for (size_t i = 0; i < count; i++)
        if (records[i].start_byte == start && !strcmp(records[i].relation, "CALLS")) {
            retained = true;
            ASSERT_FALSE(records[i].emitted);
            ASSERT_EQ(records[i].result.analysis, CBM_RESOLVE_ANALYSIS_FAILED);
            ASSERT_STR_EQ(records[i].result.reason, "lsp_analysis_failed_prior_proof_untrusted");
            ASSERT_NULL(records[i].result.proof);
        }
    ASSERT(retained);
    ASSERT_EQ(cbm_gbuf_edge_count(failed), 0);
    cbm_gbuf_free(failed);
    cbm_gbuf_free(ctx.gbuf);
    cbm_shared_resolver_free(shared);
    cbm_registry_free(ctx.registry);
    cbm_free_result(parsed);
    PASS();
}

TEST(resolver_adapter_seals_original_occurrence_and_owned_lookup) {
    resolver_adapter_fixture_t f;
    ASSERT(resolver_adapter_setup(&f));
    const cbm_resolve_proof_t *lookup = resolver_adapter_lookup(&f, true, true);
    ASSERT_NOT_NULL(lookup);
    ASSERT(cbm_shared_resolver_adapter_record_proofs(&f.adapter, f.occurrence, f.owner, f.target,
                                                     "{}", lookup));
    cbm_shared_resolver_adapter_dispose(&f.adapter);
    cbm_arena_destroy(&f.arena);
    size_t count = 0;
    const cbm_resolve_record_t *records = cbm_gbuf_resolutions(f.ctx.gbuf, &count);
    ASSERT_EQ(count, 1);
    ASSERT(records[0].emitted);
    ASSERT(cbm_resolve_record_admit(&records[0]));
    ASSERT_EQ(records[0].start_byte, 0);
    ASSERT_EQ(records[0].end_byte, 6);
    ASSERT(cbm_shared_resolver_project(f.ctx.gbuf));
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(f.ctx.gbuf, "USAGE"), 1);
    cbm_gbuf_free(f.ctx.gbuf);
    PASS();
}

TEST(resolver_adapter_retains_missing_and_unsupported_outcomes) {
    resolver_adapter_fixture_t f;
    ASSERT(resolver_adapter_setup(&f));
    const cbm_resolve_proof_t *missing = resolver_adapter_lookup(&f, true, false);
    const cbm_resolve_proof_t *unsupported = resolver_adapter_lookup(&f, false, false);
    ASSERT(cbm_shared_resolver_adapter_record_proofs(&f.adapter, f.occurrence, f.owner, 0, "{}",
                                                     missing));
    ASSERT(cbm_shared_resolver_adapter_record_proofs(&f.adapter, f.occurrence, f.owner, 0, "{}",
                                                     unsupported));
    size_t count = 0;
    const cbm_resolve_record_t *records = cbm_gbuf_resolutions(f.ctx.gbuf, &count);
    ASSERT_EQ(count, 2);
    ASSERT_FALSE(records[0].emitted);
    ASSERT_EQ(records[0].result.binding, CBM_RESOLVE_BINDING_UNRESOLVED);
    ASSERT_EQ(records[0].result.analysis, CBM_RESOLVE_ANALYSIS_COMPLETE);
    ASSERT_FALSE(records[1].emitted);
    ASSERT_EQ(records[1].result.analysis, CBM_RESOLVE_ANALYSIS_UNSUPPORTED);
    ASSERT_EQ(cbm_gbuf_edge_count(f.ctx.gbuf), 0);
    ASSERT_FALSE(atomic_load(&f.ctx.resolver_failed));
    resolver_adapter_cleanup(&f);
    PASS();
}

TEST(resolver_adapter_rejects_source_only_and_target_substitution) {
    resolver_adapter_fixture_t f;
    ASSERT(resolver_adapter_setup(&f));
    ASSERT_FALSE(cbm_shared_resolver_adapter_record_proofs(&f.adapter, f.occurrence, f.owner,
                                                           f.target, "{}", f.occurrence));
    ASSERT(atomic_load(&f.ctx.resolver_failed));
    ASSERT_EQ(cbm_gbuf_edge_count(f.ctx.gbuf), 0);
    resolver_adapter_cleanup(&f);
    ASSERT(resolver_adapter_setup(&f));
    const cbm_resolve_proof_t *lookup = resolver_adapter_lookup(&f, true, true);
    int64_t wrong = cbm_gbuf_upsert_node(f.ctx.gbuf, "Variable", "target", "proj.other.target",
                                         "other.tf", 1, 1, "{}");
    ASSERT_FALSE(cbm_shared_resolver_adapter_record_proofs(&f.adapter, f.occurrence, f.owner, wrong,
                                                           "{}", lookup));
    ASSERT(atomic_load(&f.ctx.resolver_failed));
    ASSERT_EQ(cbm_gbuf_edge_count(f.ctx.gbuf), 0);
    resolver_adapter_cleanup(&f);
    PASS();
}

TEST(resolver_adapter_rejects_changed_capture_and_occurrence_spelling) {
    resolver_adapter_fixture_t f;
    ASSERT(resolver_adapter_setup(&f));
    cbm_resolve_proof_source_t source;
    ASSERT_FALSE(cbm_shared_resolver_adapter_capture(&f.adapter, "main.tf", CBM_LANG_HCL, "change",
                                                     6, &source));
    ASSERT(atomic_load(&f.ctx.resolver_failed));
    resolver_adapter_cleanup(&f);
    ASSERT(resolver_adapter_setup(&f));
    ASSERT_NULL(cbm_shared_resolver_adapter_occurrence(
        &f.adapter, &f.source, resolver_adapter_source, 6, 1, 6, "target", "USAGE"));
    ASSERT(atomic_load(&f.ctx.resolver_failed));
    ASSERT_EQ(cbm_gbuf_edge_count(f.ctx.gbuf), 0);
    resolver_adapter_cleanup(&f);
    PASS();
}

TEST(resolver_adapter_rejects_open_lookup_and_foreign_owner) {
    resolver_adapter_fixture_t f;
    ASSERT(resolver_adapter_setup(&f));
    const cbm_resolve_proof_t *valid = resolver_adapter_lookup(&f, true, true);
    cbm_resolve_proof_key_t key = valid->value.key;
    key.lookup_closed = false;
    const cbm_resolve_proof_t *open = cbm_resolve_proof_leaf(
        &f.adapter.proofs, CBM_PROOF_ADAPTER_DOMAIN, &f.source, &key, &valid->value.outcome);
    ASSERT_FALSE(cbm_shared_resolver_adapter_record_proofs(&f.adapter, f.occurrence, f.owner,
                                                           f.target, "{}", open));
    ASSERT(atomic_load(&f.ctx.resolver_failed));
    resolver_adapter_cleanup(&f);
    ASSERT(resolver_adapter_setup(&f));
    valid = resolver_adapter_lookup(&f, true, true);
    int64_t foreign = cbm_gbuf_upsert_node(f.ctx.gbuf, "Variable", "block", "proj.foreign.block",
                                           "foreign.tf", 1, 1, "{}");
    ASSERT_FALSE(cbm_shared_resolver_adapter_record_proofs(&f.adapter, f.occurrence, foreign,
                                                           f.target, "{}", valid));
    ASSERT_EQ(cbm_gbuf_edge_count(f.ctx.gbuf), 0);
    resolver_adapter_cleanup(&f);
    PASS();
}

TEST(resolver_adapter_capabilities_match_audited_infrastructure_subsets) {
    cbm_resolve_edge_capability_t edge;
    ASSERT(cbm_resolve_edge_capability(CBM_LANG_HCL, "IMPORTS", &edge));
    ASSERT_EQ(edge.availability, CBM_RESOLVE_EDGE_SUPPORTED_SUBSET);
    ASSERT_NOT_NULL(strstr(edge.limitation, "Terraform"));
    ASSERT_NOT_NULL(strstr(edge.limitation, "Terragrunt"));
    ASSERT(cbm_resolve_edge_capability(CBM_LANG_HCL, "USAGE", &edge));
    ASSERT_EQ(edge.availability, CBM_RESOLVE_EDGE_SUPPORTED_SUBSET);
    ASSERT(cbm_resolve_edge_capability(CBM_LANG_HCL, "WRITES", &edge));
    ASSERT_EQ(edge.availability, CBM_RESOLVE_EDGE_UNSUPPORTED);
    ASSERT(cbm_resolve_edge_capability(CBM_LANG_YAML, "IMPORTS", &edge));
    ASSERT_EQ(edge.availability, CBM_RESOLVE_EDGE_SUPPORTED_SUBSET);
    ASSERT_NOT_NULL(strstr(edge.limitation, "Ansible"));
    ASSERT(cbm_resolve_edge_capability(CBM_LANG_YAML, "CALLS", &edge));
    ASSERT_EQ(edge.availability, CBM_RESOLVE_EDGE_SUPPORTED_SUBSET);
    ASSERT_NOT_NULL(strstr(edge.limitation, "Ansible"));
    ASSERT(cbm_resolve_edge_capability(CBM_LANG_YAML, "WRITES", &edge));
    ASSERT_EQ(edge.availability, CBM_RESOLVE_EDGE_UNSUPPORTED);
    PASS();
}

SUITE(resolver_contract) {
    RUN_TEST(resolver_source_snapshot_survives_c_and_python_lsp_failure);
    RUN_TEST(resolver_source_snapshot_rejects_changed_extraction_data_after_lsp_failure);
    RUN_TEST(resolver_source_snapshot_preserves_initial_extraction_failure);
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    RUN_TEST(resolver_source_snapshot_spill_roundtrip_and_failed_load);
#endif
    RUN_TEST(resolver_lsp_failure_rejects_preexisting_semantic_proof);
    RUN_TEST(resolver_adapter_source_registration_precedes_capture_and_is_idempotent);
    RUN_TEST(resolver_adapter_capabilities_match_audited_infrastructure_subsets);
    RUN_TEST(resolver_adapter_seals_original_occurrence_and_owned_lookup);
    RUN_TEST(resolver_adapter_retains_missing_and_unsupported_outcomes);
    RUN_TEST(resolver_adapter_rejects_source_only_and_target_substitution);
    RUN_TEST(resolver_adapter_rejects_changed_capture_and_occurrence_spelling);
    RUN_TEST(resolver_adapter_rejects_open_lookup_and_foreign_owner);
    RUN_TEST(resolver_record_requires_exact_source_occurrence);
    RUN_TEST(resolver_store_rejects_proof_without_occurrence_and_keeps_generation);
    RUN_TEST(resolver_target_mapping_requires_exact_identity);
    RUN_TEST(resolver_unknown_receiver_never_uses_unique_field);
    RUN_TEST(resolver_aggregate_field_rows_cannot_choose_occurrence_owner);
    RUN_TEST(resolver_field_occurrences_keep_distinct_receivers_and_origins);
    RUN_TEST(resolver_field_occurrence_conflicts_are_not_ranked);
    RUN_TEST(resolver_c_fields_and_writes_keep_exact_occurrence_proof);
    RUN_TEST(resolver_policy_rejects_incomplete_and_uncertain_evidence);
    RUN_TEST(resolver_outcomes_preserve_occurrences_without_edges);
}
