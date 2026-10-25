#include "test_framework.h"
#include "pipeline/lsp_resolve.h"
#include "resolve/policy.h"
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
    cbm_resolve_record_t row = {
        .file_path = "main.ts",
        .scope = "proj.main.caller",
        .relation = "CALLS",
        .start_byte = 0,
        .end_byte = 6,
        .result = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                   .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                   .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                   .rule = CBM_RESOLVE_RULE_SCOPE_BINDING,
                   .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                            CBM_RESOLVE_FACT_SCOPE_BOUND | CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
                   .target = "proj.lib.target",
                   .candidate_count = 1},
        .emitted = true};
    ASSERT(cbm_resolve_record_admit(&row));
    row.end_byte = 0;
    ASSERT_FALSE(cbm_resolve_record_admit(&row));
    row.end_byte = 6;
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

SUITE(resolver_contract) {
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
