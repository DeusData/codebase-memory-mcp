#include "test_framework.h"
#include "cbm.h"
#include "graph_buffer/graph_buffer.h"
#include "resolve/proof.h"
#include "resolve/snapshot.h"
#include "mcp/mcp.h"
#include "store/store.h"
#include "store/store_resolver.h"
#include "sqlite3.h"
#include <yyjson/yyjson.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* Registration order deliberately differs from source occurrence order. The
 * admitted occurrence must never leak into the abstention pages. */
static int diagnostic_rows(cbm_store_t *store, const char *project) {
    cbm_resolve_record_t rows[] = {
        {.file_path = "z.c",
         .scope = "diag.z",
         .relation = "USAGE",
         .spelling = "external",
         .start_byte = 30,
         .end_byte = 38,
         .source_origin = 1,
         .result = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                    .domain = CBM_RESOLVE_DOMAIN_EXTERNAL,
                    .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                    .reason = "external_domain"}},
        {.file_path = "a.c",
         .scope = "diag.a",
         .relation = "CALLS",
         .spelling = "twenty",
         .start_byte = 20,
         .end_byte = 26,
         .result = {.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED, .reason = "unsupported_scope"}},
        {.file_path = "a.c",
         .scope = "diag.a",
         .relation = "CALLS",
         .spelling = "ten_b",
         .start_byte = 10,
         .end_byte = 15,
         .result = {.binding = CBM_RESOLVE_BINDING_AMBIGUOUS,
                    .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                    .candidate_count = 2,
                    .reason = "ambiguous_import"}},
        {.file_path = "a.c",
         .scope = "diag.a",
         .relation = "CALLS",
         .spelling = "ten_a",
         .start_byte = 10,
         .end_byte = 15,
         .result = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE, .reason = "missing_import"}},
        {.file_path = "b.c",
         .scope = "diag.b",
         .relation = "CALLS",
         .spelling = "limited",
         .start_byte = 5,
         .end_byte = 12,
         .result = {.analysis = CBM_RESOLVE_ANALYSIS_LIMITED, .reason = "work_limit"}},
        {.file_path = "a.c",
         .scope = "diag.a",
         .relation = "CALLS",
         .spelling = "admitted",
         .start_byte = 0,
         .end_byte = 8,
         .emitted = true,
         .result = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                    .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                    .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                    .rule = CBM_RESOLVE_RULE_SCOPE_BINDING,
                    .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                             CBM_RESOLVE_FACT_SCOPE_BOUND | CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
                    .target = "diag.target",
                    .candidate_count = 1,
                    .reason = "exact_scope"}}};
    const char *source = "void admitted(void) {}\nvoid caller(void) { admitted(); }\n";
    CBMFileResult *parsed =
        cbm_extract_file(source, (int)strlen(source), CBM_LANG_C, project, "a.c", 0, NULL, NULL);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &arena, 0, 0, 0);
    cbm_gbuf_t *gb = cbm_gbuf_new(project, "/tmp");
    cbm_resolve_file_t file;
    char digest[65], target[128], scope[128];
    bool ok = parsed && gb && cbm_resolve_snapshot(parsed, "a.c", CBM_LANG_C, &file, digest);
    if (ok) {
        cbm_resolve_record_t *admitted = &rows[sizeof(rows) / sizeof(rows[0]) - 1];
        snprintf(target, sizeof(target), "%s.a.admitted", project);
        snprintf(scope, sizeof(scope), "%s.a.caller", project);
        admitted->scope = scope;
        admitted->result.target = target;
        admitted->start_byte = (uint32_t)(strstr(source, "admitted();") - source);
        admitted->end_byte = admitted->start_byte + 8;
        admitted->content_identity = file.raw_content_identity;
        admitted->configuration_identity = file.configuration_identity;
        cbm_resolve_proof_source_t captured =
            cbm_result_proof_source(parsed, CBM_SOURCE_ORIGIN_RAW, scope);
        cbm_resolve_proof_key_t key = {.lookup_domain = scope,
                                       .lookup_key = "admitted",
                                       .lookup_closed = true,
                                       .declaration_identity = target,
                                       .start_byte = admitted->start_byte,
                                       .end_byte = admitted->end_byte,
                                       .detail = "audited fixed source scope"};
        const cbm_resolve_proof_t *audit = cbm_resolve_proof_leaf(
            &pool, CBM_PROOF_ADAPTER_DOMAIN, &captured, &key, &admitted->result);
        admitted->result.proof =
            audit ? cbm_resolve_proof_derive(&pool, CBM_PROOF_REGISTRY_LOOKUP, &captured, &key,
                                             &admitted->result, &audit, 1, true)
                  : NULL;
        ok = admitted->result.proof && cbm_gbuf_record_resolver_file(gb, &file);
        for (size_t i = 0; ok && i < sizeof(rows) / sizeof(rows[0]); i++)
            ok = cbm_gbuf_record_resolution(gb, &rows[i]);
        ok = ok && cbm_gbuf_resolutions_own_shared(gb);
    }
    int result = CBM_STORE_ERR;
    if (ok) {
        size_t count = 0;
        const cbm_resolve_record_t *sealed = cbm_gbuf_resolutions(gb, &count);
        result =
            cbm_store_resolver_replace_generation(store, project, sealed, count, &file, 1, NULL, 0);
    }
    cbm_gbuf_free(gb);
    cbm_free_result(parsed);
    cbm_arena_destroy(&arena);
    return result;
}

typedef struct {
    size_t count, calls, fail_at;
    char spelling[8][32];
    uint32_t starts[8];
    bool all_abstained;
} diagnostic_page_t;

static bool collect_diagnostic(const cbm_resolve_record_t *row, void *context) {
    diagnostic_page_t *page = context;
    size_t call = page->calls++;
    if (call == page->fail_at || page->count == 8)
        return false;
    snprintf(page->spelling[page->count], sizeof(page->spelling[0]), "%s", row->spelling);
    page->starts[page->count++] = row->start_byte;
    page->all_abstained &= !row->emitted;
    return true;
}

static diagnostic_page_t empty_page(void) {
    return (diagnostic_page_t){.fail_at = SIZE_MAX, .all_abstained = true};
}

TEST(resolver_diagnostics_store_pages_are_ordered_and_exclude_edges) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(diagnostic_rows(store, "diag"), CBM_STORE_OK);
    const char *expected[] = {"ten_a", "ten_b", "twenty", "limited", "external"};
    size_t total = 0;
    for (size_t offset = 0; offset < 5; offset += 2) {
        diagnostic_page_t page = empty_page();
        size_t returned = 99;
        ASSERT_EQ(cbm_store_resolver_visit_misses(store, "diag", offset, 2, collect_diagnostic,
                                                  &page, &returned),
                  CBM_STORE_OK);
        ASSERT_EQ(returned, offset == 4 ? 1 : 2);
        ASSERT_EQ(page.count, returned);
        ASSERT(page.all_abstained);
        for (size_t i = 0; i < returned; i++)
            ASSERT_STR_EQ(page.spelling[i], expected[offset + i]);
        total += returned;
    }
    ASSERT_EQ(total, 5);
    cbm_store_close(store);
    PASS();
}

TEST(resolver_diagnostics_store_isolates_projects_and_empty_pages) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(diagnostic_rows(store, "diag"), CBM_STORE_OK);
    cbm_resolve_record_t other = {.file_path = "a.c",
                                  .scope = "other.a",
                                  .relation = "CALLS",
                                  .spelling = "other_project",
                                  .start_byte = 1,
                                  .end_byte = 9,
                                  .result = {.reason = "other_project_only"}};
    ASSERT_EQ(cbm_store_resolver_replace(store, "other", &other, 1), CBM_STORE_OK);
    diagnostic_page_t page = empty_page();
    size_t returned = 99;
    ASSERT_EQ(
        cbm_store_resolver_visit_misses(store, "other", 0, 8, collect_diagnostic, &page, &returned),
        CBM_STORE_OK);
    ASSERT_EQ(returned, 1);
    ASSERT_STR_EQ(page.spelling[0], "other_project");
    const char *projects[] = {"diag", "missing"};
    for (size_t i = 0; i < 2; i++) {
        page = empty_page();
        ASSERT_EQ(cbm_store_resolver_visit_misses(store, projects[i], 99, 2, collect_diagnostic,
                                                  &page, &returned),
                  CBM_STORE_OK);
        ASSERT_EQ(returned, 0);
        ASSERT_EQ(page.calls, 0);
    }
    ASSERT_EQ(
        cbm_store_resolver_visit_misses(store, "diag", 0, 0, collect_diagnostic, &page, &returned),
        CBM_STORE_OK);
    ASSERT_EQ(returned, 0);
    cbm_store_close(store);
    PASS();
}

TEST(resolver_diagnostics_store_callback_failure_aborts_and_releases_statement) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(diagnostic_rows(store, "diag"), CBM_STORE_OK);
    diagnostic_page_t page = empty_page();
    page.fail_at = 1;
    size_t returned = 99;
    ASSERT_EQ(
        cbm_store_resolver_visit_misses(store, "diag", 0, 5, collect_diagnostic, &page, &returned),
        CBM_STORE_ERR);
    ASSERT_EQ(page.calls, 2);
    ASSERT_EQ(returned, 1);
    /* Replacement/read after cancellation verifies that no active statement
     * retains a lock and that the rejected row was not counted as delivered. */
    ASSERT_EQ(diagnostic_rows(store, "diag"), CBM_STORE_OK);
    page = empty_page();
    ASSERT_EQ(
        cbm_store_resolver_visit_misses(store, "diag", 0, 5, collect_diagnostic, &page, &returned),
        CBM_STORE_OK);
    ASSERT_EQ(returned, 5);
    ASSERT_STR_EQ(page.spelling[0], "ten_a");
    cbm_store_close(store);
    PASS();
}

static int deny_resolver_read(void *context, int action, const char *table, const char *column,
                              const char *database, const char *trigger) {
    (void)context;
    (void)column;
    (void)database;
    (void)trigger;
    return action == SQLITE_READ && table && strcmp(table, "resolver_occurrences") == 0
               ? SQLITE_DENY
               : SQLITE_OK;
}

TEST(resolver_diagnostics_store_read_failure_is_not_an_empty_page) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(diagnostic_rows(store, "diag"), CBM_STORE_OK);
    diagnostic_page_t page = empty_page();
    size_t returned = 99;
    ASSERT_EQ(sqlite3_set_authorizer(cbm_store_get_db(store), deny_resolver_read, NULL), SQLITE_OK);
    int rc =
        cbm_store_resolver_visit_misses(store, "diag", 0, 5, collect_diagnostic, &page, &returned);
    ASSERT_EQ(sqlite3_set_authorizer(cbm_store_get_db(store), NULL, NULL), SQLITE_OK);
    ASSERT_EQ(rc, CBM_STORE_ERR);
    ASSERT_EQ(page.calls, 0);
    ASSERT_EQ(returned, 0);
    cbm_store_close(store);
    PASS();
}

static cbm_mcp_server_t *diagnostic_server(bool rows) {
    cbm_mcp_server_t *server = cbm_mcp_server_new(NULL); /* in-memory; no user cache */
    cbm_store_t *store = server ? cbm_mcp_server_store(server) : NULL;
    bool ok = store && cbm_store_upsert_project(store, "diag", "/resolver-fixture") == CBM_STORE_OK;
    if (ok && rows)
        ok = diagnostic_rows(store, "diag") == CBM_STORE_OK;
    if (!ok) {
        cbm_mcp_server_free(server);
        return NULL;
    }
    cbm_mcp_server_set_project(server, "diag");
    return server;
}

static yyjson_doc *diagnostic_status(cbm_mcp_server_t *server, const char *arguments) {
    char *response = cbm_mcp_handle_tool(server, "index_status", arguments);
    yyjson_doc *outer = response ? yyjson_read(response, strlen(response), 0) : NULL;
    yyjson_val *root = outer ? yyjson_doc_get_root(outer) : NULL;
    yyjson_val *content = yyjson_obj_get(root, "content");
    const char *text = yyjson_get_str(yyjson_obj_get(yyjson_arr_get(content, 0), "text"));
    yyjson_doc *inner = text ? yyjson_read(text, strlen(text), 0) : NULL;
    yyjson_doc_free(outer);
    free(response);
    return inner;
}

static yyjson_val *resolver_report(yyjson_doc *doc) {
    return doc ? yyjson_obj_get(yyjson_doc_get_root(doc), "resolver") : NULL;
}

TEST(resolver_diagnostics_mcp_counts_and_page_shape) {
    cbm_mcp_server_t *server = diagnostic_server(true);
    ASSERT_NOT_NULL(server);
    yyjson_doc *doc =
        diagnostic_status(server, "{\"project\":\"diag\",\"format\":\"json\",\"diagnostics\":"
                                  "\"summary\",\"resolver_limit\":2}");
    ASSERT_NOT_NULL(doc);
    yyjson_val *report = resolver_report(doc);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(report, "status")), "available");
    ASSERT_EQ(yyjson_get_uint(yyjson_obj_get(report, "occurrences")), 6);
    ASSERT_EQ(yyjson_get_uint(yyjson_obj_get(report, "admitted")), 1);
    ASSERT_EQ(yyjson_get_uint(yyjson_obj_get(report, "not_admitted")), 5);
    ASSERT_EQ(yyjson_get_uint(yyjson_obj_get(report, "sample_total")), 5);
    ASSERT_EQ(yyjson_get_uint(yyjson_obj_get(report, "sample_returned")), 2);
    ASSERT_EQ(yyjson_get_uint(yyjson_obj_get(report, "sample_offset")), 0);
    ASSERT_EQ(yyjson_get_uint(yyjson_obj_get(report, "sample_limit")), 2);
    ASSERT(yyjson_get_bool(yyjson_obj_get(report, "has_more")));
    ASSERT_EQ(yyjson_get_uint(yyjson_obj_get(report, "next_offset")), 2);
    yyjson_val *rows = yyjson_obj_get(report, "samples");
    ASSERT_EQ(yyjson_arr_size(rows), 2);
    yyjson_val *row = yyjson_arr_get(rows, 1);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(row, "file")), "a.c");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(row, "scope")), "diag.a");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(row, "relation")), "CALLS");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(row, "spelling")), "ten_b");
    ASSERT_EQ(yyjson_get_uint(yyjson_obj_get(row, "start_byte")), 10);
    ASSERT_EQ(yyjson_get_uint(yyjson_obj_get(row, "end_byte")), 15);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(row, "binding")), "ambiguous");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(row, "analysis")), "complete");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(row, "domain")), "unknown");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(row, "reason")), "ambiguous_import");
    const char *interpretation = yyjson_get_str(yyjson_obj_get(report, "interpretation"));
    ASSERT_NOT_NULL(interpretation);
    ASSERT_NOT_NULL(strstr(interpretation, "not recall or precision measurements"));
    ASSERT_NULL(yyjson_obj_get(report, "recall"));
    ASSERT_NULL(yyjson_obj_get(report, "precision"));
    yyjson_doc_free(doc);
    doc = diagnostic_status(
        server,
        "{\"project\":\"diag\",\"format\":\"json\",\"resolver_offset\":2,\"resolver_limit\":999}");
    ASSERT_NOT_NULL(doc);
    report = resolver_report(doc);
    ASSERT_EQ(yyjson_get_uint(yyjson_obj_get(report, "sample_limit")), 500);
    ASSERT_EQ(yyjson_get_uint(yyjson_obj_get(report, "sample_returned")), 3);
    ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(report, "samples")), 3);
    ASSERT(!yyjson_get_bool(yyjson_obj_get(report, "has_more")));
    ASSERT_NULL(yyjson_obj_get(report, "next_offset"));
    yyjson_doc_free(doc);
    cbm_mcp_server_free(server);
    PASS();
}

TEST(resolver_diagnostics_mcp_absence_zero_generation_and_zero_limit) {
    cbm_mcp_server_t *server = diagnostic_server(false);
    ASSERT_NOT_NULL(server);
    yyjson_doc *doc = diagnostic_status(
        server, "{\"project\":\"diag\",\"format\":\"json\",\"diagnostics\":\"full\"}");
    ASSERT_NOT_NULL(doc);
    yyjson_val *report = resolver_report(doc);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(report, "status")), "unavailable");
    ASSERT_NULL(yyjson_obj_get(report, "occurrences"));
    ASSERT_NULL(yyjson_obj_get(report, "samples"));
    yyjson_doc_free(doc);
    ASSERT_EQ(cbm_store_resolver_replace(cbm_mcp_server_store(server), "diag", NULL, 0),
              CBM_STORE_OK);
    doc = diagnostic_status(
        server, "{\"project\":\"diag\",\"format\":\"json\",\"diagnostics\":\"summary\"}");
    ASSERT_NOT_NULL(doc);
    report = resolver_report(doc);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(report, "status")), "available");
    ASSERT_EQ(yyjson_get_uint(yyjson_obj_get(report, "sample_total")), 0);
    ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(report, "samples")), 0);
    ASSERT_EQ(yyjson_get_uint(yyjson_obj_get(report, "sample_limit")), 5);
    ASSERT(!yyjson_get_bool(yyjson_obj_get(report, "has_more")));
    yyjson_doc_free(doc);
    ASSERT_EQ(diagnostic_rows(cbm_mcp_server_store(server), "diag"), CBM_STORE_OK);
    const char *requests[] = {
        "{\"project\":\"diag\",\"format\":\"json\"}",
        "{\"project\":\"diag\",\"format\":\"json\",\"diagnostics\":\"full\",\"resolver_limit\":0}"};
    for (size_t i = 0; i < 2; i++) {
        doc = diagnostic_status(server, requests[i]);
        ASSERT_NOT_NULL(doc);
        report = resolver_report(doc);
        ASSERT_EQ(yyjson_get_uint(yyjson_obj_get(report, "not_admitted")), 5);
        ASSERT_NULL(yyjson_obj_get(report, "samples"));
        yyjson_doc_free(doc);
    }
    cbm_mcp_server_free(server);
    PASS();
}

TEST(resolver_diagnostics_mcp_unreadable_metadata_reports_error) {
    cbm_mcp_server_t *server = diagnostic_server(true);
    ASSERT_NOT_NULL(server);
    sqlite3 *db = cbm_store_get_db(cbm_mcp_server_store(server));
    ASSERT_EQ(sqlite3_set_authorizer(db, deny_resolver_read, NULL), SQLITE_OK);
    yyjson_doc *doc = diagnostic_status(
        server, "{\"project\":\"diag\",\"format\":\"json\",\"diagnostics\":\"summary\"}");
    ASSERT_EQ(sqlite3_set_authorizer(db, NULL, NULL), SQLITE_OK);
    ASSERT_NOT_NULL(doc);
    yyjson_val *report = resolver_report(doc);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(report, "status")), "error");
    ASSERT_NULL(yyjson_obj_get(report, "occurrences"));
    ASSERT_NULL(yyjson_obj_get(report, "samples"));
    yyjson_doc_free(doc);
    cbm_mcp_server_free(server);
    PASS();
}

TEST(resolver_diagnostics_capabilities_are_explicit_and_opt_in) {
    cbm_mcp_server_t *server = diagnostic_server(false);
    ASSERT_NOT_NULL(server);
    yyjson_doc *doc = diagnostic_status(
        server, "{\"project\":\"diag\",\"format\":\"json\",\"resolver_capabilities\":true}");
    ASSERT_NOT_NULL(doc);
    yyjson_val *rows = yyjson_obj_get(yyjson_doc_get_root(doc), "resolver_capabilities");
    ASSERT_EQ(yyjson_arr_size(rows), CBM_LANG_COUNT);
    yyjson_val *python = yyjson_arr_get(rows, CBM_LANG_PYTHON);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(python, "adapter")), "python");
    ASSERT(yyjson_arr_size(yyjson_obj_get(python, "supported_proof_subsets")) > 0);
    yyjson_val *swift = yyjson_arr_get(rows, CBM_LANG_SWIFT);
    ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(swift, "supported_proof_subsets")), 0);
    ASSERT_NOT_NULL(yyjson_get_str(yyjson_obj_get(swift, "limitation")));
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(resolver_report(doc), "status")), "unavailable");
    yyjson_doc_free(doc);
    doc = diagnostic_status(server, "{\"project\":\"diag\",\"format\":\"json\"}");
    ASSERT_NOT_NULL(doc);
    ASSERT_NULL(yyjson_obj_get(yyjson_doc_get_root(doc), "resolver_capabilities"));
    yyjson_doc_free(doc);
    cbm_mcp_server_free(server);
    PASS();
}

SUITE(resolver_diagnostics) {
    RUN_TEST(resolver_diagnostics_capabilities_are_explicit_and_opt_in);
    RUN_TEST(resolver_diagnostics_store_pages_are_ordered_and_exclude_edges);
    RUN_TEST(resolver_diagnostics_store_isolates_projects_and_empty_pages);
    RUN_TEST(resolver_diagnostics_store_callback_failure_aborts_and_releases_statement);
    RUN_TEST(resolver_diagnostics_store_read_failure_is_not_an_empty_page);
    RUN_TEST(resolver_diagnostics_mcp_counts_and_page_shape);
    RUN_TEST(resolver_diagnostics_mcp_absence_zero_generation_and_zero_limit);
    RUN_TEST(resolver_diagnostics_mcp_unreadable_metadata_reports_error);
}
