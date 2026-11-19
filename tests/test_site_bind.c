/*
 * test_site_bind.c -- the site hook of the shared cross-file layer
 * (src/pipeline/site_bind.h).
 *
 * A file of a language without a row is inactive: every call and usage site
 * of it gets NO_ANSWER (main's name chain decides, unchanged) and the end
 * step draws no edge. Go has an engine and never gets a row of this layer,
 * so these hold as rows for other languages arrive.
 */
#include "test_framework.h"
#include "../src/pipeline/site_bind.h"
#include "../src/graph_buffer/graph_buffer.h"

#include <stdint.h>
#include <string.h>

TEST(site_bind_language_without_row_answers_nothing) {
    cbm_gbuf_t *gb = cbm_gbuf_new("p", "/tmp/p");
    ASSERT_NOT_NULL(gb);
    int64_t caller = cbm_gbuf_upsert_node(gb, "Function", "f", "p.a.f", "a.go", 1, 3, "{}");
    int64_t callee = cbm_gbuf_upsert_node(gb, "Function", "g", "p.b.g", "b.go", 1, 3, "{}");
    ASSERT_GT(caller, 0);
    ASSERT_GT(callee, 0);
    const cbm_gbuf_node_t *source = cbm_gbuf_find_by_qn(gb, "p.a.f");
    ASSERT_NOT_NULL(source);

    CBMCall calls[1];
    memset(calls, 0, sizeof(calls));
    calls[0].callee_name = "g";
    calls[0].enclosing_func_qn = "p.a.f";
    CBMUsage usages[1];
    memset(usages, 0, sizeof(usages));
    usages[0].ref_name = "g";
    usages[0].enclosing_func_qn = "p.a.f";
    CBMFileResult result;
    memset(&result, 0, sizeof(result));
    result.calls.items = calls;
    result.calls.count = 1;
    result.usages.items = usages;
    result.usages.count = 1;

    cbm_site_file_t sf;
    cbm_pipeline_site_bind_begin(&sf, NULL, gb, "a.go", CBM_LANG_GO, &result);
    ASSERT_FALSE(sf.active);

    cbm_site_answer_t at_call = cbm_pipeline_site_bind_call(&sf, &calls[0], source);
    ASSERT_EQ(at_call.verdict, CBM_SITE_NO_ANSWER);
    ASSERT_NULL(at_call.res.qualified_name);
    ASSERT_NULL(at_call.target);

    cbm_site_answer_t at_usage = cbm_pipeline_site_bind_usage(&sf, &usages[0], source);
    ASSERT_EQ(at_usage.verdict, CBM_SITE_NO_ANSWER);
    ASSERT_NULL(at_usage.res.qualified_name);
    ASSERT_NULL(at_usage.target);

    int edges_before = cbm_gbuf_edge_count(gb);
    cbm_pipeline_site_bind_end(&sf, gb);
    ASSERT_EQ(cbm_gbuf_edge_count(gb), edges_before);

    cbm_gbuf_free(gb);
    PASS();
}

SUITE(site_bind) {
    RUN_TEST(site_bind_language_without_row_answers_nothing);
}
