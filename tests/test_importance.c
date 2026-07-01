/*
 * test_importance.c — AC1 (spec Part 1 / P3): index-time persisted per-symbol
 * importance score. Test plan: test-plan.md rows #1-#7, #10-#12 (fixture
 * suite). Rows #8/#9 (real-corpus connectors sweep) and #13 (weighted-degree
 * vs PageRank judgment-set decision) are manual/artifact checks run and
 * recorded outside this suite (builder-notes.md).
 *
 * Formula under test (pass_importance.c):
 *   importance = sqrt(num_refs) x priv x generic x distinct x test_penalty
 */
#include "test_framework.h"
#include "test_helpers.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "store/store.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/compat.h"
#include "yyjson/yyjson.h"

#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdatomic.h>

/* ── Shared helpers ───────────────────────────────────────────────────── */

/* Parse the numeric value of "importance" out of a properties_json blob.
 * Returns a sentinel (-999.0) if the key is absent -- callers must check
 * presence separately (strstr) before trusting the parsed value. */
static double extract_importance(const char *json) {
    if (!json) {
        return -999.0;
    }
    const char *p = strstr(json, "\"importance\":");
    if (!p) {
        return -999.0;
    }
    p += strlen("\"importance\":");
    return strtod(p, NULL);
}

static int write_files(const char *dir, const char *const *names, const char *const *contents,
                       int n) {
    for (int i = 0; i < n; i++) {
        char path[600];
        snprintf(path, sizeof(path), "%s/%s", dir, names[i]);
        if (th_write_file(path, contents[i]) != 0) {
            return -1;
        }
    }
    return 0;
}

/* ── Row #1: every Function/Method/Class node carries the score;
 * File nodes are excluded by design ─────────────────────────────────── */

TEST(importance_present_on_symbol_nodes_full_reindex) {
    char *tmp = th_mktempdir("cbm_imp1");
    if (!tmp) {
        FAIL("tmpdir");
    }
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "%s", tmp);

    const char *names[] = {"main.py"};
    const char *contents[] = {"def helper():\n    return 1\n\n"
                              "def caller():\n    return helper() + helper()\n\n"
                              "class Widget:\n    def render(self):\n        return helper()\n"};
    if (write_files(tmpdir, names, contents, 1) != 0) {
        th_rmtree(tmpdir);
        FAIL("write fixture");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/graph.db", tmpdir);
    cbm_pipeline_t *p = cbm_pipeline_new(tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    const char *project = cbm_pipeline_project_name(p);

    cbm_node_t *funcs = NULL;
    int fc = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_label(s, project, "Function", &funcs, &fc), CBM_STORE_OK);
    ASSERT_GT(fc, 0);
    for (int i = 0; i < fc; i++) {
        ASSERT_NOT_NULL(funcs[i].properties_json);
        ASSERT_TRUE(strstr(funcs[i].properties_json, "\"importance\":") != NULL);
    }
    cbm_store_free_nodes(funcs, fc);

    cbm_node_t *methods = NULL;
    int mc = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_label(s, project, "Method", &methods, &mc), CBM_STORE_OK);
    ASSERT_GT(mc, 0);
    for (int i = 0; i < mc; i++) {
        ASSERT_TRUE(strstr(methods[i].properties_json, "\"importance\":") != NULL);
    }
    cbm_store_free_nodes(methods, mc);

    cbm_node_t *classes = NULL;
    int cc = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_label(s, project, "Class", &classes, &cc), CBM_STORE_OK);
    ASSERT_GT(cc, 0);
    for (int i = 0; i < cc; i++) {
        ASSERT_TRUE(strstr(classes[i].properties_json, "\"importance\":") != NULL);
    }
    cbm_store_free_nodes(classes, cc);

    /* Design: File nodes are excluded -- the pass only touches
     * Function/Method/Class. */
    cbm_node_t *files = NULL;
    int filec = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_label(s, project, "File", &files, &filec), CBM_STORE_OK);
    for (int i = 0; i < filec; i++) {
        if (files[i].properties_json) {
            ASSERT_TRUE(strstr(files[i].properties_json, "\"importance\":") == NULL);
        }
    }
    cbm_store_free_nodes(files, filec);

    cbm_store_close(s);
    cbm_pipeline_free(p);
    th_rmtree(tmpdir);
    PASS();
}

/* ── Row #2: base = sqrt(num_refs), num_refs = incoming CALLS + USAGE,
 * including the num_refs == 0 floor ─────────────────────────────────── */

TEST(importance_base_sqrt_num_refs) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test-proj", "/tmp/test");
    ASSERT_NOT_NULL(gb);

    int64_t target =
        cbm_gbuf_upsert_node(gb, "Function", "target", "pkg.target", "pkg/main.go", 1, 1, "{}");
    int64_t caller1 =
        cbm_gbuf_upsert_node(gb, "Function", "caller1", "pkg.caller1", "pkg/main.go", 2, 2, "{}");
    int64_t caller2 =
        cbm_gbuf_upsert_node(gb, "Function", "caller2", "pkg.caller2", "pkg/main.go", 3, 3, "{}");
    int64_t consumer = cbm_gbuf_upsert_node(gb, "Function", "consumer", "pkg.consumer",
                                            "pkg/main.go", 4, 4, "{}");
    int64_t lonely =
        cbm_gbuf_upsert_node(gb, "Function", "lonelyfn", "pkg.lonelyfn", "pkg/main.go", 5, 5, "{}");
    ASSERT_GT(target, 0);
    ASSERT_GT(lonely, 0);

    cbm_gbuf_insert_edge(gb, caller1, target, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, caller2, target, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, consumer, target, "USAGE", "{}"); /* spans BOTH edge types */

    atomic_int cancelled = 0;
    cbm_pipeline_ctx_t ctx = {
        .project_name = "test-proj",
        .repo_path = "/tmp/test",
        .gbuf = gb,
        .registry = NULL,
        .cancelled = &cancelled,
    };
    cbm_pipeline_pass_importance(&ctx);

    const cbm_gbuf_node_t *tnode = cbm_gbuf_find_by_id(gb, target);
    ASSERT_NOT_NULL(tnode);
    ASSERT_TRUE(strstr(tnode->properties_json, "\"importance\":") != NULL);
    ASSERT_FLOAT_EQ(extract_importance(tnode->properties_json), sqrt(3.0), 1e-6);

    /* num_refs == 0 -> sqrt(0) == 0 floor */
    const cbm_gbuf_node_t *lnode = cbm_gbuf_find_by_id(gb, lonely);
    ASSERT_NOT_NULL(lnode);
    ASSERT_TRUE(strstr(lnode->properties_json, "\"importance\":") != NULL);
    ASSERT_FLOAT_EQ(extract_importance(lnode->properties_json), 0.0, 1e-9);

    cbm_gbuf_free(gb);
    PASS();
}

/* ── Row #3: x0.1 for private (leading-underscore) symbols ───────────── */

TEST(importance_private_multiplier) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test-proj", "/tmp/test");
    ASSERT_NOT_NULL(gb);

    int64_t priv_target =
        cbm_gbuf_upsert_node(gb, "Function", "_run", "pkg._run", "pkg/main.go", 1, 1, "{}");
    int64_t pub_target =
        cbm_gbuf_upsert_node(gb, "Function", "pubfn", "pkg.pubfn", "pkg/main.go", 2, 2, "{}");

    for (int i = 0; i < 4; i++) {
        char name[32], qn[48];
        snprintf(name, sizeof(name), "pcaller%d", i);
        snprintf(qn, sizeof(qn), "pkg.pcaller%d", i);
        int64_t c = cbm_gbuf_upsert_node(gb, "Function", name, qn, "pkg/main.go", 10 + i, 10 + i,
                                         "{}");
        cbm_gbuf_insert_edge(gb, c, priv_target, "CALLS", "{}");
    }
    for (int i = 0; i < 4; i++) {
        char name[32], qn[48];
        snprintf(name, sizeof(name), "qcaller%d", i);
        snprintf(qn, sizeof(qn), "pkg.qcaller%d", i);
        int64_t c = cbm_gbuf_upsert_node(gb, "Function", name, qn, "pkg/main.go", 20 + i, 20 + i,
                                         "{}");
        cbm_gbuf_insert_edge(gb, c, pub_target, "CALLS", "{}");
    }

    atomic_int cancelled = 0;
    cbm_pipeline_ctx_t ctx = {
        .project_name = "test-proj", .repo_path = "/tmp/test", .gbuf = gb, .cancelled = &cancelled};
    cbm_pipeline_pass_importance(&ctx);

    const cbm_gbuf_node_t *priv_node = cbm_gbuf_find_by_id(gb, priv_target);
    const cbm_gbuf_node_t *pub_node = cbm_gbuf_find_by_id(gb, pub_target);
    ASSERT_FLOAT_EQ(extract_importance(priv_node->properties_json), sqrt(4.0) * 0.1, 1e-6);
    ASSERT_FLOAT_EQ(extract_importance(pub_node->properties_json), sqrt(4.0), 1e-6);

    cbm_gbuf_free(gb);
    PASS();
}

/* ── Row #4: x0.1 when a name is DEFINED in >=5 DISTINCT files (not nodes);
 * boundary N-1 (4 files) vs N (5 files) ──────────────────────────────── */

TEST(importance_generic_name_multiplier) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test-proj", "/tmp/test");
    ASSERT_NOT_NULL(gb);

    /* "clientx": defined in exactly 5 DISTINCT files -> generic. */
    int64_t generic_target = 0;
    for (int i = 0; i < 5; i++) {
        char qn[48], file[48];
        snprintf(qn, sizeof(qn), "pkg%d.clientx", i);
        snprintf(file, sizeof(file), "pkg%d/client.go", i);
        int64_t id = cbm_gbuf_upsert_node(gb, "Function", "clientx", qn, file, 1, 1, "{}");
        if (i == 4) {
            generic_target = id; /* the one we'll assert on */
        }
    }
    int64_t g_caller =
        cbm_gbuf_upsert_node(gb, "Function", "gcaller", "pkg.gcaller", "pkg/main.go", 1, 1, "{}");
    cbm_gbuf_insert_edge(gb, g_caller, generic_target, "CALLS", "{}");

    /* "widgetxx": defined in exactly 4 DISTINCT files (N-1 boundary) -> NOT generic. */
    int64_t boundary_target = 0;
    for (int i = 0; i < 4; i++) {
        char qn[48], file[48];
        snprintf(qn, sizeof(qn), "wpkg%d.widgetxx", i);
        snprintf(file, sizeof(file), "wpkg%d/widget.go", i);
        int64_t id = cbm_gbuf_upsert_node(gb, "Function", "widgetxx", qn, file, 1, 1, "{}");
        if (i == 3) {
            boundary_target = id;
        }
    }
    int64_t b_caller =
        cbm_gbuf_upsert_node(gb, "Function", "bcaller", "pkg.bcaller", "pkg/main.go", 2, 2, "{}");
    cbm_gbuf_insert_edge(gb, b_caller, boundary_target, "CALLS", "{}");

    /* "helperyy": 5 total definitions but only 4 DISTINCT files (2 defs share
     * fileA) -> must count distinct FILES, not distinct nodes -> NOT generic. */
    int64_t dup_target = cbm_gbuf_upsert_node(gb, "Function", "helperyy", "fileA.helperyy_1",
                                              "fileA.go", 1, 1, "{}");
    cbm_gbuf_upsert_node(gb, "Function", "helperyy", "fileA.helperyy_2", "fileA.go", 5, 5, "{}");
    cbm_gbuf_upsert_node(gb, "Function", "helperyy", "fileB.helperyy", "fileB.go", 1, 1, "{}");
    cbm_gbuf_upsert_node(gb, "Function", "helperyy", "fileC.helperyy", "fileC.go", 1, 1, "{}");
    cbm_gbuf_upsert_node(gb, "Function", "helperyy", "fileD.helperyy", "fileD.go", 1, 1, "{}");
    int64_t d_caller =
        cbm_gbuf_upsert_node(gb, "Function", "dcaller", "pkg.dcaller", "pkg/main.go", 3, 3, "{}");
    cbm_gbuf_insert_edge(gb, d_caller, dup_target, "CALLS", "{}");

    atomic_int cancelled = 0;
    cbm_pipeline_ctx_t ctx = {
        .project_name = "test-proj", .repo_path = "/tmp/test", .gbuf = gb, .cancelled = &cancelled};
    cbm_pipeline_pass_importance(&ctx);

    const cbm_gbuf_node_t *gn = cbm_gbuf_find_by_id(gb, generic_target);
    const cbm_gbuf_node_t *bn = cbm_gbuf_find_by_id(gb, boundary_target);
    const cbm_gbuf_node_t *dn = cbm_gbuf_find_by_id(gb, dup_target);
    ASSERT_FLOAT_EQ(extract_importance(gn->properties_json), sqrt(1.0) * 0.1, 1e-6);
    ASSERT_FLOAT_EQ(extract_importance(bn->properties_json), sqrt(1.0), 1e-6);
    ASSERT_FLOAT_EQ(extract_importance(dn->properties_json), sqrt(1.0), 1e-6);

    cbm_gbuf_free(gb);
    PASS();
}

/* ── Row #5: x10 for distinctive (snake_case OR camelCase) AND len>=8;
 * len==8 vs len==7 boundary; a plain len>=8 lowercase word gets no bonus ── */

TEST(importance_distinctive_identifier_multiplier) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test-proj", "/tmp/test");
    ASSERT_NOT_NULL(gb);

    struct {
        const char *name;
        double expected_multiplier;
    } cases[] = {
        {"make_proposals", 10.0}, /* snake, len 14 */
        {"parseInvoice", 10.0},   /* camel, len 12 */
        {"run", 1.0},             /* len < 8 */
        {"now", 1.0},             /* len < 8 */
        {"ab_cdefg", 10.0},       /* snake, len == 8 (boundary: bonus) */
        {"ab_cdef", 1.0},         /* snake, len == 7 (boundary: no bonus) */
        {"duplicate", 1.0},       /* len 9, all-lowercase, neither snake nor camel */
    };
    enum { N_CASES = 7 };
    int64_t ids[N_CASES];

    for (int i = 0; i < N_CASES; i++) {
        char qn[64];
        snprintf(qn, sizeof(qn), "pkg.%s", cases[i].name);
        ids[i] =
            cbm_gbuf_upsert_node(gb, "Function", cases[i].name, qn, "pkg/main.go", i + 1, i + 1, "{}");
        char cname[32], cqn[64];
        snprintf(cname, sizeof(cname), "caller_of_%d", i);
        snprintf(cqn, sizeof(cqn), "pkg.caller_of_%d", i);
        int64_t caller =
            cbm_gbuf_upsert_node(gb, "Function", cname, cqn, "pkg/main.go", 100 + i, 100 + i, "{}");
        cbm_gbuf_insert_edge(gb, caller, ids[i], "CALLS", "{}");
    }

    atomic_int cancelled = 0;
    cbm_pipeline_ctx_t ctx = {
        .project_name = "test-proj", .repo_path = "/tmp/test", .gbuf = gb, .cancelled = &cancelled};
    cbm_pipeline_pass_importance(&ctx);

    for (int i = 0; i < N_CASES; i++) {
        const cbm_gbuf_node_t *n = cbm_gbuf_find_by_id(gb, ids[i]);
        ASSERT_NOT_NULL(n);
        /* num_refs == 1 for every case -> importance == distinct multiplier directly. */
        ASSERT_FLOAT_EQ(extract_importance(n->properties_json), cases[i].expected_multiplier, 1e-6);
    }

    cbm_gbuf_free(gb);
    PASS();
}

/* ── Row #6: edge-based test penalty (TESTS/TESTS_FILE), NOT filename
 * strstr -- central port claim + inversion/discriminating checks ───── */

TEST(importance_test_penalty_edge_based) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test-proj", "/tmp/test");
    ASSERT_NOT_NULL(gb);

    /* (a) real test helper in a NON-test-named file, called directly by a
     * Test-named function -> gets a TESTS edge as TARGET -> penalized.
     * Proves edge-based detection catches what strstr would miss. */
    int64_t helper = cbm_gbuf_upsert_node(gb, "Function", "make_helper_fn", "pkg.make_helper_fn",
                                          "testutil.go", 1, 1, "{}");
    int64_t test_fn = cbm_gbuf_upsert_node(gb, "Function", "TestSomething", "pkg.TestSomething",
                                           "foo_test.go", 1, 1, "{}");
    cbm_gbuf_insert_edge(gb, test_fn, helper, "TESTS", "{}");
    int64_t helper_caller = cbm_gbuf_upsert_node(gb, "Function", "othercaller", "pkg.othercaller",
                                                 "pkg/main.go", 1, 1, "{}");
    cbm_gbuf_insert_edge(gb, helper_caller, helper, "CALLS", "{}");

    /* Control: same shape (snake, len>=8, num_refs=1) but NO TESTS edge. */
    int64_t control = cbm_gbuf_upsert_node(gb, "Function", "make_control_fn", "pkg.make_control_fn",
                                           "pkg/prod.go", 1, 1, "{}");
    int64_t control_caller = cbm_gbuf_upsert_node(gb, "Function", "ctrlcaller", "pkg.ctrlcaller",
                                                  "pkg/main.go", 2, 2, "{}");
    cbm_gbuf_insert_edge(gb, control_caller, control, "CALLS", "{}");

    /* (b) inversion/discriminating check: PATH contains "test" as a
     * substring but there is NO TESTS/TESTS_FILE edge -> must NOT be
     * penalized (proves the strstr path was truly replaced). */
    int64_t path_substr = cbm_gbuf_upsert_node(gb, "Function", "latest_helper_fn",
                                               "pkg.latest_helper_fn", "src/testutil_helpers.go", 1,
                                               1, "{}");
    int64_t ps_caller =
        cbm_gbuf_upsert_node(gb, "Function", "pscaller", "pkg.pscaller", "pkg/main.go", 3, 3, "{}");
    cbm_gbuf_insert_edge(gb, ps_caller, path_substr, "CALLS", "{}");

    /* (c) a helper colocated in a genuine test file, detected via the
     * TESTS_FILE file-level edge (pass_tests never emits a direct TESTS
     * edge here because the target already lives in a test-classified
     * file -- see create_tests_edges' tgt_is_test exclusion). */
    int64_t test_file_node =
        cbm_gbuf_upsert_node(gb, "File", "recA_test.go", "pkg.__file__.recA_test.go",
                             "recA_test.go", 0, 0, "{}");
    int64_t prod_file_node = cbm_gbuf_upsert_node(gb, "File", "recA.go", "pkg.__file__.recA.go",
                                                  "recA.go", 0, 0, "{}");
    cbm_gbuf_insert_edge(gb, test_file_node, prod_file_node, "TESTS_FILE", "{}");
    int64_t seed_fn = cbm_gbuf_upsert_node(gb, "Function", "seed_recording_fn",
                                           "pkg.seed_recording_fn", "recA_test.go", 5, 5, "{}");
    int64_t seed_caller =
        cbm_gbuf_upsert_node(gb, "Function", "seedcaller", "pkg.seedcaller", "pkg/main.go", 4, 4,
                             "{}");
    cbm_gbuf_insert_edge(gb, seed_caller, seed_fn, "CALLS", "{}");

    atomic_int cancelled = 0;
    cbm_pipeline_ctx_t ctx = {
        .project_name = "test-proj", .repo_path = "/tmp/test", .gbuf = gb, .cancelled = &cancelled};
    cbm_pipeline_pass_importance(&ctx);

    /* num_refs=1, distinct(snake,len>=8)=10 for all of these -> the only
     * variable is the test_penalty factor (0.1 penalized, 1.0 not). */
    ASSERT_FLOAT_EQ(extract_importance(cbm_gbuf_find_by_id(gb, helper)->properties_json),
                   sqrt(1.0) * 10.0 * 0.1, 1e-6);
    ASSERT_FLOAT_EQ(extract_importance(cbm_gbuf_find_by_id(gb, control)->properties_json),
                   sqrt(1.0) * 10.0, 1e-6);
    ASSERT_FLOAT_EQ(extract_importance(cbm_gbuf_find_by_id(gb, path_substr)->properties_json),
                   sqrt(1.0) * 10.0, 1e-6); /* NOT penalized despite "test" substring */
    ASSERT_FLOAT_EQ(extract_importance(cbm_gbuf_find_by_id(gb, seed_fn)->properties_json),
                   sqrt(1.0) * 10.0 * 0.1, 1e-6); /* penalized via TESTS_FILE membership */

    cbm_gbuf_free(gb);
    PASS();
}

/* ── Row #7: the pass runs AFTER pass_tests and CALLS/USAGE edges exist
 * (full-pipeline ordering check, real pass_tests edge creation) ────── */

TEST(importance_ordering_after_tests_and_calls) {
    char *tmp = th_mktempdir("cbm_imp7");
    if (!tmp) {
        FAIL("tmpdir");
    }
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "%s", tmp);

    const char *names[] = {"helpers.go", "main_test.go"};
    const char *contents[] = {
        "package main\n\nfunc make_fixture_fn() {}\n",
        "package main\n\nimport \"testing\"\n\nfunc TestFixture(t *testing.T) { make_fixture_fn() }\n"};
    if (write_files(tmpdir, names, contents, 2) != 0) {
        th_rmtree(tmpdir);
        FAIL("write fixture");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/graph.db", tmpdir);
    cbm_pipeline_t *p = cbm_pipeline_new(tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    const char *project = cbm_pipeline_project_name(p);

    cbm_node_t *funcs = NULL;
    int fc = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_label(s, project, "Function", &funcs, &fc), CBM_STORE_OK);
    const cbm_node_t *fixture = NULL;
    for (int i = 0; i < fc; i++) {
        if (strcmp(funcs[i].name, "make_fixture_fn") == 0) {
            fixture = &funcs[i];
            break;
        }
    }
    ASSERT_NOT_NULL(fixture);
    /* Only called by TestFixture (num_refs=1), snake+len>=8 (distinct=10),
     * called-by-test (test_penalty=0.1) -> exactly 1.0. A mis-ordered pass
     * would see num_refs=0 (score 0) or test_penalty=1.0 (score 10) instead
     * -- either misordering is distinguishable from the expected 1.0. */
    ASSERT_TRUE(strstr(fixture->properties_json, "\"importance\":") != NULL);
    ASSERT_FLOAT_EQ(extract_importance(fixture->properties_json), 1.0, 1e-6);

    cbm_store_free_nodes(funcs, fc);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    th_rmtree(tmpdir);
    PASS();
}

/* ── Row #10: persisted score survives dump -> SQLite -> read-back ──── */

TEST(importance_round_trip_persist) {
    char *tmp = th_mktempdir("cbm_imp10");
    if (!tmp) {
        FAIL("tmpdir");
    }
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "%s", tmp);
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/roundtrip.db", tmpdir);

    cbm_store_t *s1 = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s1);
    cbm_store_upsert_project(s1, "proj", tmpdir);
    cbm_node_t n = {.project = "proj",
                    .label = "Function",
                    .name = "scored_fn",
                    .qualified_name = "proj.scored_fn",
                    .file_path = "f.go",
                    .properties_json = "{\"importance\":3.140000}"};
    int64_t id = cbm_store_upsert_node(s1, &n);
    ASSERT_GT(id, 0);
    cbm_store_checkpoint(s1);
    cbm_store_close(s1);

    cbm_store_t *s2 = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s2);
    cbm_node_t out = {0};
    ASSERT_EQ(cbm_store_find_node_by_qn(s2, "proj", "proj.scored_fn", &out), CBM_STORE_OK);
    ASSERT_NOT_NULL(out.properties_json);
    ASSERT_TRUE(strstr(out.properties_json, "\"importance\":") != NULL);
    ASSERT_FLOAT_EQ(extract_importance(out.properties_json), 3.14, 1e-6);
    cbm_node_free_fields(&out);
    cbm_store_close(s2);

    th_rmtree(tmpdir);
    PASS();
}

/* ── Row #11: AC7 migration -- a pre-feature row reads absent/null without
 * crashing; re-indexing (rewriting the row) makes the score present ─── */

TEST(importance_migration_null_until_reindex) {
    char *tmp = th_mktempdir("cbm_imp11");
    if (!tmp) {
        FAIL("tmpdir");
    }
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "%s", tmp);
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/legacy.db", tmpdir);

    /* Simulate a pre-feature index: a node with NO $.importance key. */
    cbm_store_t *s1 = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s1);
    cbm_store_upsert_project(s1, "proj", tmpdir);
    cbm_node_t legacy = {.project = "proj",
                         .label = "Function",
                         .name = "legacy_fn",
                         .qualified_name = "proj.legacy_fn",
                         .file_path = "f.go",
                         .properties_json = "{}"};
    ASSERT_GT(cbm_store_upsert_node(s1, &legacy), 0);
    cbm_store_checkpoint(s1);
    cbm_store_close(s1);

    /* Reopen: absent key reads cleanly, no crash, no error. */
    cbm_store_t *s2 = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s2);
    cbm_node_t out1 = {0};
    ASSERT_EQ(cbm_store_find_node_by_qn(s2, "proj", "proj.legacy_fn", &out1), CBM_STORE_OK);
    ASSERT_NOT_NULL(out1.properties_json);
    ASSERT_TRUE(strstr(out1.properties_json, "\"importance\":") == NULL);
    cbm_node_free_fields(&out1);

    /* Re-index: rewrite the row with the score present (upsert by same QN). */
    cbm_node_t reindexed = {.project = "proj",
                            .label = "Function",
                            .name = "legacy_fn",
                            .qualified_name = "proj.legacy_fn",
                            .file_path = "f.go",
                            .properties_json = "{\"importance\":5.000000}"};
    ASSERT_GT(cbm_store_upsert_node(s2, &reindexed), 0);
    cbm_store_checkpoint(s2);
    cbm_store_close(s2);

    cbm_store_t *s3 = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s3);
    cbm_node_t out2 = {0};
    ASSERT_EQ(cbm_store_find_node_by_qn(s3, "proj", "proj.legacy_fn", &out2), CBM_STORE_OK);
    ASSERT_TRUE(strstr(out2.properties_json, "\"importance\":") != NULL);
    ASSERT_FLOAT_EQ(extract_importance(out2.properties_json), 5.0, 1e-6);
    cbm_node_free_fields(&out2);
    cbm_store_close(s3);

    th_rmtree(tmpdir);
    PASS();
}

/* ── Row #12: properties_json stays valid JSON after the append; the
 * append guard refuses to touch an already-malformed object ────────── */

TEST(importance_properties_json_valid_after_append) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test-proj", "/tmp/test");
    ASSERT_NOT_NULL(gb);

    int64_t a = cbm_gbuf_upsert_node(gb, "Function", "alpha_fn", "pkg.alpha_fn", "pkg/a.go", 1, 1,
                                     "{}");
    int64_t b = cbm_gbuf_upsert_node(gb, "Function", "beta_fn", "pkg.beta_fn", "pkg/b.go", 1, 1,
                                     "{\"docstring\":\"hi\"}");
    int64_t caller =
        cbm_gbuf_upsert_node(gb, "Function", "caller_fn", "pkg.caller_fn", "pkg/c.go", 1, 1, "{}");
    cbm_gbuf_insert_edge(gb, caller, a, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, caller, b, "CALLS", "{}");

    atomic_int cancelled = 0;
    cbm_pipeline_ctx_t ctx = {
        .project_name = "test-proj", .repo_path = "/tmp/test", .gbuf = gb, .cancelled = &cancelled};
    cbm_pipeline_pass_importance(&ctx);

    /* Mutation/inversion-sensitive: any future edit that breaks the append's
     * buffer sizing or escaping and produces malformed JSON fails HERE. */
    const int64_t ids[] = {a, b};
    for (int i = 0; i < 2; i++) {
        const cbm_gbuf_node_t *n = cbm_gbuf_find_by_id(gb, ids[i]);
        ASSERT_NOT_NULL(n->properties_json);
        yyjson_doc *doc = yyjson_read(n->properties_json, strlen(n->properties_json), 0);
        ASSERT_NOT_NULL(doc);
        yyjson_val *root = yyjson_doc_get_root(doc);
        ASSERT_TRUE(yyjson_is_obj(root));
        ASSERT_NOT_NULL(yyjson_obj_get(root, "importance"));
        yyjson_doc_free(doc);
    }

    /* Guard: a pre-existing malformed (non-`{...}`) properties_json is left
     * untouched rather than further corrupted (mirrors
     * append_complexity_props' bail behaviour). */
    int64_t bad =
        cbm_gbuf_upsert_node(gb, "Function", "bad_fn", "pkg.bad_fn", "pkg/d.go", 1, 1, "{}");
    cbm_gbuf_node_t *bad_node = (cbm_gbuf_node_t *)cbm_gbuf_find_by_id(gb, bad);
    ASSERT_NOT_NULL(bad_node);
    free(bad_node->properties_json);
    bad_node->properties_json = strdup("not-a-json-object");
    cbm_pipeline_importance_append_prop(bad_node, 5.0);
    ASSERT_STR_EQ(bad_node->properties_json, "not-a-json-object");

    cbm_gbuf_free(gb);
    PASS();
}

SUITE(importance) {
    RUN_TEST(importance_present_on_symbol_nodes_full_reindex);
    RUN_TEST(importance_base_sqrt_num_refs);
    RUN_TEST(importance_private_multiplier);
    RUN_TEST(importance_generic_name_multiplier);
    RUN_TEST(importance_distinctive_identifier_multiplier);
    RUN_TEST(importance_test_penalty_edge_based);
    RUN_TEST(importance_ordering_after_tests_and_calls);
    RUN_TEST(importance_round_trip_persist);
    RUN_TEST(importance_migration_null_until_reindex);
    RUN_TEST(importance_properties_json_valid_after_append);
}
