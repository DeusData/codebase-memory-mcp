/*
 * test_parallel.c — Tests for the three-phase parallel pipeline.
 *
 * Validates parity between sequential (4-pass) and parallel (3-phase)
 * pipeline modes on a small Go test fixture.
 *
 * Suite: suite_parallel
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "pipeline/pass_lsp_cross.h"
#include "pipeline/lsp_resolve.h"
#include "pipeline/worker_pool.h"
#include "graph_buffer/graph_buffer.h"
#include "discover/discover.h"
#include "foundation/platform.h"
#include "foundation/log.h"
#include "cbm.h"

#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <sys/stat.h>

/* ── Helper: create temp test repo ───────────────────────────────── */

static char g_par_tmpdir[256];

TEST(usage_semantic_reference_candidate_trusts_marked_producer) {
    CBMUsage usage = {0};
    usage.kind = CBM_USAGE_VALUE;
    usage.may_be_call_reference = true;
    ASSERT_TRUE(cbm_pipeline_usage_semantic_reference_candidate(&usage));

    usage.may_be_call_reference = false;
    ASSERT_FALSE(cbm_pipeline_usage_semantic_reference_candidate(&usage));

    usage.kind = CBM_USAGE_CALL_REFERENCE;
    ASSERT_TRUE(cbm_pipeline_usage_semantic_reference_candidate(&usage));

    /* Lexical evidence gates only the later textual fallback. An exact
     * occurrence row must still get the chance to prove one callable target. */
    usage.semantic_reference_blocked = true;
    ASSERT_TRUE(cbm_pipeline_usage_semantic_reference_candidate(&usage));
    PASS();
}

static int setup_parallel_repo(void) {
    snprintf(g_par_tmpdir, sizeof(g_par_tmpdir), "/tmp/cbm_par_XXXXXX");
    if (!cbm_mkdtemp(g_par_tmpdir))
        return -1;

    char path[512];

    /* main.go */
    snprintf(path, sizeof(path), "%s/main.go", g_par_tmpdir);
    FILE *f = fopen(path, "w");
    if (!f)
        return -1;
    fprintf(f, "package main\n\nimport \"pkg\"\n\n"
               "func main() {\n\tpkg.Serve()\n}\n");
    fclose(f);

    /* pkg/ */
    snprintf(path, sizeof(path), "%s/pkg", g_par_tmpdir);
    cbm_mkdir(path);

    /* pkg/service.go */
    snprintf(path, sizeof(path), "%s/pkg/service.go", g_par_tmpdir);
    f = fopen(path, "w");
    if (!f)
        return -1;
    fprintf(f, "package pkg\n\nimport \"pkg/util\"\n\n"
               "func Serve() {\n\tutil.Help()\n}\n");
    fclose(f);

    /* pkg/util/ */
    snprintf(path, sizeof(path), "%s/pkg/util", g_par_tmpdir);
    cbm_mkdir(path);

    /* pkg/util/helper.go */
    snprintf(path, sizeof(path), "%s/pkg/util/helper.go", g_par_tmpdir);
    f = fopen(path, "w");
    if (!f)
        return -1;
    fprintf(f, "package util\n\nfunc Help() {}\n");
    fclose(f);

    return 0;
}

static void rm_rf(const char *path) {
    th_rmtree(path);
}

static void teardown_parallel_repo(void) {
    if (g_par_tmpdir[0])
        rm_rf(g_par_tmpdir);
    g_par_tmpdir[0] = '\0';
}

/* The compact parity helpers normally omit the production structure pass.
 * Tests that exercise graph-derived import maps must seed its File-node
 * precondition explicitly, without changing legacy parity fixtures. */
static void seed_test_file_nodes(cbm_gbuf_t *gbuf, const char *project,
                                 const cbm_file_info_t *files, int file_count) {
    if (!gbuf || !project || !files) {
        return;
    }
    for (int i = 0; i < file_count; i++) {
        const char *rel = files[i].rel_path;
        if (!rel) {
            continue;
        }
        char *file_qn = cbm_pipeline_fqn_compute(project, rel, "__file__");
        const char *slash = strrchr(rel, '/');
        const char *basename = slash ? slash + 1 : rel;
        if (file_qn) {
            cbm_gbuf_upsert_node(gbuf, "File", basename, file_qn, rel, 0, 0, "{}");
        }
        free(file_qn);
    }
}

/* ── Run sequential pipeline on files, returning gbuf ─────────────── */

static cbm_gbuf_t *run_sequential(const char *project, const char *repo_path,
                                  cbm_file_info_t *files, int file_count) {
    cbm_gbuf_t *gbuf = cbm_gbuf_new(project, repo_path);
    cbm_registry_t *reg = cbm_registry_new();
    atomic_int cancelled;
    atomic_init(&cancelled, 0);

    cbm_pipeline_ctx_t ctx = {
        .project_name = project,
        .repo_path = repo_path,
        .gbuf = gbuf,
        .registry = reg,
        .cancelled = &cancelled,
    };

    cbm_init();
    cbm_pipeline_pass_definitions(&ctx, files, file_count);
    cbm_pipeline_pass_calls(&ctx, files, file_count);
    cbm_pipeline_pass_usages(&ctx, files, file_count);
    cbm_pipeline_pass_semantic(&ctx, files, file_count);

    cbm_registry_free(reg);
    return gbuf;
}

typedef void (*parallel_result_mutator_fn)(CBMFileResult **result_cache, int file_count, void *ud);

/* Production's sequential pipeline retains one extraction cache and runs the
 * cross-file LSP pass between definitions and edge materialization. Keep this
 * explicit helper for tests whose semantic target is declared in another file;
 * the lightweight run_sequential helper above intentionally predates that pass. */
static cbm_gbuf_t *run_sequential_with_lsp_cross_and_mutator(
    const char *project, const char *repo_path, cbm_file_info_t *files, int file_count,
    parallel_result_mutator_fn mutator, void *mutator_ud, bool seed_structure) {
    cbm_gbuf_t *gbuf = cbm_gbuf_new(project, repo_path);
    cbm_registry_t *reg = cbm_registry_new();
    CBMFileResult **cache = (CBMFileResult **)calloc((size_t)file_count, sizeof(CBMFileResult *));
    if (!gbuf || !reg || !cache) {
        cbm_gbuf_free(gbuf);
        cbm_registry_free(reg);
        free(cache);
        return NULL;
    }
    atomic_int cancelled;
    atomic_init(&cancelled, 0);
    cbm_pipeline_ctx_t ctx = {
        .project_name = project,
        .repo_path = repo_path,
        .gbuf = gbuf,
        .registry = reg,
        .cancelled = &cancelled,
        .result_cache = cache,
    };

    if (seed_structure) {
        seed_test_file_nodes(gbuf, project, files, file_count);
    }

    cbm_init();
    cbm_pipeline_pass_definitions(&ctx, files, file_count);
    cbm_pipeline_pass_lsp_cross(&ctx, files, file_count, cache);
    if (mutator) {
        mutator(cache, file_count, mutator_ud);
    }
    cbm_pipeline_pass_calls(&ctx, files, file_count);
    cbm_pipeline_pass_usages(&ctx, files, file_count);
    cbm_pipeline_pass_semantic(&ctx, files, file_count);

    for (int i = 0; i < file_count; i++) {
        cbm_free_result(cache[i]);
    }
    free(cache);
    cbm_registry_free(reg);
    return gbuf;
}

static cbm_gbuf_t *run_sequential_with_lsp_cross(const char *project, const char *repo_path,
                                                 cbm_file_info_t *files, int file_count) {
    return run_sequential_with_lsp_cross_and_mutator(project, repo_path, files, file_count, NULL,
                                                     NULL, false);
}

/* ── Run parallel pipeline on files, returning gbuf ───────────────── */

static cbm_gbuf_t *run_parallel_with_extract_opts_and_mutator(
    const char *project, const char *repo_path, cbm_file_info_t *files, int file_count,
    int worker_count, const cbm_parallel_extract_opts_t *extract_opts,
    parallel_result_mutator_fn mutator, void *mutator_ud, bool seed_structure) {
    cbm_gbuf_t *gbuf = cbm_gbuf_new(project, repo_path);
    cbm_registry_t *reg = cbm_registry_new();
    atomic_int cancelled;
    atomic_init(&cancelled, 0);

    cbm_pipeline_ctx_t ctx = {
        .project_name = project,
        .repo_path = repo_path,
        .gbuf = gbuf,
        .registry = reg,
        .cancelled = &cancelled,
    };

    if (seed_structure) {
        seed_test_file_nodes(gbuf, project, files, file_count);
    }

    _Atomic int64_t shared_ids;
    int64_t gbuf_next = cbm_gbuf_next_id(gbuf);
    atomic_init(&shared_ids, gbuf_next);

    CBMFileResult **result_cache = calloc((size_t)file_count, sizeof(CBMFileResult *));

    cbm_init();
    if (extract_opts) {
        cbm_parallel_extract_ex(&ctx, files, file_count, result_cache, &shared_ids, worker_count,
                                extract_opts);
    } else {
        cbm_parallel_extract(&ctx, files, file_count, result_cache, &shared_ids, worker_count);
    }
    cbm_gbuf_set_next_id(gbuf, atomic_load(&shared_ids));

    if (mutator) {
        mutator(result_cache, file_count, mutator_ud);
    }

    cbm_build_registry_from_cache(&ctx, files, file_count, result_cache);
    int64_t registry_next = cbm_gbuf_next_id(gbuf);
    if (registry_next > atomic_load(&shared_ids)) {
        atomic_store(&shared_ids, registry_next);
    }

    /* Cross-file LSP — mirrors run_parallel_pipeline ordering in pipeline.c.
     * Build the project-wide all_defs[] precondition, then feed it into
     * cbm_parallel_resolve where the fused resolve_worker invokes
     * cbm_pxc_run_one(_ts) per file BEFORE materializing CALLS edges. */
    char **def_modules = (char **)calloc((size_t)file_count, sizeof(char *));
    int def_count = 0;
    CBMLSPDef *all_defs = def_modules
                              ? cbm_pxc_collect_all_defs(result_cache, files, file_count,
                                                         ctx.project_name, def_modules, &def_count)
                              : NULL;
    CBMModuleDefIndex *module_def_index =
        all_defs ? cbm_pxc_build_module_def_index(all_defs, def_count) : NULL;

    cbm_parallel_resolve(&ctx, files, file_count, result_cache, &shared_ids, worker_count, all_defs,
                         def_count, def_modules, module_def_index,
                         NULL /* cross_registries — tests use per-file path */);
    cbm_gbuf_set_next_id(gbuf, atomic_load(&shared_ids));

    cbm_pxc_free_module_def_index(module_def_index);
    free(all_defs);
    if (def_modules) {
        for (int i = 0; i < file_count; i++) {
            free(def_modules[i]);
        }
        free(def_modules);
    }

    for (int i = 0; i < file_count; i++)
        if (result_cache[i])
            cbm_free_result(result_cache[i]);
    free(result_cache);

    cbm_registry_free(reg);
    return gbuf;
}

static cbm_gbuf_t *run_parallel_with_extract_opts(const char *project, const char *repo_path,
                                                  cbm_file_info_t *files, int file_count,
                                                  int worker_count,
                                                  const cbm_parallel_extract_opts_t *extract_opts) {
    return run_parallel_with_extract_opts_and_mutator(
        project, repo_path, files, file_count, worker_count, extract_opts, NULL, NULL, false);
}

static cbm_gbuf_t *run_parallel(const char *project, const char *repo_path, cbm_file_info_t *files,
                                int file_count, int worker_count) {
    return run_parallel_with_extract_opts(project, repo_path, files, file_count, worker_count,
                                          NULL);
}

/* ── Parity Tests ─────────────────────────────────────────────────── */

static cbm_gbuf_t *g_seq_gbuf = NULL;
static cbm_gbuf_t *g_par_gbuf = NULL;
static int g_parity_setup_done = 0;

static int ensure_parity_setup(void) {
    if (g_parity_setup_done)
        return 0;

    if (setup_parallel_repo() != 0)
        return -1;

    /* Discover files */
    cbm_discover_opts_t opts = {.mode = CBM_MODE_FULL};
    cbm_file_info_t *files = NULL;
    int file_count = 0;
    if (cbm_discover(g_par_tmpdir, &opts, &files, &file_count) != 0)
        return -1;

    const char *project = "par-test";

    /* Build structure for both (need File/Folder nodes before definitions) */
    /* For parity, we need the structure pass too. Let's just compare
     * definition/call/usage/semantic edge counts. */

    /* Run both modes */
    g_seq_gbuf = run_sequential(project, g_par_tmpdir, files, file_count);
    g_par_gbuf = run_parallel(project, g_par_tmpdir, files, file_count, 2);

    cbm_discover_free(files, file_count);
    g_parity_setup_done = 1;
    return 0;
}

static void parity_teardown(void) {
    if (g_seq_gbuf) {
        cbm_gbuf_free(g_seq_gbuf);
        g_seq_gbuf = NULL;
    }
    if (g_par_gbuf) {
        cbm_gbuf_free(g_par_gbuf);
        g_par_gbuf = NULL;
    }
    teardown_parallel_repo();
    g_parity_setup_done = 0;
}

/* Node count parity */
TEST(parallel_node_count) {
    if (ensure_parity_setup() != 0)
        FAIL("setup failed");
    int seq = cbm_gbuf_node_count(g_seq_gbuf);
    int par = cbm_gbuf_node_count(g_par_gbuf);
    ASSERT_GT(seq, 0);
    ASSERT_EQ(seq, par);
    PASS();
}

/* Edge type parity tests */
static int assert_edge_type_parity(const char *type) {
    if (ensure_parity_setup() != 0)
        return -1;
    int seq = cbm_gbuf_edge_count_by_type(g_seq_gbuf, type);
    int par = cbm_gbuf_edge_count_by_type(g_par_gbuf, type);
    if (seq != par) {
        printf("  FAIL: %s edges: seq=%d par=%d\n", type, seq, par);
        return 1;
    }
    return 0;
}

TEST(parallel_calls_parity) {
    int rc = assert_edge_type_parity("CALLS");
    if (rc == -1)
        FAIL("setup failed");
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(parallel_defines_parity) {
    int rc = assert_edge_type_parity("DEFINES");
    if (rc == -1)
        FAIL("setup failed");
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(parallel_defines_method_parity) {
    int rc = assert_edge_type_parity("DEFINES_METHOD");
    if (rc == -1)
        FAIL("setup failed");
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(parallel_imports_parity) {
    int rc = assert_edge_type_parity("IMPORTS");
    if (rc == -1)
        FAIL("setup failed");
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(parallel_usage_parity) {
    int rc = assert_edge_type_parity("USAGE");
    if (rc == -1)
        FAIL("setup failed");
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(parallel_inherits_parity) {
    int rc = assert_edge_type_parity("INHERITS");
    if (rc == -1)
        FAIL("setup failed");
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(parallel_implements_parity) {
    int rc = assert_edge_type_parity("IMPLEMENTS");
    if (rc == -1)
        FAIL("setup failed");
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(parallel_total_edges) {
    if (ensure_parity_setup() != 0)
        FAIL("setup failed");
    int seq = cbm_gbuf_edge_count(g_seq_gbuf);
    int par = cbm_gbuf_edge_count(g_par_gbuf);
    ASSERT_GT(seq, 0);
    ASSERT_EQ(seq, par);
    PASS();
}

/* ── Empty file list ──────────────────────────────────────────────── */

TEST(parallel_empty_files) {
    cbm_gbuf_t *gbuf = cbm_gbuf_new("empty-proj", "/tmp");
    cbm_registry_t *reg = cbm_registry_new();
    atomic_int cancelled;
    atomic_init(&cancelled, 0);

    cbm_pipeline_ctx_t ctx = {
        .project_name = "empty-proj",
        .repo_path = "/tmp",
        .gbuf = gbuf,
        .registry = reg,
        .cancelled = &cancelled,
    };

    _Atomic int64_t shared_ids;
    atomic_init(&shared_ids, 1);

    CBMFileResult **cache = NULL;
    int rc = cbm_parallel_extract(&ctx, NULL, 0, cache, &shared_ids, 2);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(cbm_gbuf_node_count(gbuf), 0);

    cbm_registry_free(reg);
    cbm_gbuf_free(gbuf);
    PASS();
}

/* ── Regression: args JSON must not overflow the props buffer ──────── */

/* A call with many long string arguments makes append_args_json()'s running
 * position exceed the fixed CBM_SZ_2K `props` stack buffer in
 * emit_normal_calls_edge(): format_call_arg() returns snprintf's UNtruncated
 * length, so pos += n could run past the buffer and the trailing
 * buf[pos]='\0' wrote out of bounds (stack-buffer-overflow; caught by the
 * stack canary as a SIGABRT on real repos). This indexes a fixture whose
 * single call carries enough long args to drive pos past 2 KB; under the
 * ASan test build a regression aborts here. */
TEST(parallel_args_json_no_overflow) {
    char dir[256];
    snprintf(dir, sizeof(dir), "/tmp/cbm_argov_XXXXXX");
    ASSERT_TRUE(cbm_mkdtemp(dir) != NULL);

    char path[512];
    snprintf(path, sizeof(path), "%s/app.ts", dir);
    FILE *f = fopen(path, "w");
    ASSERT_TRUE(f != NULL);
    fputs("function sink(...xs: string[]) { return xs; }\n", f);
    fputs("function caller() {\n  sink(\n", f);
    for (int i = 0; i < 60; i++) {
        /* 100-char string literal per arg; 60 args => args JSON well past the
         * 2 KB props buffer, forcing the pre-fix overshoot. */
        fputs("    \"", f);
        for (int j = 0; j < 100; j++)
            fputc('a' + (i % 26), f);
        fputs(i < 59 ? "\",\n" : "\"\n", f);
    }
    fputs("  );\n}\n", f);
    fclose(f);

    cbm_discover_opts_t opts = {.mode = CBM_MODE_FULL};
    cbm_file_info_t *files = NULL;
    int file_count = 0;
    ASSERT_EQ(cbm_discover(dir, &opts, &files, &file_count), 0);
    ASSERT_GT(file_count, 0);

    cbm_gbuf_t *gbuf = run_parallel("argov-test", dir, files, file_count, 4);
    ASSERT_TRUE(gbuf != NULL);
    ASSERT_GT(cbm_gbuf_edge_count(gbuf), 0);

    cbm_gbuf_free(gbuf);
    cbm_discover_free(files, file_count);
    th_rmtree(dir);
    PASS();
}

/* ── Graph buffer merge tests ─────────────────────────────────────── */

TEST(gbuf_shared_ids_unique) {
    _Atomic int64_t shared = 1;
    cbm_gbuf_t *ga = cbm_gbuf_new_shared_ids("proj", "/", &shared);
    cbm_gbuf_t *gb = cbm_gbuf_new_shared_ids("proj", "/", &shared);

    int64_t id1 = cbm_gbuf_upsert_node(ga, "Function", "foo", "proj.foo", "a.go", 1, 5, "{}");
    int64_t id2 = cbm_gbuf_upsert_node(gb, "Function", "bar", "proj.bar", "b.go", 1, 3, "{}");
    ASSERT_GT(id1, 0);
    ASSERT_GT(id2, 0);
    ASSERT_NEQ(id1, id2);

    cbm_gbuf_free(ga);
    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_merge_nodes) {
    _Atomic int64_t shared = 1;
    cbm_gbuf_t *dst = cbm_gbuf_new_shared_ids("proj", "/", &shared);
    cbm_gbuf_t *src = cbm_gbuf_new_shared_ids("proj", "/", &shared);

    cbm_gbuf_upsert_node(dst, "Function", "a", "proj.a", "a.go", 1, 5, "{}");
    cbm_gbuf_upsert_node(dst, "Function", "b", "proj.b", "a.go", 6, 10, "{}");
    cbm_gbuf_upsert_node(src, "Function", "c", "proj.c", "b.go", 1, 5, "{}");
    cbm_gbuf_upsert_node(src, "Function", "d", "proj.d", "b.go", 6, 10, "{}");

    ASSERT_EQ(cbm_gbuf_node_count(dst), 2);
    cbm_gbuf_merge(dst, src);
    ASSERT_EQ(cbm_gbuf_node_count(dst), 4);

    ASSERT_NOT_NULL(cbm_gbuf_find_by_qn(dst, "proj.c"));
    ASSERT_NOT_NULL(cbm_gbuf_find_by_qn(dst, "proj.d"));
    /* dst originals still there */
    ASSERT_NOT_NULL(cbm_gbuf_find_by_qn(dst, "proj.a"));
    ASSERT_NOT_NULL(cbm_gbuf_find_by_qn(dst, "proj.b"));

    cbm_gbuf_free(src);
    cbm_gbuf_free(dst);
    PASS();
}

TEST(gbuf_merge_edges) {
    _Atomic int64_t shared = 1;
    cbm_gbuf_t *dst = cbm_gbuf_new_shared_ids("proj", "/", &shared);
    cbm_gbuf_t *src = cbm_gbuf_new_shared_ids("proj", "/", &shared);

    int64_t a = cbm_gbuf_upsert_node(dst, "Function", "a", "proj.a", "a.go", 1, 5, "{}");
    int64_t b = cbm_gbuf_upsert_node(dst, "Function", "b", "proj.b", "a.go", 6, 10, "{}");
    /* Put an edge in src that references dst nodes (by ID) */
    cbm_gbuf_insert_edge(src, a, b, "CALLS", "{}");

    cbm_gbuf_merge(dst, src);
    ASSERT_GT(cbm_gbuf_edge_count(dst), 0);

    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_source_type(dst, a, "CALLS", &edges, &count);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(edges[0]->target_id, b);

    cbm_gbuf_free(src);
    cbm_gbuf_free(dst);
    PASS();
}

TEST(gbuf_merge_empty_src) {
    _Atomic int64_t shared = 1;
    cbm_gbuf_t *dst = cbm_gbuf_new_shared_ids("proj", "/", &shared);
    cbm_gbuf_t *src = cbm_gbuf_new_shared_ids("proj", "/", &shared);

    cbm_gbuf_upsert_node(dst, "Function", "a", "proj.a", "a.go", 1, 5, "{}");
    int before = cbm_gbuf_node_count(dst);
    cbm_gbuf_merge(dst, src);
    ASSERT_EQ(cbm_gbuf_node_count(dst), before);

    cbm_gbuf_free(src);
    cbm_gbuf_free(dst);
    PASS();
}

TEST(gbuf_merge_src_free_safe) {
    _Atomic int64_t shared = 1;
    cbm_gbuf_t *dst = cbm_gbuf_new_shared_ids("proj", "/", &shared);
    cbm_gbuf_t *src = cbm_gbuf_new_shared_ids("proj", "/", &shared);

    cbm_gbuf_upsert_node(src, "Function", "x", "proj.x", "x.go", 1, 5, "{}");
    cbm_gbuf_merge(dst, src);
    cbm_gbuf_free(src); /* must not crash */

    /* dst node still accessible */
    ASSERT_NOT_NULL(cbm_gbuf_find_by_qn(dst, "proj.x"));
    cbm_gbuf_free(dst);
    PASS();
}

TEST(gbuf_next_id_accessors) {
    cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/");
    ASSERT_EQ(cbm_gbuf_next_id(gb), 1);

    cbm_gbuf_upsert_node(gb, "Function", "foo", "proj.foo", "f.go", 1, 5, "{}");
    ASSERT_GT(cbm_gbuf_next_id(gb), 1);

    cbm_gbuf_set_next_id(gb, 100);
    int64_t id = cbm_gbuf_upsert_node(gb, "Function", "bar", "proj.bar", "f.go", 6, 10, "{}");
    ASSERT_GTE(id, 100);

    cbm_gbuf_free(gb);
    PASS();
}

/* ── Parallel-pipeline LSP-override regression ────────────────────── */
/* Pin the wiring fix that unified pass_calls.c (sequential) and
 * pass_parallel.c (parallel) on cbm_pipeline_find_lsp_resolution +
 * CBM_LSP_CONFIDENCE_FLOOR (lsp_resolve.h). Before the unification, the
 * parallel path carried its own lsp_override_resolution_pp at floor 0.5
 * while the sequential path used find_lsp_resolution at floor 0.6, so a
 * project produced different CALLS edge attributions depending on which
 * pipeline mode kicked in. This test indexes a small Python repo via
 * the parallel pipeline and asserts at least one resulting CALLS edge
 * carries an "lsp_*" strategy — proof the parallel path consults
 * result->resolved_calls and emits LSP-attributed edges. */

typedef struct {
    int lsp_strategy_count;
    int total_calls;
} lsp_edge_count_ctx_t;

static void count_lsp_call_edges(const cbm_gbuf_edge_t *edge, void *ud) {
    lsp_edge_count_ctx_t *c = ud;
    if (!edge || !edge->type || strcmp(edge->type, "CALLS") != 0) {
        return;
    }
    c->total_calls++;
    if (edge->properties_json && strstr(edge->properties_json, "\"strategy\":\"lsp")) {
        c->lsp_strategy_count++;
    }
}

static const char *class_method_tail(const char *qn) {
    if (!qn) {
        return NULL;
    }
    const char *last = strrchr(qn, '.');
    if (!last || last == qn) {
        return NULL;
    }
    const char *second = last;
    while (second > qn) {
        second--;
        if (*second == '.') {
            return second == qn ? qn : second + 1;
        }
    }
    return qn;
}

static const cbm_gbuf_node_t *find_unique_callable_node_by_tail(const cbm_gbuf_t *gbuf,
                                                                const char *tail) {
    const char *method = tail ? strrchr(tail, '.') : NULL;
    method = method ? method + 1 : tail;
    if (!gbuf || !tail || !method) {
        return NULL;
    }
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (cbm_gbuf_find_by_name(gbuf, method, &nodes, &count) != 0) {
        return NULL;
    }
    const cbm_gbuf_node_t *match = NULL;
    for (int i = 0; i < count; i++) {
        const cbm_gbuf_node_t *node = nodes[i];
        if (!node || !node->label || !node->qualified_name) {
            continue;
        }
        if (strcmp(node->label, "Method") != 0 && strcmp(node->label, "Function") != 0) {
            continue;
        }
        const char *node_tail = class_method_tail(node->qualified_name);
        if (!node_tail || strcmp(node_tail, tail) != 0) {
            continue;
        }
        if (match) {
            return NULL;
        }
        match = node;
    }
    return match;
}

static bool qn_has_segment_suffix(const char *qualified_name, const char *suffix) {
    if (!qualified_name || !suffix) {
        return false;
    }
    size_t qualified_len = strlen(qualified_name);
    size_t suffix_len = strlen(suffix);
    if (suffix_len > qualified_len ||
        strcmp(qualified_name + qualified_len - suffix_len, suffix) != 0) {
        return false;
    }
    return qualified_len == suffix_len || qualified_name[qualified_len - suffix_len - 1] == '.';
}

static const cbm_gbuf_node_t *find_unique_node_by_name_label_qn_suffix(const cbm_gbuf_t *gbuf,
                                                                       const char *name,
                                                                       const char *label,
                                                                       const char *qn_suffix) {
    if (!gbuf || !name || !label || !qn_suffix) {
        return NULL;
    }
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (cbm_gbuf_find_by_name(gbuf, name, &nodes, &count) != 0) {
        return NULL;
    }
    const cbm_gbuf_node_t *match = NULL;
    for (int i = 0; i < count; i++) {
        const cbm_gbuf_node_t *node = nodes[i];
        if (!node || !node->label || !node->qualified_name || strcmp(node->label, label) != 0 ||
            !qn_has_segment_suffix(node->qualified_name, qn_suffix)) {
            continue;
        }
        if (match) {
            return NULL;
        }
        match = node;
    }
    return match;
}

static int count_edges_between_nodes(const cbm_gbuf_t *gbuf, const cbm_gbuf_node_t *source,
                                     const cbm_gbuf_node_t *target, const char *edge_type) {
    if (!gbuf || !source || !target || !edge_type) {
        return -1;
    }
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    if (cbm_gbuf_find_edges_by_source_type(gbuf, source->id, edge_type, &edges, &count) != 0) {
        return -1;
    }
    int matches = 0;
    for (int i = 0; i < count; i++) {
        if (edges[i] && edges[i]->target_id == target->id) {
            matches++;
        }
    }
    return matches;
}

static bool has_edge_from_callable_to_node(const cbm_gbuf_t *gbuf, const char *source_tail,
                                           const cbm_gbuf_node_t *target, const char *edge_type) {
    const cbm_gbuf_node_t *source = find_unique_callable_node_by_tail(gbuf, source_tail);
    if (!source || !target || !edge_type) {
        return false;
    }
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    if (cbm_gbuf_find_edges_by_source_type(gbuf, source->id, edge_type, &edges, &count) != 0) {
        return false;
    }
    for (int i = 0; i < count; i++) {
        if (edges[i] && edges[i]->target_id == target->id) {
            return true;
        }
    }
    return false;
}

static const cbm_gbuf_edge_t *find_calls_edge_by_tails(const cbm_gbuf_t *gbuf,
                                                       const char *source_tail,
                                                       const char *target_tail) {
    const cbm_gbuf_node_t *source = find_unique_callable_node_by_tail(gbuf, source_tail);
    const cbm_gbuf_node_t *target = find_unique_callable_node_by_tail(gbuf, target_tail);
    if (!source || !target) {
        return NULL;
    }

    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    if (cbm_gbuf_find_edges_by_source_type(gbuf, source->id, "CALLS", &edges, &count) != 0) {
        return NULL;
    }
    for (int i = 0; i < count; i++) {
        if (edges[i] && edges[i]->target_id == target->id) {
            return edges[i];
        }
    }
    return NULL;
}

static bool has_edge_from_callable_to_qn(const cbm_gbuf_t *gbuf, const char *source_tail,
                                         const char *target_qn, const char *edge_type) {
    const cbm_gbuf_node_t *source = find_unique_callable_node_by_tail(gbuf, source_tail);
    const cbm_gbuf_node_t *target = cbm_gbuf_find_by_qn(gbuf, target_qn);
    if (!source || !target)
        return false;
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    if (cbm_gbuf_find_edges_by_source_type(gbuf, source->id, edge_type, &edges, &count) != 0)
        return false;
    for (int i = 0; i < count; i++) {
        if (edges[i] && edges[i]->target_id == target->id)
            return true;
    }
    return false;
}

static bool callable_has_call_target_fragment(const cbm_gbuf_t *gbuf, const char *source_tail,
                                              const char *target_fragment) {
    const cbm_gbuf_node_t *source = find_unique_callable_node_by_tail(gbuf, source_tail);
    if (!source || !target_fragment)
        return false;
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    if (cbm_gbuf_find_edges_by_source_type(gbuf, source->id, "CALLS", &edges, &count) != 0)
        return false;
    for (int i = 0; i < count; i++) {
        const cbm_gbuf_node_t *target =
            edges[i] ? cbm_gbuf_find_by_id(gbuf, edges[i]->target_id) : NULL;
        if (target && target->qualified_name && strstr(target->qualified_name, target_fragment))
            return true;
    }
    return false;
}

static int count_calls_edges_to_tail(const cbm_gbuf_t *gbuf, const char *target_tail) {
    const cbm_gbuf_node_t *target = find_unique_callable_node_by_tail(gbuf, target_tail);
    if (!target)
        return -1;
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    if (cbm_gbuf_find_edges_by_target_type(gbuf, target->id, "CALLS", &edges, &count) != 0)
        return -1;
    return count;
}

typedef struct {
    bool injected;
} lsp_legacy_injection_t;

/* Test-only cache mutation: preserve the real exact Alpha.render semantic
 * record, then append a higher-confidence legacy 0:0 record for Beta.render.
 * The shared linear matcher and the compact parallel index must both prefer
 * the exact occurrence. */
static void inject_higher_confidence_legacy_render(CBMFileResult **result_cache, int file_count,
                                                   void *ud) {
    lsp_legacy_injection_t *state = (lsp_legacy_injection_t *)ud;
    if (!state) {
        return;
    }
    for (int file = 0; file < file_count; file++) {
        CBMFileResult *result = result_cache ? result_cache[file] : NULL;
        if (!result) {
            continue;
        }
        const CBMResolvedCall *exact = NULL;
        const char *beta_qn = NULL;
        for (int i = 0; i < result->resolved_calls.count; i++) {
            const CBMResolvedCall *candidate = &result->resolved_calls.items[i];
            if (candidate->kind == CBM_RESOLVED_INVOCATION && candidate->caller_qn &&
                strstr(candidate->caller_qn, "Caller.run") && candidate->callee_qn &&
                strstr(candidate->callee_qn, "Alpha.render") &&
                candidate->site_end_byte > candidate->site_start_byte) {
                exact = candidate;
                break;
            }
        }
        for (int i = 0; i < result->defs.count; i++) {
            const CBMDefinition *definition = &result->defs.items[i];
            if (definition->qualified_name && strstr(definition->qualified_name, "Beta.render")) {
                beta_qn = definition->qualified_name;
                break;
            }
        }
        if (!exact || !beta_qn) {
            continue;
        }
        CBMResolvedCall legacy = *exact;
        legacy.callee_qn = beta_qn;
        legacy.strategy = "test_legacy_zero_span";
        legacy.confidence = 0.99f;
        legacy.site_start_byte = 0;
        legacy.site_end_byte = 0;
        cbm_resolvedcall_push(&result->resolved_calls, &result->arena, legacy);
        state->injected = true;
        return;
    }
}

TEST(parallel_lsp_index_exact_site_beats_legacy_record_in_graph) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_par_lsp_site_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("mkdtemp failed");
    }

    char path[512];
    snprintf(path, sizeof(path), "%s/app.py", tmpdir);
    FILE *file = cbm_fopen(path, "w");
    if (!file) {
        rmdir(tmpdir);
        FAIL("fopen app.py failed");
    }
    fprintf(file, "class Alpha:\n"
                  "    def render(self):\n"
                  "        return 'alpha'\n"
                  "\n"
                  "class Beta:\n"
                  "    def render(self):\n"
                  "        return 'beta'\n"
                  "\n"
                  "class Caller:\n"
                  "    def run(self):\n"
                  "        value = Alpha()\n"
                  "        return value.render()\n");
    fclose(file);

    cbm_file_info_t files[1] = {0};
    files[0].path = path;
    files[0].rel_path = (char *)"app.py";
    files[0].language = CBM_LANG_PYTHON;
    lsp_legacy_injection_t injection = {0};
    cbm_gbuf_t *gbuf = run_parallel_with_extract_opts_and_mutator(
        "cbm_par_lsp_site", tmpdir, files, 1, 1, NULL, inject_higher_confidence_legacy_render,
        &injection, false);
    ASSERT_NOT_NULL(gbuf);
    ASSERT_TRUE(injection.injected);

    const cbm_gbuf_edge_t *exact = find_calls_edge_by_tails(gbuf, "Caller.run", "Alpha.render");
    const cbm_gbuf_edge_t *wrong = find_calls_edge_by_tails(gbuf, "Caller.run", "Beta.render");
    if (!exact || wrong) {
        printf("  lsp index occurrence diagnostic: exact=%s legacy_wrong=%s\n",
               exact ? "present" : "absent", wrong ? "present" : "absent");
    }
    ASSERT_NOT_NULL(exact);
    ASSERT_NULL(wrong);

    cbm_gbuf_free(gbuf);
    unlink(path);
    rmdir(tmpdir);
    PASS();
}

typedef struct {
    bool injected;
} lsp_exact_ambiguity_injection_t;

/* Append a distinct semantic target at the same exact source occurrence and
 * make the carrier semantic-only. Both the shared matcher and the compact
 * parallel index must fail closed; neither confidence nor textual registry
 * fallback may select one target. */
static void inject_distinct_exact_render_target(CBMFileResult **result_cache, int file_count,
                                                void *ud) {
    lsp_exact_ambiguity_injection_t *state = (lsp_exact_ambiguity_injection_t *)ud;
    if (!state) {
        return;
    }
    for (int file = 0; file < file_count; file++) {
        CBMFileResult *result = result_cache ? result_cache[file] : NULL;
        if (!result) {
            continue;
        }
        CBMCall *carrier = NULL;
        const CBMResolvedCall *exact = NULL;
        const char *beta_qn = NULL;
        for (int i = 0; i < result->calls.count; i++) {
            CBMCall *candidate = &result->calls.items[i];
            if (candidate->callee_name && candidate->enclosing_func_qn &&
                strstr(candidate->enclosing_func_qn, "Caller.run") &&
                strcmp(cbm_pipeline_call_callee_leaf(candidate->callee_name), "render") == 0 &&
                cbm_pipeline_source_site_present(candidate->site_start_byte,
                                                 candidate->site_end_byte)) {
                carrier = candidate;
                break;
            }
        }
        for (int i = 0; i < result->resolved_calls.count; i++) {
            const CBMResolvedCall *candidate = &result->resolved_calls.items[i];
            if (candidate->kind == CBM_RESOLVED_INVOCATION && candidate->caller_qn &&
                strstr(candidate->caller_qn, "Caller.run") && candidate->callee_qn &&
                strstr(candidate->callee_qn, "Alpha.render") &&
                cbm_pipeline_source_site_present(candidate->site_start_byte,
                                                 candidate->site_end_byte)) {
                exact = candidate;
                break;
            }
        }
        for (int i = 0; i < result->defs.count; i++) {
            const CBMDefinition *definition = &result->defs.items[i];
            if (definition->qualified_name && strstr(definition->qualified_name, "Beta.render")) {
                beta_qn = definition->qualified_name;
                break;
            }
        }
        if (!carrier || !exact || !beta_qn ||
            !cbm_pipeline_source_site_eq(carrier->site_start_byte, carrier->site_end_byte,
                                         exact->site_start_byte, exact->site_end_byte)) {
            continue;
        }
        carrier->requires_lsp_resolution = true;
        CBMResolvedCall ambiguous = *exact;
        ambiguous.callee_qn = beta_qn;
        ambiguous.strategy = "test_distinct_exact_target";
        ambiguous.confidence = 0.99f;
        cbm_resolvedcall_push(&result->resolved_calls, &result->arena, ambiguous);
        state->injected = true;
        return;
    }
}

TEST(parallel_lsp_index_distinct_exact_targets_fail_closed_in_graph) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_par_lsp_ambiguous_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("mkdtemp failed");
    }
    char path[512];
    snprintf(path, sizeof(path), "%s/app.py", tmpdir);
    FILE *file = cbm_fopen(path, "w");
    if (!file) {
        rmdir(tmpdir);
        FAIL("fopen app.py failed");
    }
    fprintf(file, "class Alpha:\n"
                  "    def render(self): return 'alpha'\n"
                  "class Beta:\n"
                  "    def render(self): return 'beta'\n"
                  "class Caller:\n"
                  "    def run(self):\n"
                  "        value = Alpha()\n"
                  "        return value.render()\n");
    fclose(file);

    cbm_file_info_t file_info = {0};
    file_info.path = path;
    file_info.rel_path = (char *)"app.py";
    file_info.language = CBM_LANG_PYTHON;
    lsp_exact_ambiguity_injection_t injection = {0};
    cbm_gbuf_t *gbuf = run_parallel_with_extract_opts_and_mutator(
        "cbm_par_lsp_ambiguous", tmpdir, &file_info, 1, 1, NULL,
        inject_distinct_exact_render_target, &injection, false);
    ASSERT_NOT_NULL(gbuf);
    ASSERT_TRUE(injection.injected);
    ASSERT_NOT_NULL(find_unique_callable_node_by_tail(gbuf, "Alpha.render"));
    ASSERT_NOT_NULL(find_unique_callable_node_by_tail(gbuf, "Beta.render"));
    const cbm_gbuf_edge_t *alpha = find_calls_edge_by_tails(gbuf, "Caller.run", "Alpha.render");
    const cbm_gbuf_edge_t *beta = find_calls_edge_by_tails(gbuf, "Caller.run", "Beta.render");
    if (alpha || beta) {
        printf("  lsp exact ambiguity diagnostic: alpha=%s beta=%s\n", alpha ? "present" : "absent",
               beta ? "present" : "absent");
    }
    ASSERT_NULL(alpha);
    ASSERT_NULL(beta);

    cbm_gbuf_free(gbuf);
    unlink(path);
    rmdir(tmpdir);
    PASS();
}

typedef struct {
    lsp_exact_ambiguity_injection_t ambiguity;
    bool legacy_injected;
    bool carrier_allows_fallback;
    bool shared_matcher_failed_closed;
} lsp_exact_ambiguity_legacy_probe_t;

/* Reuse the exact-ambiguity injection above, then add a unique 0:0 row while
 * leaving the carrier eligible for legacy resolution. The authoritative
 * matcher must keep the exact-site ambiguity authoritative; the compact
 * parallel index must not reinterpret its tombstone as "try legacy". */
static void inject_legacy_after_exact_render_ambiguity(CBMFileResult **result_cache, int file_count,
                                                       void *ud) {
    lsp_exact_ambiguity_legacy_probe_t *probe = (lsp_exact_ambiguity_legacy_probe_t *)ud;
    if (!probe) {
        return;
    }
    inject_distinct_exact_render_target(result_cache, file_count, &probe->ambiguity);
    if (!probe->ambiguity.injected) {
        return;
    }

    for (int file = 0; file < file_count; file++) {
        CBMFileResult *result = result_cache ? result_cache[file] : NULL;
        if (!result) {
            continue;
        }
        CBMCall *carrier = NULL;
        const CBMResolvedCall *exact = NULL;
        const char *legacy_qn = NULL;
        for (int i = 0; i < result->calls.count; i++) {
            CBMCall *candidate = &result->calls.items[i];
            if (candidate->callee_name && candidate->enclosing_func_qn &&
                strstr(candidate->enclosing_func_qn, "Caller.run") &&
                strcmp(cbm_pipeline_call_callee_leaf(candidate->callee_name), "render") == 0 &&
                cbm_pipeline_source_site_present(candidate->site_start_byte,
                                                 candidate->site_end_byte)) {
                carrier = candidate;
                break;
            }
        }
        for (int i = 0; i < result->resolved_calls.count; i++) {
            const CBMResolvedCall *candidate = &result->resolved_calls.items[i];
            if (candidate->kind == CBM_RESOLVED_INVOCATION && candidate->caller_qn &&
                strstr(candidate->caller_qn, "Caller.run") && candidate->callee_qn &&
                strstr(candidate->callee_qn, "Alpha.render") &&
                cbm_pipeline_source_site_present(candidate->site_start_byte,
                                                 candidate->site_end_byte)) {
                exact = candidate;
                break;
            }
        }
        for (int i = 0; i < result->defs.count; i++) {
            const CBMDefinition *definition = &result->defs.items[i];
            if (definition->qualified_name && strstr(definition->qualified_name, "Legacy.render")) {
                legacy_qn = definition->qualified_name;
                break;
            }
        }
        if (!carrier || !exact || !legacy_qn ||
            !cbm_pipeline_source_site_eq(carrier->site_start_byte, carrier->site_end_byte,
                                         exact->site_start_byte, exact->site_end_byte)) {
            continue;
        }

        carrier->requires_lsp_resolution = false;
        CBMResolvedCall legacy = *exact;
        legacy.callee_qn = legacy_qn;
        legacy.strategy = "test_legacy_after_exact_ambiguity";
        legacy.confidence = 0.99f;
        legacy.site_start_byte = 0;
        legacy.site_end_byte = 0;
        cbm_resolvedcall_push(&result->resolved_calls, &result->arena, legacy);

        probe->legacy_injected = true;
        probe->carrier_allows_fallback = !carrier->requires_lsp_resolution;
        probe->shared_matcher_failed_closed =
            cbm_pipeline_find_lsp_resolution(&result->resolved_calls, carrier, false) == NULL;
        return;
    }
}

TEST(parallel_lsp_index_exact_ambiguity_does_not_fall_through_to_legacy) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_par_lsp_ambiguous_legacy_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("mkdtemp failed");
    }
    char path[512];
    snprintf(path, sizeof(path), "%s/app.py", tmpdir);
    FILE *file = cbm_fopen(path, "w");
    if (!file) {
        rmdir(tmpdir);
        FAIL("fopen app.py failed");
    }
    fprintf(file, "class Alpha:\n"
                  "    def render(self): return 'alpha'\n"
                  "class Beta:\n"
                  "    def render(self): return 'beta'\n"
                  "class Legacy:\n"
                  "    def render(self): return 'legacy'\n"
                  "class Caller:\n"
                  "    def run(self):\n"
                  "        value = Alpha()\n"
                  "        return value.render()\n");
    fclose(file);

    cbm_file_info_t file_info = {0};
    file_info.path = path;
    file_info.rel_path = (char *)"app.py";
    file_info.language = CBM_LANG_PYTHON;
    lsp_exact_ambiguity_legacy_probe_t probe = {0};
    cbm_gbuf_t *gbuf = run_parallel_with_extract_opts_and_mutator(
        "cbm_par_lsp_ambiguous_legacy", tmpdir, &file_info, 1, 1, NULL,
        inject_legacy_after_exact_render_ambiguity, &probe, false);
    ASSERT_NOT_NULL(gbuf);

    int alpha_edges = count_calls_edges_to_tail(gbuf, "Alpha.render");
    int beta_edges = count_calls_edges_to_tail(gbuf, "Beta.render");
    int legacy_edges = count_calls_edges_to_tail(gbuf, "Legacy.render");
    const cbm_gbuf_edge_t *legacy_edge =
        find_calls_edge_by_tails(gbuf, "Caller.run", "Legacy.render");
    if (!probe.ambiguity.injected || !probe.legacy_injected || !probe.carrier_allows_fallback ||
        !probe.shared_matcher_failed_closed || alpha_edges != 1 || beta_edges != 0 ||
        legacy_edges != 0) {
        printf("  exact ambiguity + legacy diagnostic: exact_ambiguity=%d legacy=%d "
               "fallback_allowed=%d shared_failed_closed=%d alpha=%d beta=%d legacy_edge=%d "
               "props=%s\n",
               probe.ambiguity.injected, probe.legacy_injected, probe.carrier_allows_fallback,
               probe.shared_matcher_failed_closed, alpha_edges, beta_edges, legacy_edges,
               legacy_edge && legacy_edge->properties_json ? legacy_edge->properties_json : "");
    }

    cbm_gbuf_free(gbuf);
    unlink(path);
    rmdir(tmpdir);
    ASSERT_TRUE(probe.ambiguity.injected);
    ASSERT_TRUE(probe.legacy_injected);
    ASSERT_TRUE(probe.carrier_allows_fallback);
    ASSERT_TRUE(probe.shared_matcher_failed_closed);
    /* The ordinary registry/type fallback may still recover the source-proven
     * Alpha receiver. Only the lower-ranked legacy semantic row is forbidden. */
    ASSERT_EQ(alpha_edges, 1);
    ASSERT_EQ(beta_edges, 0);
    ASSERT_EQ(legacy_edges, 0);
    PASS();
}

typedef struct {
    bool injected;
    bool authoritative_exact_wins;
    bool legacy_key_fits;
    bool exact_key_overflows;
} lsp_long_key_probe_t;

static void inject_long_caller_exact_and_legacy(CBMFileResult **result_cache, int file_count,
                                                void *ud) {
    lsp_long_key_probe_t *probe = (lsp_long_key_probe_t *)ud;
    if (!probe)
        return;
    for (int file = 0; file < file_count; file++) {
        CBMFileResult *result = result_cache ? result_cache[file] : NULL;
        if (!result)
            continue;
        CBMCall *carrier = NULL;
        CBMResolvedCall *exact = NULL;
        const char *beta_qn = NULL;
        for (int i = 0; i < result->calls.count; i++) {
            CBMCall *candidate = &result->calls.items[i];
            if (candidate->callee_name &&
                strcmp(cbm_pipeline_call_callee_leaf(candidate->callee_name), "render") == 0) {
                carrier = candidate;
                break;
            }
        }
        for (int i = 0; i < result->resolved_calls.count; i++) {
            CBMResolvedCall *candidate = &result->resolved_calls.items[i];
            if (candidate->kind == CBM_RESOLVED_INVOCATION && candidate->callee_qn &&
                strstr(candidate->callee_qn, "Alpha.render") &&
                candidate->site_end_byte > candidate->site_start_byte) {
                exact = candidate;
                break;
            }
        }
        for (int i = 0; i < result->defs.count; i++) {
            const CBMDefinition *definition = &result->defs.items[i];
            if (definition->qualified_name && strstr(definition->qualified_name, "Beta.render")) {
                beta_qn = definition->qualified_name;
                break;
            }
        }
        if (!carrier || !exact || !beta_qn)
            continue;

        CBMResolvedCall legacy = *exact;
        legacy.callee_qn = beta_qn;
        legacy.strategy = "test_long_key_legacy";
        legacy.confidence = 0.99f;
        legacy.site_start_byte = 0;
        legacy.site_end_byte = 0;
        cbm_resolvedcall_push(&result->resolved_calls, &result->arena, legacy);

        size_t leaf_len = strlen("render");
        size_t legacy_len = strlen(carrier->enclosing_func_qn) + 1U + leaf_len;
        char site_suffix[64];
        int suffix_len = snprintf(site_suffix, sizeof(site_suffix), "|%u:%u",
                                  carrier->site_start_byte, carrier->site_end_byte);
        size_t exact_len = legacy_len + (suffix_len > 0 ? (size_t)suffix_len : 0U);
        probe->legacy_key_fits = legacy_len < 1024U;
        probe->exact_key_overflows = exact_len >= 1024U;
        const CBMResolvedCall *authoritative =
            cbm_pipeline_find_lsp_resolution(&result->resolved_calls, carrier, false);
        probe->authoritative_exact_wins = authoritative && authoritative->callee_qn &&
                                          strstr(authoritative->callee_qn, "Alpha.render") != NULL;
        probe->injected = true;
        return;
    }
}

TEST(parallel_lsp_long_exact_key_never_yields_legacy_target) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_par_lsp_long_key_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("mkdtemp failed");
    char path[512];
    snprintf(path, sizeof(path), "%s/app.py", tmpdir);
    FILE *file = cbm_fopen(path, "w");
    if (!file) {
        rmdir(tmpdir);
        FAIL("fopen app.py failed");
    }
    enum { FUNCTION_NAME_LEN = 997 };
    char function_name[FUNCTION_NAME_LEN + 1];
    memcpy(function_name, "run_", 4);
    memset(function_name + 4, 'q', FUNCTION_NAME_LEN - 4);
    function_name[FUNCTION_NAME_LEN] = '\0';
    fputs("class Alpha:\n"
          "    def render(self):\n"
          "        return 'alpha'\n"
          "class Beta:\n"
          "    def render(self):\n"
          "        return 'beta'\n",
          file);
    fprintf(file, "def %s(value: Alpha):\n    return value.render()\n", function_name);
    fclose(file);

    cbm_file_info_t files[1] = {0};
    files[0].path = path;
    files[0].rel_path = (char *)"app.py";
    files[0].language = CBM_LANG_PYTHON;
    lsp_long_key_probe_t probe = {0};
    cbm_gbuf_t *gbuf = run_parallel_with_extract_opts_and_mutator(
        "cbm_long_key", tmpdir, files, 1, 1, NULL, inject_long_caller_exact_and_legacy, &probe,
        false);
    ASSERT_NOT_NULL(gbuf);
    int alpha_edges = count_calls_edges_to_tail(gbuf, "Alpha.render");
    int beta_edges = count_calls_edges_to_tail(gbuf, "Beta.render");
    if (!probe.injected || !probe.authoritative_exact_wins || !probe.legacy_key_fits ||
        !probe.exact_key_overflows || alpha_edges < 1 || beta_edges != 0) {
        printf("  long exact-key diagnostic: injected=%d authoritative=%d legacy_fits=%d "
               "exact_overflow=%d alpha=%d beta=%d\n",
               probe.injected, probe.authoritative_exact_wins, probe.legacy_key_fits,
               probe.exact_key_overflows, alpha_edges, beta_edges);
    }
    cbm_gbuf_free(gbuf);
    unlink(path);
    rmdir(tmpdir);
    ASSERT_TRUE(probe.injected);
    ASSERT_TRUE(probe.authoritative_exact_wins);
    ASSERT_TRUE(probe.legacy_key_fits);
    ASSERT_TRUE(probe.exact_key_overflows);
    ASSERT_GTE(alpha_edges, 1);
    ASSERT_EQ(beta_edges, 0);
    PASS();
}

typedef struct {
    int carrier_count;
    int exact_match_count;
} lsp_synthetic_index_probe_t;

static void inspect_synthetic_add_occurrences(CBMFileResult **result_cache, int file_count,
                                              void *ud) {
    lsp_synthetic_index_probe_t *probe = (lsp_synthetic_index_probe_t *)ud;
    if (!probe) {
        return;
    }
    for (int file = 0; file < file_count; file++) {
        CBMFileResult *result = result_cache[file];
        if (!result) {
            continue;
        }
        for (int i = 0; i < result->calls.count; i++) {
            const CBMCall *call = &result->calls.items[i];
            if (!call->requires_lsp_resolution || !call->callee_name ||
                strcmp(call->callee_name, "__add__") != 0 ||
                !cbm_pipeline_source_site_present(call->site_start_byte, call->site_end_byte)) {
                continue;
            }
            probe->carrier_count++;
            const CBMResolvedCall *resolved =
                cbm_pipeline_find_lsp_resolution(&result->resolved_calls, call, false);
            if (resolved &&
                cbm_pipeline_source_site_eq(call->site_start_byte, call->site_end_byte,
                                            resolved->site_start_byte, resolved->site_end_byte)) {
                probe->exact_match_count++;
            }
        }
    }
}

TEST(parallel_lsp_exact_index_handles_repeated_synthetic_occurrences_without_linear_scan) {
    enum { OCCURRENCES = 128 };
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_par_lsp_perf_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("mkdtemp failed");
    }
    char path[512];
    snprintf(path, sizeof(path), "%s/app.py", tmpdir);
    FILE *file = cbm_fopen(path, "w");
    if (!file) {
        rmdir(tmpdir);
        FAIL("fopen app.py failed");
    }
    fprintf(file, "class Number:\n"
                  "    def __add__(self, other):\n"
                  "        return self\n"
                  "\n"
                  "def combine(left: Number, right: Number):\n");
    for (int i = 0; i < OCCURRENCES; i++) {
        fprintf(file, "    value = left + right\n");
    }
    fprintf(file, "    return value\n");
    fclose(file);

    cbm_file_info_t files[1] = {0};
    files[0].path = path;
    files[0].rel_path = (char *)"app.py";
    files[0].language = CBM_LANG_PYTHON;
    lsp_synthetic_index_probe_t probe = {0};
    cbm_pp_lsp_linear_fallback_rows_reset();
    cbm_gbuf_t *gbuf = run_parallel_with_extract_opts_and_mutator(
        "cbm_par_lsp_perf", tmpdir, files, 1, 1, NULL, inspect_synthetic_add_occurrences, &probe,
        false);
    ASSERT_NOT_NULL(gbuf);
    ASSERT_EQ(probe.carrier_count, OCCURRENCES);
    ASSERT_EQ(probe.exact_match_count, OCCURRENCES);
    ASSERT_NOT_NULL(find_calls_edge_by_tails(gbuf, "app.combine", "Number.__add__"));
    uint64_t fallback_rows = cbm_pp_lsp_linear_fallback_rows();
    if (fallback_rows != 0) {
        printf("  synthetic exact-index diagnostic: occurrences=%d linear_rows=%llu\n", OCCURRENCES,
               (unsigned long long)fallback_rows);
    }
    ASSERT_EQ(fallback_rows, 0);

    cbm_gbuf_free(gbuf);
    unlink(path);
    rmdir(tmpdir);
    PASS();
}

#if defined(CBM_CALL_REFERENCE_LOOKUP_TEST_API) && CBM_CALL_REFERENCE_LOOKUP_TEST_API
enum { CALL_REFERENCE_SCALE_OCCURRENCES = 128 };

typedef struct {
    bool injected;
    int reference_count;
} call_reference_scale_probe_t;

/* Replace incidental extractor usages with a deterministic one-to-one set of
 * exact callable references. Every target is a real extracted Function node,
 * so the fused resolver must traverse its ordinary usage-materialization path
 * and emit 128 distinct CALL_REFERENCE edges. */
static void inject_call_reference_scale_rows(CBMFileResult **result_cache, int file_count,
                                             void *ud) {
    call_reference_scale_probe_t *probe = (call_reference_scale_probe_t *)ud;
    if (!probe || !result_cache) {
        return;
    }
    for (int file = 0; file < file_count; file++) {
        CBMFileResult *result = result_cache[file];
        if (!result) {
            continue;
        }
        const char *caller_qn = NULL;
        for (int i = 0; i < result->defs.count; i++) {
            const CBMDefinition *definition = &result->defs.items[i];
            if (definition->name && strcmp(definition->name, "scale_caller") == 0) {
                caller_qn = definition->qualified_name;
                break;
            }
        }
        if (!caller_qn) {
            continue;
        }

        result->usages.count = 0;
        result->resolved_calls.count = 0;
        int inserted = 0;
        for (int i = 0; i < result->defs.count; i++) {
            const CBMDefinition *target = &result->defs.items[i];
            if (!target->name || !target->qualified_name ||
                strncmp(target->name, "scale_target_", strlen("scale_target_")) != 0) {
                continue;
            }
            uint32_t start = 10000U + (uint32_t)inserted * 16U;
            CBMUsage usage = {0};
            usage.ref_name = target->name;
            usage.enclosing_func_qn = caller_qn;
            usage.kind = CBM_USAGE_CALL_REFERENCE;
            usage.may_be_call_reference = true;
            /* Exact occurrence proof must win even when lexical evidence
             * forbids the later raw-name fallback. */
            usage.semantic_reference_blocked = true;
            usage.semantic_reference_local_shadow = true;
            usage.site_start_byte = start;
            usage.site_end_byte = start + (uint32_t)strlen(target->name);
            cbm_usages_push(&result->usages, &result->arena, usage);

            CBMResolvedCall resolved = {0};
            resolved.caller_qn = caller_qn;
            resolved.callee_qn = target->qualified_name;
            resolved.strategy = "test_exact_reference_scale";
            resolved.confidence = 0.95f;
            resolved.kind = CBM_RESOLVED_CALL_REFERENCE;
            resolved.site_start_byte = usage.site_start_byte;
            resolved.site_end_byte = usage.site_end_byte;
            cbm_resolvedcall_push(&result->resolved_calls, &result->arena, resolved);
            inserted++;
        }
        probe->reference_count = inserted;
        probe->injected = inserted == CALL_REFERENCE_SCALE_OCCURRENCES;
        return;
    }
}

TEST(parallel_call_reference_lookup_rows_grow_linearly) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_par_ref_scale_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("mkdtemp failed");
    }
    char path[512];
    snprintf(path, sizeof(path), "%s/app.py", tmpdir);
    FILE *file = cbm_fopen(path, "w");
    if (!file) {
        rmdir(tmpdir);
        FAIL("fopen app.py failed");
    }
    for (int i = 0; i < CALL_REFERENCE_SCALE_OCCURRENCES; i++) {
        fprintf(file, "def scale_target_%03d():\n    return %d\n", i, i);
    }
    fputs("def scale_caller():\n    return None\n", file);
    fclose(file);

    cbm_file_info_t file_info = {0};
    file_info.path = path;
    file_info.rel_path = (char *)"app.py";
    file_info.language = CBM_LANG_PYTHON;
    call_reference_scale_probe_t probe = {0};

    cbm_pipeline_lsp_reference_lookup_test_reset();
    cbm_gbuf_t *gbuf = run_parallel_with_extract_opts_and_mutator(
        "cbm_par_ref_scale", tmpdir, &file_info, 1, 1, NULL, inject_call_reference_scale_rows,
        &probe, false);
    uint64_t rows_examined = cbm_pipeline_lsp_reference_lookup_test_rows_examined();

    const cbm_gbuf_node_t *caller =
        gbuf ? find_unique_callable_node_by_tail(gbuf, "app.scale_caller") : NULL;
    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    int lookup_rc = caller ? cbm_gbuf_find_edges_by_source_type(gbuf, caller->id, "CALL_REFERENCE",
                                                                &edges, &edge_count)
                           : -1;
    int exact_target_edges = 0;
    for (int i = 0; lookup_rc == 0 && i < edge_count; i++) {
        const cbm_gbuf_node_t *target =
            edges[i] ? cbm_gbuf_find_by_id(gbuf, edges[i]->target_id) : NULL;
        if (target && target->name &&
            strncmp(target->name, "scale_target_", strlen("scale_target_")) == 0) {
            exact_target_edges++;
        }
    }

    uint64_t linear_budget = (uint64_t)CALL_REFERENCE_SCALE_OCCURRENCES * 8U;
    if (!probe.injected || lookup_rc != 0 || edge_count != CALL_REFERENCE_SCALE_OCCURRENCES ||
        exact_target_edges != CALL_REFERENCE_SCALE_OCCURRENCES || rows_examined > linear_budget) {
        printf("  call-reference lookup scale diagnostic: injected=%d references=%d "
               "edges=%d exact_targets=%d rows=%llu budget=%llu\n",
               probe.injected, probe.reference_count, edge_count, exact_target_edges,
               (unsigned long long)rows_examined, (unsigned long long)linear_budget);
    }

    cbm_gbuf_free(gbuf);
    unlink(path);
    rmdir(tmpdir);
    ASSERT_TRUE(probe.injected);
    ASSERT_EQ(probe.reference_count, CALL_REFERENCE_SCALE_OCCURRENCES);
    ASSERT_EQ(lookup_rc, 0);
    ASSERT_EQ(edge_count, CALL_REFERENCE_SCALE_OCCURRENCES);
    ASSERT_EQ(exact_target_edges, CALL_REFERENCE_SCALE_OCCURRENCES);
    /* The current full scan examines N rows for every one of N usages:
     * 128*128 = 16,384. A site index should keep this below a small linear
     * allowance while retaining the exact target/ambiguity checks. */
    ASSERT_LTE(rows_examined, linear_budget);
    PASS();
}
#endif

/* A lexical callable named like a module component owns the JSX occurrence.
 * Keep a direct component call as a positive control, and require both graph
 * builders to reject textual fallback for the shadowed tag. */
TEST(parallel_tsx_local_component_shadow_has_no_false_call) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_par_tsx_shadow_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("mkdtemp failed");
    }
    char path[512];
    snprintf(path, sizeof(path), "%s/main.tsx", tmpdir);
    FILE *file = cbm_fopen(path, "w");
    if (!file) {
        rmdir(tmpdir);
        FAIL("fopen main.tsx failed");
    }
    fputs("function Card(): JSX.Element { return <div/>; }\n"
          "function Good(): JSX.Element { return <Card/>; }\n"
          "function Shadowed(Card: () => JSX.Element): JSX.Element { return <Card/>; }\n",
          file);
    fclose(file);

    cbm_file_info_t files[1] = {0};
    files[0].path = path;
    files[0].rel_path = (char *)"main.tsx";
    files[0].language = CBM_LANG_TSX;
    cbm_gbuf_t *sequential = run_sequential("cbm_tsx_shadow", tmpdir, files, 1);
    cbm_gbuf_t *parallel = run_parallel("cbm_tsx_shadow", tmpdir, files, 1, 1);
    ASSERT_NOT_NULL(sequential);
    ASSERT_NOT_NULL(parallel);

    const bool sequential_nodes = find_unique_callable_node_by_tail(sequential, "main.Card") &&
                                  find_unique_callable_node_by_tail(sequential, "main.Good") &&
                                  find_unique_callable_node_by_tail(sequential, "main.Shadowed");
    const bool parallel_nodes = find_unique_callable_node_by_tail(parallel, "main.Card") &&
                                find_unique_callable_node_by_tail(parallel, "main.Good") &&
                                find_unique_callable_node_by_tail(parallel, "main.Shadowed");
    const bool sequential_good =
        find_calls_edge_by_tails(sequential, "main.Good", "main.Card") != NULL;
    const bool parallel_good = find_calls_edge_by_tails(parallel, "main.Good", "main.Card") != NULL;
    const bool sequential_wrong =
        find_calls_edge_by_tails(sequential, "main.Shadowed", "main.Card") != NULL;
    const bool parallel_wrong =
        find_calls_edge_by_tails(parallel, "main.Shadowed", "main.Card") != NULL;
    if (!sequential_nodes || !parallel_nodes || !sequential_good || !parallel_good ||
        sequential_wrong || parallel_wrong) {
        printf("  TSX shadow graph diagnostic: seq_nodes=%d par_nodes=%d seq_good=%d "
               "par_good=%d seq_wrong=%d par_wrong=%d\n",
               sequential_nodes, parallel_nodes, sequential_good, parallel_good, sequential_wrong,
               parallel_wrong);
    }

    cbm_gbuf_free(sequential);
    cbm_gbuf_free(parallel);
    unlink(path);
    rmdir(tmpdir);
    ASSERT_TRUE(sequential_nodes);
    ASSERT_TRUE(parallel_nodes);
    ASSERT_TRUE(sequential_good);
    ASSERT_TRUE(parallel_good);
    ASSERT_FALSE(sequential_wrong);
    ASSERT_FALSE(parallel_wrong);
    PASS();
}

typedef struct {
    bool injected;
} exact_noncallable_reference_probe_t;

static void inject_exact_noncallable_reference(CBMFileResult **result_cache, int file_count,
                                               void *ud) {
    exact_noncallable_reference_probe_t *probe = (exact_noncallable_reference_probe_t *)ud;
    if (!probe || !result_cache) {
        return;
    }
    for (int file = 0; file < file_count; file++) {
        CBMFileResult *result = result_cache[file];
        if (!result) {
            continue;
        }
        const char *caller_qn = NULL;
        const char *target_qn = NULL;
        for (int i = 0; i < result->defs.count; i++) {
            const CBMDefinition *definition = &result->defs.items[i];
            if (definition->name && definition->qualified_name &&
                strcmp(definition->name, "carrier") == 0) {
                caller_qn = definition->qualified_name;
            }
            if (definition->name && definition->qualified_name && definition->label &&
                strcmp(definition->name, "semanticValue") == 0 &&
                strcmp(definition->label, "Variable") == 0) {
                target_qn = definition->qualified_name;
            }
        }
        if (!caller_qn || !target_qn) {
            continue;
        }

        result->usages.count = 0;
        result->resolved_calls.count = 0;
        CBMUsage usage = {0};
        usage.ref_name = "semanticAlias";
        usage.enclosing_func_qn = caller_qn;
        usage.kind = CBM_USAGE_VALUE;
        usage.may_be_call_reference = true;
        /* The raw-name path is deliberately unavailable. A resulting USAGE
         * therefore proves that the exact semantic row was joined and then
         * classified by the materialized target's non-callable label. */
        usage.semantic_reference_blocked = true;
        usage.site_start_byte = 40000U;
        usage.site_end_byte = 40000U + (uint32_t)strlen(usage.ref_name);
        cbm_usages_push(&result->usages, &result->arena, usage);

        CBMResolvedCall resolved = {0};
        resolved.caller_qn = caller_qn;
        resolved.callee_qn = target_qn;
        resolved.strategy = "lsp_callable_value_reference";
        resolved.reason = usage.ref_name;
        resolved.confidence = 0.99f;
        resolved.kind = CBM_RESOLVED_CALL_REFERENCE;
        resolved.site_start_byte = usage.site_start_byte;
        resolved.site_end_byte = usage.site_end_byte;
        cbm_resolvedcall_push(&result->resolved_calls, &result->arena, resolved);
        probe->injected = true;
        return;
    }
}

TEST(parallel_exact_semantic_noncallable_target_stays_usage) {
    static const char source[] = "const semanticValue = 1;\n"
                                 "function consume(value: unknown): void {}\n"
                                 "export function carrier(): void { consume(semanticValue); }\n";
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_par_exact_noncallable_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("mkdtemp failed");
    }
    char path[512];
    snprintf(path, sizeof(path), "%s/app.ts", tmpdir);
    if (th_write_file(path, source) != 0) {
        th_rmtree(tmpdir);
        FAIL("failed to write exact non-callable fixture");
    }
    cbm_file_info_t file = {0};
    file.path = path;
    file.rel_path = (char *)"app.ts";
    file.language = CBM_LANG_TYPESCRIPT;
    exact_noncallable_reference_probe_t sequential_probe = {0};
    exact_noncallable_reference_probe_t parallel_probe = {0};
    cbm_gbuf_t *sequential = run_sequential_with_lsp_cross_and_mutator(
        "cbm_exact_noncallable", tmpdir, &file, 1, inject_exact_noncallable_reference,
        &sequential_probe, false);
    cbm_gbuf_t *parallel = run_parallel_with_extract_opts_and_mutator(
        "cbm_exact_noncallable", tmpdir, &file, 1, 1, NULL, inject_exact_noncallable_reference,
        &parallel_probe, false);
    ASSERT_NOT_NULL(sequential);
    ASSERT_NOT_NULL(parallel);

    const cbm_gbuf_node_t *sequential_target = find_unique_node_by_name_label_qn_suffix(
        sequential, "semanticValue", "Variable", "app.semanticValue");
    const cbm_gbuf_node_t *parallel_target = find_unique_node_by_name_label_qn_suffix(
        parallel, "semanticValue", "Variable", "app.semanticValue");
    const bool sequential_usage =
        has_edge_from_callable_to_node(sequential, "app.carrier", sequential_target, "USAGE");
    const bool parallel_usage =
        has_edge_from_callable_to_node(parallel, "app.carrier", parallel_target, "USAGE");
    const bool sequential_promoted =
        has_edge_from_callable_to_node(sequential, "app.carrier", sequential_target,
                                       "CALL_REFERENCE") ||
        has_edge_from_callable_to_node(sequential, "app.carrier", sequential_target, "CALLS");
    const bool parallel_promoted =
        has_edge_from_callable_to_node(parallel, "app.carrier", parallel_target,
                                       "CALL_REFERENCE") ||
        has_edge_from_callable_to_node(parallel, "app.carrier", parallel_target, "CALLS");

    cbm_gbuf_free(sequential);
    cbm_gbuf_free(parallel);
    th_rmtree(tmpdir);
    ASSERT_TRUE(sequential_probe.injected);
    ASSERT_TRUE(parallel_probe.injected);
    ASSERT_NOT_NULL(sequential_target);
    ASSERT_NOT_NULL(parallel_target);
    ASSERT_TRUE(sequential_usage);
    ASSERT_TRUE(parallel_usage);
    ASSERT_FALSE(sequential_promoted);
    ASSERT_FALSE(parallel_promoted);
    PASS();
}

/* A value argument whose exact target is a module Variable remains an ordinary
 * USAGE even when the TS semantic pass marks the occurrence as reference-like.
 * Conversely, a parameter or local declaration with the same spelling owns its
 * lexical occurrence and must not fall through to the module Variable. Exercise
 * both the sequential pass_usages path and the fused parallel resolver. */
TEST(parallel_typescript_module_value_usage_respects_lexical_shadows) {
    static const char settings_source[] =
        "export const config = { enabled: true };\n"
        "function consumeConfig(value: { enabled: boolean }): void {}\n"
        "export function moduleArgument(): void { consumeConfig(config); }\n";
    static const char shadows_source[] =
        "import { config } from './settings';\n"
        "function consumeShadow(value: { enabled: boolean }): void {}\n"
        "export function parameterShadow(config: { enabled: boolean }): void {\n"
        "  consumeShadow(config);\n"
        "}\n"
        "export function localShadow(): void {\n"
        "  const config = { enabled: false };\n"
        "  consumeShadow(config);\n"
        "}\n";

    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_par_ts_usage_scope_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("mkdtemp failed");
    }
    char settings_path[512];
    char shadows_path[512];
    snprintf(settings_path, sizeof(settings_path), "%s/settings.ts", tmpdir);
    snprintf(shadows_path, sizeof(shadows_path), "%s/shadows.ts", tmpdir);
    if (th_write_file(settings_path, settings_source) != 0 ||
        th_write_file(shadows_path, shadows_source) != 0) {
        th_rmtree(tmpdir);
        FAIL("failed to write TypeScript usage-scope fixture");
    }

    cbm_file_info_t files[2] = {0};
    files[0].path = settings_path;
    files[0].rel_path = (char *)"settings.ts";
    files[0].language = CBM_LANG_TYPESCRIPT;
    files[1].path = shadows_path;
    files[1].rel_path = (char *)"shadows.ts";
    files[1].language = CBM_LANG_TYPESCRIPT;
    cbm_gbuf_t *sequential = run_sequential("cbm_ts_usage_scope", tmpdir, files, 2);
    cbm_gbuf_t *parallel = run_parallel("cbm_ts_usage_scope", tmpdir, files, 2, 2);
    ASSERT_NOT_NULL(sequential);
    ASSERT_NOT_NULL(parallel);

    const cbm_gbuf_node_t *sequential_config = find_unique_node_by_name_label_qn_suffix(
        sequential, "config", "Variable", "settings.config");
    const cbm_gbuf_node_t *parallel_config =
        find_unique_node_by_name_label_qn_suffix(parallel, "config", "Variable", "settings.config");
    const bool sequential_target = sequential_config != NULL;
    const bool parallel_target = parallel_config != NULL;
    const bool sequential_callers =
        find_unique_callable_node_by_tail(sequential, "settings.moduleArgument") &&
        find_unique_callable_node_by_tail(sequential, "shadows.parameterShadow") &&
        find_unique_callable_node_by_tail(sequential, "shadows.localShadow");
    const bool parallel_callers =
        find_unique_callable_node_by_tail(parallel, "settings.moduleArgument") &&
        find_unique_callable_node_by_tail(parallel, "shadows.parameterShadow") &&
        find_unique_callable_node_by_tail(parallel, "shadows.localShadow");

    const bool sequential_module_usage = has_edge_from_callable_to_node(
        sequential, "settings.moduleArgument", sequential_config, "USAGE");
    const bool parallel_module_usage = has_edge_from_callable_to_node(
        parallel, "settings.moduleArgument", parallel_config, "USAGE");
    const bool sequential_module_reference =
        has_edge_from_callable_to_node(sequential, "settings.moduleArgument", sequential_config,
                                       "CALL_REFERENCE") ||
        has_edge_from_callable_to_node(sequential, "settings.moduleArgument", sequential_config,
                                       "CALLS");
    const bool parallel_module_reference =
        has_edge_from_callable_to_node(parallel, "settings.moduleArgument", parallel_config,
                                       "CALL_REFERENCE") ||
        has_edge_from_callable_to_node(parallel, "settings.moduleArgument", parallel_config,
                                       "CALLS");
    const bool sequential_parameter_leak =
        has_edge_from_callable_to_node(sequential, "shadows.parameterShadow", sequential_config,
                                       "USAGE") ||
        has_edge_from_callable_to_node(sequential, "shadows.parameterShadow", sequential_config,
                                       "CALL_REFERENCE") ||
        has_edge_from_callable_to_node(sequential, "shadows.parameterShadow", sequential_config,
                                       "CALLS");
    const bool parallel_parameter_leak =
        has_edge_from_callable_to_node(parallel, "shadows.parameterShadow", parallel_config,
                                       "USAGE") ||
        has_edge_from_callable_to_node(parallel, "shadows.parameterShadow", parallel_config,
                                       "CALL_REFERENCE") ||
        has_edge_from_callable_to_node(parallel, "shadows.parameterShadow", parallel_config,
                                       "CALLS");
    const bool sequential_local_leak =
        has_edge_from_callable_to_node(sequential, "shadows.localShadow", sequential_config,
                                       "USAGE") ||
        has_edge_from_callable_to_node(sequential, "shadows.localShadow", sequential_config,
                                       "CALL_REFERENCE") ||
        has_edge_from_callable_to_node(sequential, "shadows.localShadow", sequential_config,
                                       "CALLS");
    const bool parallel_local_leak =
        has_edge_from_callable_to_node(parallel, "shadows.localShadow", parallel_config, "USAGE") ||
        has_edge_from_callable_to_node(parallel, "shadows.localShadow", parallel_config,
                                       "CALL_REFERENCE") ||
        has_edge_from_callable_to_node(parallel, "shadows.localShadow", parallel_config, "CALLS");

    if (!sequential_target || !parallel_target || !sequential_callers || !parallel_callers ||
        !sequential_module_usage || !parallel_module_usage || sequential_module_reference ||
        parallel_module_reference || sequential_parameter_leak || parallel_parameter_leak ||
        sequential_local_leak || parallel_local_leak) {
        printf("  TS module-value diagnostic: target=%d/%d callers=%d/%d usage=%d/%d "
               "reference=%d/%d parameter_leak=%d/%d local_leak=%d/%d\n",
               sequential_target, parallel_target, sequential_callers, parallel_callers,
               sequential_module_usage, parallel_module_usage, sequential_module_reference,
               parallel_module_reference, sequential_parameter_leak, parallel_parameter_leak,
               sequential_local_leak, parallel_local_leak);
    }

    cbm_gbuf_free(sequential);
    cbm_gbuf_free(parallel);
    th_rmtree(tmpdir);
    ASSERT_TRUE(sequential_target);
    ASSERT_TRUE(parallel_target);
    ASSERT_TRUE(sequential_callers);
    ASSERT_TRUE(parallel_callers);
    ASSERT_TRUE(sequential_module_usage);
    ASSERT_TRUE(parallel_module_usage);
    ASSERT_FALSE(sequential_module_reference);
    ASSERT_FALSE(parallel_module_reference);
    ASSERT_FALSE(sequential_parameter_leak);
    ASSERT_FALSE(parallel_parameter_leak);
    ASSERT_FALSE(sequential_local_leak);
    ASSERT_FALSE(parallel_local_leak);
    PASS();
}

enum {
    TS_EXACT_SELECTED_IMPORT = 0,
    TS_EXACT_DECOY_IMPORT,
    TS_EXACT_LOCAL_CLASS,
    TS_EXACT_NAMESPACE_INNER_CALL,
    TS_EXACT_NAMESPACE_CONSTRUCTOR_CALL,
    TS_EXACT_EDGE_COUNT
};

typedef struct {
    int edge_counts[2][TS_EXACT_EDGE_COUNT]; /* sequential, parallel */
} ts_exact_parity_observation_t;

static ts_exact_parity_observation_t run_ts_exact_parity_fixture(CBMLanguage main_language,
                                                                 const char *extension,
                                                                 const char *project) {
    ts_exact_parity_observation_t observation;
    for (int route = 0; route < 2; route++) {
        for (int edge = 0; edge < TS_EXACT_EDGE_COUNT; edge++) {
            observation.edge_counts[route][edge] = -1;
        }
    }

    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_par_ts_exact_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        return observation;
    }

    char selected_path[512];
    char decoy_path[512];
    char main_path[512];
    char main_rel[64];
    snprintf(selected_path, sizeof(selected_path), "%s/a/types.ts", tmpdir);
    snprintf(decoy_path, sizeof(decoy_path), "%s/b/types.ts", tmpdir);
    snprintf(main_path, sizeof(main_path), "%s/main.%s", tmpdir, extension);
    snprintf(main_rel, sizeof(main_rel), "main.%s", extension);

    static const char type_source[] = "export interface Config { enabled: boolean }\n";
    static const char main_source[] =
        "import type { Config } from './a/types';\n"
        "export function choose(cfg: Config): boolean { return cfg.enabled; }\n"
        "namespace App {\n"
        "  export namespace Utils {\n"
        "    export function clamp(): number { return 1; }\n"
        "    export function normalise(): number { return clamp(); }\n"
        "  }\n"
        "  export class Config {\n"
        "    constructor() { Utils.normalise(); }\n"
        "  }\n"
        "}\n";
    if (th_write_file(selected_path, type_source) != 0 ||
        th_write_file(decoy_path, type_source) != 0 || th_write_file(main_path, main_source) != 0) {
        th_rmtree(tmpdir);
        return observation;
    }

    cbm_file_info_t files[3] = {0};
    files[0].path = selected_path;
    files[0].rel_path = (char *)"a/types.ts";
    files[0].language = CBM_LANG_TYPESCRIPT;
    files[1].path = decoy_path;
    files[1].rel_path = (char *)"b/types.ts";
    files[1].language = CBM_LANG_TYPESCRIPT;
    files[2].path = main_path;
    files[2].rel_path = main_rel;
    files[2].language = main_language;

    cbm_gbuf_t *graphs[2] = {
        run_sequential_with_lsp_cross_and_mutator(project, tmpdir, files, 3, NULL, NULL, true),
        run_parallel_with_extract_opts_and_mutator(project, tmpdir, files, 3, 2, NULL, NULL, NULL,
                                                   true),
    };
    for (int route = 0; route < 2; route++) {
        cbm_gbuf_t *graph = graphs[route];
        const cbm_gbuf_node_t *choose =
            find_unique_node_by_name_label_qn_suffix(graph, "choose", "Function", "main.choose");
        const cbm_gbuf_node_t *selected = find_unique_node_by_name_label_qn_suffix(
            graph, "Config", "Interface", "a.types.Config");
        const cbm_gbuf_node_t *decoy = find_unique_node_by_name_label_qn_suffix(
            graph, "Config", "Interface", "b.types.Config");
        const cbm_gbuf_node_t *local_class =
            find_unique_node_by_name_label_qn_suffix(graph, "Config", "Class", "main.App.Config");
        const cbm_gbuf_node_t *normalise = find_unique_node_by_name_label_qn_suffix(
            graph, "normalise", "Function", "main.App.Utils.normalise");
        const cbm_gbuf_node_t *clamp = find_unique_node_by_name_label_qn_suffix(
            graph, "clamp", "Function", "main.App.Utils.clamp");
        const cbm_gbuf_node_t *constructor = find_unique_node_by_name_label_qn_suffix(
            graph, "constructor", "Method", "main.App.Config.constructor");

        observation.edge_counts[route][TS_EXACT_SELECTED_IMPORT] =
            count_edges_between_nodes(graph, choose, selected, "USAGE");
        observation.edge_counts[route][TS_EXACT_DECOY_IMPORT] =
            count_edges_between_nodes(graph, choose, decoy, "USAGE");
        observation.edge_counts[route][TS_EXACT_LOCAL_CLASS] =
            count_edges_between_nodes(graph, choose, local_class, "USAGE");
        observation.edge_counts[route][TS_EXACT_NAMESPACE_INNER_CALL] =
            count_edges_between_nodes(graph, normalise, clamp, "CALLS");
        observation.edge_counts[route][TS_EXACT_NAMESPACE_CONSTRUCTOR_CALL] =
            count_edges_between_nodes(graph, constructor, normalise, "CALLS");
    }

    cbm_gbuf_free(graphs[0]);
    cbm_gbuf_free(graphs[1]);
    th_rmtree(tmpdir);
    return observation;
}

static void print_ts_exact_parity_diagnostic(const char *language,
                                             const ts_exact_parity_observation_t *observation) {
    printf("  %s exact parity: seq=[%d,%d,%d,%d,%d] par=[%d,%d,%d,%d,%d]\n", language,
           observation->edge_counts[0][TS_EXACT_SELECTED_IMPORT],
           observation->edge_counts[0][TS_EXACT_DECOY_IMPORT],
           observation->edge_counts[0][TS_EXACT_LOCAL_CLASS],
           observation->edge_counts[0][TS_EXACT_NAMESPACE_INNER_CALL],
           observation->edge_counts[0][TS_EXACT_NAMESPACE_CONSTRUCTOR_CALL],
           observation->edge_counts[1][TS_EXACT_SELECTED_IMPORT],
           observation->edge_counts[1][TS_EXACT_DECOY_IMPORT],
           observation->edge_counts[1][TS_EXACT_LOCAL_CLASS],
           observation->edge_counts[1][TS_EXACT_NAMESPACE_INNER_CALL],
           observation->edge_counts[1][TS_EXACT_NAMESPACE_CONSTRUCTOR_CALL]);
}

TEST(parallel_typescript_import_namespace_exact_parity) {
    ts_exact_parity_observation_t observation =
        run_ts_exact_parity_fixture(CBM_LANG_TYPESCRIPT, "ts", "cbm_ts_exact_parity");
    print_ts_exact_parity_diagnostic("TypeScript", &observation);
    for (int route = 0; route < 2; route++) {
        ASSERT_EQ(observation.edge_counts[route][TS_EXACT_SELECTED_IMPORT], 1);
        ASSERT_EQ(observation.edge_counts[route][TS_EXACT_DECOY_IMPORT], 0);
        ASSERT_EQ(observation.edge_counts[route][TS_EXACT_LOCAL_CLASS], 0);
        ASSERT_EQ(observation.edge_counts[route][TS_EXACT_NAMESPACE_INNER_CALL], 1);
        ASSERT_EQ(observation.edge_counts[route][TS_EXACT_NAMESPACE_CONSTRUCTOR_CALL], 1);
    }
    PASS();
}

TEST(parallel_tsx_import_namespace_exact_parity) {
    ts_exact_parity_observation_t observation =
        run_ts_exact_parity_fixture(CBM_LANG_TSX, "tsx", "cbm_tsx_exact_parity");
    print_ts_exact_parity_diagnostic("TSX", &observation);
    for (int route = 0; route < 2; route++) {
        ASSERT_EQ(observation.edge_counts[route][TS_EXACT_SELECTED_IMPORT], 1);
        ASSERT_EQ(observation.edge_counts[route][TS_EXACT_DECOY_IMPORT], 0);
        ASSERT_EQ(observation.edge_counts[route][TS_EXACT_LOCAL_CLASS], 0);
        ASSERT_EQ(observation.edge_counts[route][TS_EXACT_NAMESPACE_INNER_CALL], 1);
        ASSERT_EQ(observation.edge_counts[route][TS_EXACT_NAMESPACE_CONSTRUCTOR_CALL], 1);
    }
    PASS();
}

typedef struct {
    bool carrier;
    bool semantic;
    bool exact_join;
} kotlin_implicit_probe_t;

static kotlin_implicit_probe_t probe_kotlin_implicit_site(const CBMFileResult *result,
                                                          const char *caller, const char *callee,
                                                          const char *target_qn,
                                                          const char *strategy) {
    kotlin_implicit_probe_t probe = {0};
    if (!result || !caller || !callee || !target_qn || !strategy) {
        return probe;
    }
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        const char *call_caller =
            call->enclosing_func_qn ? strrchr(call->enclosing_func_qn, '.') : NULL;
        call_caller = call_caller ? call_caller + 1 : call->enclosing_func_qn;
        if (!call->requires_lsp_resolution || !call->callee_name || !call_caller ||
            strcmp(call->callee_name, callee) != 0 || strcmp(call_caller, caller) != 0 ||
            !cbm_pipeline_source_site_present(call->site_start_byte, call->site_end_byte)) {
            continue;
        }
        probe.carrier = true;
    }
    for (int j = 0; j < result->resolved_calls.count; j++) {
        const CBMResolvedCall *resolved = &result->resolved_calls.items[j];
        const char *resolved_caller =
            resolved->caller_qn ? strrchr(resolved->caller_qn, '.') : NULL;
        resolved_caller = resolved_caller ? resolved_caller + 1 : resolved->caller_qn;
        if (resolved->kind != CBM_RESOLVED_INVOCATION || !resolved_caller || !resolved->callee_qn ||
            !resolved->strategy || strcmp(resolved_caller, caller) != 0 ||
            strcmp(resolved->callee_qn, target_qn) != 0 ||
            strcmp(resolved->strategy, strategy) != 0) {
            continue;
        }
        probe.semantic = true;
        for (int i = 0; i < result->calls.count; i++) {
            const CBMCall *call = &result->calls.items[i];
            const char *call_caller =
                call->enclosing_func_qn ? strrchr(call->enclosing_func_qn, '.') : NULL;
            call_caller = call_caller ? call_caller + 1 : call->enclosing_func_qn;
            if (!call->requires_lsp_resolution || !call->callee_name || !call_caller ||
                strcmp(call->callee_name, callee) != 0 || strcmp(call_caller, caller) != 0) {
                continue;
            }
            if (cbm_pipeline_source_site_eq(call->site_start_byte, call->site_end_byte,
                                            resolved->site_start_byte, resolved->site_end_byte)) {
                probe.exact_join = true;
            }
        }
    }
    return probe;
}

/* External Kotlin protocol targets must not borrow a project method merely
 * because the final Class.method segments agree. Raw assertions prove that the
 * iterator/destructuring carriers and their stdlib semantic rows really exist;
 * the graph negatives therefore cannot pass by dropping synthesis. Local alias
 * and operator calls are positive controls for the narrowed target policy. */
TEST(parallel_kotlin_external_protocol_does_not_use_project_class_method_tail) {
    static const char collision_source[] = "package collision\n"
                                           "class IntArray {\n"
                                           "  fun iterator(): IntArray = this\n"
                                           "}\n"
                                           "class Pair {\n"
                                           "  operator fun component1(): Int = 0\n"
                                           "}\n";
    static const char main_source[] =
        "package app\n"
        "fun loopBuiltIn(values: kotlin.IntArray) {\n"
        "  for (value in values) { println(value) }\n"
        "}\n"
        "fun destructureBuiltIn(value: kotlin.Pair<Int, Int>) {\n"
        "  val (left, right) = value\n"
        "}\n"
        "fun projectHandler(): Int = 7\n"
        "fun aliasCaller(): Int {\n"
        "  val callback = ::projectHandler\n"
        "  return callback()\n"
        "}\n"
        "class Box { operator fun plus(other: Box): Box = this }\n"
        "fun operatorCaller(left: Box, right: Box): Box = left + right\n";
    const char *project = "cbm_kt_target_policy";

    CBMFileResult *raw = cbm_extract_file(main_source, (int)strlen(main_source), CBM_LANG_KOTLIN,
                                          project, "app/Main.kt", 0, NULL, NULL);
    ASSERT_NOT_NULL(raw);
    ASSERT_FALSE(raw->has_error || raw->parse_incomplete);
    kotlin_implicit_probe_t iterator = probe_kotlin_implicit_site(
        raw, "loopBuiltIn", "iterator", "kotlin.IntArray.iterator", "lsp_kt_iterator");
    kotlin_implicit_probe_t component = probe_kotlin_implicit_site(
        raw, "destructureBuiltIn", "component1", "kotlin.Pair.component1", "lsp_kt_destructure");
    cbm_free_result(raw);

    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_par_kt_target_policy_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("mkdtemp failed");
    }
    char collision_path[512];
    char main_path[512];
    snprintf(collision_path, sizeof(collision_path), "%s/collision/Collisions.kt", tmpdir);
    snprintf(main_path, sizeof(main_path), "%s/app/Main.kt", tmpdir);
    if (th_write_file(collision_path, collision_source) != 0 ||
        th_write_file(main_path, main_source) != 0) {
        th_rmtree(tmpdir);
        FAIL("failed to write Kotlin target-policy fixture");
    }

    cbm_file_info_t files[2] = {0};
    files[0].path = collision_path;
    files[0].rel_path = (char *)"collision/Collisions.kt";
    files[0].language = CBM_LANG_KOTLIN;
    files[1].path = main_path;
    files[1].rel_path = (char *)"app/Main.kt";
    files[1].language = CBM_LANG_KOTLIN;
    cbm_gbuf_t *sequential = run_sequential_with_lsp_cross(project, tmpdir, files, 2);
    cbm_gbuf_t *parallel = run_parallel(project, tmpdir, files, 2, 2);
    ASSERT_NOT_NULL(sequential);
    ASSERT_NOT_NULL(parallel);

    const cbm_gbuf_node_t *sequential_iterator = find_unique_node_by_name_label_qn_suffix(
        sequential, "iterator", "Method", "Collisions.IntArray.iterator");
    const cbm_gbuf_node_t *parallel_iterator = find_unique_node_by_name_label_qn_suffix(
        parallel, "iterator", "Method", "Collisions.IntArray.iterator");
    const cbm_gbuf_node_t *sequential_component = find_unique_node_by_name_label_qn_suffix(
        sequential, "component1", "Method", "Collisions.Pair.component1");
    const cbm_gbuf_node_t *parallel_component = find_unique_node_by_name_label_qn_suffix(
        parallel, "component1", "Method", "Collisions.Pair.component1");
    const bool sequential_nodes =
        sequential_iterator && sequential_component &&
        find_unique_callable_node_by_tail(sequential, "Main.loopBuiltIn") &&
        find_unique_callable_node_by_tail(sequential, "Main.destructureBuiltIn");
    const bool parallel_nodes =
        parallel_iterator && parallel_component &&
        find_unique_callable_node_by_tail(parallel, "Main.loopBuiltIn") &&
        find_unique_callable_node_by_tail(parallel, "Main.destructureBuiltIn");
    const bool sequential_alias =
        find_calls_edge_by_tails(sequential, "Main.aliasCaller", "Main.projectHandler") != NULL;
    const bool parallel_alias =
        find_calls_edge_by_tails(parallel, "Main.aliasCaller", "Main.projectHandler") != NULL;
    const bool sequential_operator =
        find_calls_edge_by_tails(sequential, "Main.operatorCaller", "Box.plus") != NULL;
    const bool parallel_operator =
        find_calls_edge_by_tails(parallel, "Main.operatorCaller", "Box.plus") != NULL;
    const bool sequential_wrong_iterator = has_edge_from_callable_to_node(
        sequential, "Main.loopBuiltIn", sequential_iterator, "CALLS");
    const bool parallel_wrong_iterator =
        has_edge_from_callable_to_node(parallel, "Main.loopBuiltIn", parallel_iterator, "CALLS");
    const bool sequential_wrong_component = has_edge_from_callable_to_node(
        sequential, "Main.destructureBuiltIn", sequential_component, "CALLS");
    const bool parallel_wrong_component = has_edge_from_callable_to_node(
        parallel, "Main.destructureBuiltIn", parallel_component, "CALLS");

    if (!iterator.carrier || !iterator.semantic || !iterator.exact_join || !component.carrier ||
        !component.semantic || !component.exact_join || !sequential_nodes || !parallel_nodes ||
        !sequential_alias || !parallel_alias || !sequential_operator || !parallel_operator ||
        sequential_wrong_iterator || parallel_wrong_iterator || sequential_wrong_component ||
        parallel_wrong_component) {
        printf("  Kotlin target-policy diagnostic: iterator_raw=%d/%d/%d "
               "component_raw=%d/%d/%d nodes=%d/%d alias=%d/%d operator=%d/%d "
               "wrong_iterator=%d/%d wrong_component=%d/%d\n",
               iterator.carrier, iterator.semantic, iterator.exact_join, component.carrier,
               component.semantic, component.exact_join, sequential_nodes, parallel_nodes,
               sequential_alias, parallel_alias, sequential_operator, parallel_operator,
               sequential_wrong_iterator, parallel_wrong_iterator, sequential_wrong_component,
               parallel_wrong_component);
    }

    cbm_gbuf_free(sequential);
    cbm_gbuf_free(parallel);
    th_rmtree(tmpdir);
    ASSERT_TRUE(iterator.carrier);
    ASSERT_TRUE(iterator.semantic);
    ASSERT_TRUE(iterator.exact_join);
    ASSERT_TRUE(component.carrier);
    ASSERT_TRUE(component.semantic);
    ASSERT_TRUE(component.exact_join);
    ASSERT_TRUE(sequential_nodes);
    ASSERT_TRUE(parallel_nodes);
    ASSERT_TRUE(sequential_alias);
    ASSERT_TRUE(parallel_alias);
    ASSERT_TRUE(sequential_operator);
    ASSERT_TRUE(parallel_operator);
    ASSERT_FALSE(sequential_wrong_iterator);
    ASSERT_FALSE(parallel_wrong_iterator);
    ASSERT_FALSE(sequential_wrong_component);
    ASSERT_FALSE(parallel_wrong_component);
    PASS();
}

/* Kotlin's unary, membership, and index conventions are real invocations just
 * like binary `plus`: the LSP already proves the exact operator method at the
 * exact expression span. The syntax extractor must provide a matching guarded
 * CBMCall carrier so both graph paths can materialize CALLS without a textual
 * guess. This test separately proves carrier, semantic row, exact join, and
 * graph edge for each syntax family. */
TEST(parallel_kotlin_nonbinary_operator_carriers_reach_graph) {
    static const char definitions_source[] =
        "package app\n"
        "class UnaryBox { operator fun unaryMinus(): UnaryBox = this }\n"
        "class Bag { operator fun contains(value: Int): Boolean = true }\n"
        "class Slots { operator fun get(index: Int): Int = index }\n"
        "class Counter {\n"
        "  operator fun inc(): Counter = this\n"
        "  operator fun dec(): Counter = this\n"
        "}\n";
    static const char callers_source[] = "package app\n"
                                         "fun negate(value: UnaryBox): UnaryBox = -value\n"
                                         "fun hasValue(bag: Bag): Boolean = 1 in bag\n"
                                         "fun lacksValue(bag: Bag): Boolean = 1 !in bag\n"
                                         "fun lookup(slots: Slots): Int = slots[0]\n"
                                         "fun increment(value: Counter): Counter { var current = "
                                         "value; current++; return current }\n"
                                         "fun decrement(value: Counter): Counter { var current = "
                                         "value; current--; return current }\n"
                                         "fun isBag(value: Any): Boolean = value is Bag\n"
                                         "fun assign(slots: Slots) { slots[0] = 1 }\n";
    static const char source[] = "package app\n"
                                 "class UnaryBox { operator fun unaryMinus(): UnaryBox = this }\n"
                                 "class Bag { operator fun contains(value: Int): Boolean = true }\n"
                                 "class Slots { operator fun get(index: Int): Int = index }\n"
                                 "class Counter {\n"
                                 "  operator fun inc(): Counter = this\n"
                                 "  operator fun dec(): Counter = this\n"
                                 "}\n"
                                 "fun negate(value: UnaryBox): UnaryBox = -value\n"
                                 "fun hasValue(bag: Bag): Boolean = 1 in bag\n"
                                 "fun lacksValue(bag: Bag): Boolean = 1 !in bag\n"
                                 "fun lookup(slots: Slots): Int = slots[0]\n"
                                 "fun increment(value: Counter): Counter { var current = value; "
                                 "current++; return current }\n"
                                 "fun decrement(value: Counter): Counter { var current = value; "
                                 "current--; return current }\n"
                                 "fun isBag(value: Any): Boolean = value is Bag\n"
                                 "fun assign(slots: Slots) { slots[0] = 1 }\n";
    const char *project = "cbm_kt_nonbinary_operators";

    CBMFileResult *raw = cbm_extract_file(source, (int)strlen(source), CBM_LANG_KOTLIN, project,
                                          "app/Main.kt", 0, NULL, NULL);
    ASSERT_NOT_NULL(raw);
    ASSERT_FALSE(raw->has_error || raw->parse_incomplete);
    kotlin_implicit_probe_t unary = probe_kotlin_implicit_site(
        raw, "negate", "unaryMinus", "app.UnaryBox.unaryMinus", "lsp_kt_operator");
    kotlin_implicit_probe_t contains = probe_kotlin_implicit_site(
        raw, "hasValue", "contains", "app.Bag.contains", "lsp_kt_operator");
    kotlin_implicit_probe_t not_contains = probe_kotlin_implicit_site(
        raw, "lacksValue", "contains", "app.Bag.contains", "lsp_kt_operator");
    kotlin_implicit_probe_t index =
        probe_kotlin_implicit_site(raw, "lookup", "get", "app.Slots.get", "lsp_kt_operator");
    kotlin_implicit_probe_t increment =
        probe_kotlin_implicit_site(raw, "increment", "inc", "app.Counter.inc", "lsp_kt_operator");
    kotlin_implicit_probe_t decrement =
        probe_kotlin_implicit_site(raw, "decrement", "dec", "app.Counter.dec", "lsp_kt_operator");
    kotlin_implicit_probe_t is_negative =
        probe_kotlin_implicit_site(raw, "isBag", "contains", "app.Bag.contains", "lsp_kt_operator");
    kotlin_implicit_probe_t assignment_negative =
        probe_kotlin_implicit_site(raw, "assign", "get", "app.Slots.get", "lsp_kt_operator");
    cbm_free_result(raw);

    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_par_kt_nonbinary_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("mkdtemp failed");
    }
    char definitions_path[512];
    char callers_path[512];
    snprintf(definitions_path, sizeof(definitions_path), "%s/app/Operators.kt", tmpdir);
    snprintf(callers_path, sizeof(callers_path), "%s/app/Main.kt", tmpdir);
    if (th_write_file(definitions_path, definitions_source) != 0 ||
        th_write_file(callers_path, callers_source) != 0) {
        th_rmtree(tmpdir);
        FAIL("failed to write Kotlin nonbinary-operator fixture");
    }
    cbm_file_info_t files[2] = {0};
    files[0].path = definitions_path;
    files[0].rel_path = (char *)"app/Operators.kt";
    files[0].language = CBM_LANG_KOTLIN;
    files[1].path = callers_path;
    files[1].rel_path = (char *)"app/Main.kt";
    files[1].language = CBM_LANG_KOTLIN;
    cbm_gbuf_t *sequential = run_sequential_with_lsp_cross(project, tmpdir, files, 2);
    cbm_gbuf_t *parallel = run_parallel(project, tmpdir, files, 2, 2);
    ASSERT_NOT_NULL(sequential);
    ASSERT_NOT_NULL(parallel);

    const bool sequential_unary =
        find_calls_edge_by_tails(sequential, "Main.negate", "UnaryBox.unaryMinus") != NULL;
    const bool parallel_unary =
        find_calls_edge_by_tails(parallel, "Main.negate", "UnaryBox.unaryMinus") != NULL;
    const bool sequential_contains =
        find_calls_edge_by_tails(sequential, "Main.hasValue", "Bag.contains") != NULL;
    const bool parallel_contains =
        find_calls_edge_by_tails(parallel, "Main.hasValue", "Bag.contains") != NULL;
    const bool sequential_not_contains =
        find_calls_edge_by_tails(sequential, "Main.lacksValue", "Bag.contains") != NULL;
    const bool parallel_not_contains =
        find_calls_edge_by_tails(parallel, "Main.lacksValue", "Bag.contains") != NULL;
    const bool sequential_index =
        find_calls_edge_by_tails(sequential, "Main.lookup", "Slots.get") != NULL;
    const bool parallel_index =
        find_calls_edge_by_tails(parallel, "Main.lookup", "Slots.get") != NULL;
    const bool sequential_increment =
        find_calls_edge_by_tails(sequential, "Main.increment", "Counter.inc") != NULL;
    const bool parallel_increment =
        find_calls_edge_by_tails(parallel, "Main.increment", "Counter.inc") != NULL;
    const bool sequential_decrement =
        find_calls_edge_by_tails(sequential, "Main.decrement", "Counter.dec") != NULL;
    const bool parallel_decrement =
        find_calls_edge_by_tails(parallel, "Main.decrement", "Counter.dec") != NULL;
    const bool sequential_false_contains =
        find_calls_edge_by_tails(sequential, "Main.isBag", "Bag.contains") != NULL;
    const bool parallel_false_contains =
        find_calls_edge_by_tails(parallel, "Main.isBag", "Bag.contains") != NULL;
    const bool sequential_false_get =
        find_calls_edge_by_tails(sequential, "Main.assign", "Slots.get") != NULL;
    const bool parallel_false_get =
        find_calls_edge_by_tails(parallel, "Main.assign", "Slots.get") != NULL;
    if (!unary.carrier || !unary.semantic || !unary.exact_join || !contains.carrier ||
        !contains.semantic || !contains.exact_join || !not_contains.carrier ||
        !not_contains.semantic || !not_contains.exact_join || !index.carrier || !index.semantic ||
        !index.exact_join || !increment.carrier || !increment.semantic || !increment.exact_join ||
        !decrement.carrier || !decrement.semantic || !decrement.exact_join || is_negative.carrier ||
        is_negative.semantic || assignment_negative.carrier || !sequential_unary ||
        !parallel_unary || !sequential_contains || !parallel_contains || !sequential_not_contains ||
        !parallel_not_contains || !sequential_index || !parallel_index || !sequential_increment ||
        !parallel_increment || !sequential_decrement || !parallel_decrement ||
        sequential_false_contains || parallel_false_contains || sequential_false_get ||
        parallel_false_get) {
        printf("  Kotlin nonbinary diagnostic: unary=%d/%d/%d/%d/%d "
               "contains=%d/%d/%d/%d/%d notin=%d/%d/%d/%d/%d "
               "index=%d/%d/%d/%d/%d inc=%d/%d/%d/%d/%d dec=%d/%d/%d/%d/%d "
               "negative=%d/%d/%d/%d/%d/%d\n",
               unary.carrier, unary.semantic, unary.exact_join, sequential_unary, parallel_unary,
               contains.carrier, contains.semantic, contains.exact_join, sequential_contains,
               parallel_contains, not_contains.carrier, not_contains.semantic,
               not_contains.exact_join, sequential_not_contains, parallel_not_contains,
               index.carrier, index.semantic, index.exact_join, sequential_index, parallel_index,
               increment.carrier, increment.semantic, increment.exact_join, sequential_increment,
               parallel_increment, decrement.carrier, decrement.semantic, decrement.exact_join,
               sequential_decrement, parallel_decrement, is_negative.carrier, is_negative.semantic,
               assignment_negative.carrier, sequential_false_contains, parallel_false_contains,
               sequential_false_get || parallel_false_get);
    }

    cbm_gbuf_free(sequential);
    cbm_gbuf_free(parallel);
    th_rmtree(tmpdir);
    ASSERT_TRUE(unary.carrier);
    ASSERT_TRUE(unary.semantic);
    ASSERT_TRUE(unary.exact_join);
    ASSERT_TRUE(contains.carrier);
    ASSERT_TRUE(contains.semantic);
    ASSERT_TRUE(contains.exact_join);
    ASSERT_TRUE(not_contains.carrier);
    ASSERT_TRUE(not_contains.semantic);
    ASSERT_TRUE(not_contains.exact_join);
    ASSERT_TRUE(index.carrier);
    ASSERT_TRUE(index.semantic);
    ASSERT_TRUE(index.exact_join);
    ASSERT_TRUE(increment.carrier);
    ASSERT_TRUE(increment.semantic);
    ASSERT_TRUE(increment.exact_join);
    ASSERT_TRUE(decrement.carrier);
    ASSERT_TRUE(decrement.semantic);
    ASSERT_TRUE(decrement.exact_join);
    ASSERT_FALSE(is_negative.carrier);
    ASSERT_FALSE(is_negative.semantic);
    ASSERT_FALSE(assignment_negative.carrier);
    ASSERT_TRUE(sequential_unary);
    ASSERT_TRUE(parallel_unary);
    ASSERT_TRUE(sequential_contains);
    ASSERT_TRUE(parallel_contains);
    ASSERT_TRUE(sequential_not_contains);
    ASSERT_TRUE(parallel_not_contains);
    ASSERT_TRUE(sequential_index);
    ASSERT_TRUE(parallel_index);
    ASSERT_TRUE(sequential_increment);
    ASSERT_TRUE(parallel_increment);
    ASSERT_TRUE(sequential_decrement);
    ASSERT_TRUE(parallel_decrement);
    ASSERT_FALSE(sequential_false_contains);
    ASSERT_FALSE(parallel_false_contains);
    ASSERT_FALSE(sequential_false_get);
    ASSERT_FALSE(parallel_false_get);
    PASS();
}

/* A known external macro semantic owns its parser occurrence even when the
 * external target is not materialized as a graph node.  It must not fall back
 * to a same-named local function in either pipeline. */
TEST(parallel_rust_known_macro_does_not_fallback_to_local_function) {
    static const char source[] = "fn println() {}\nfn run() { println!(\"macro\"); }\n";
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_par_rust_macro_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("mkdtemp failed");
    }
    char path[512];
    snprintf(path, sizeof(path), "%s/main.rs", tmpdir);
    FILE *file = cbm_fopen(path, "w");
    if (!file) {
        rmdir(tmpdir);
        FAIL("fopen main.rs failed");
    }
    fputs(source, file);
    fclose(file);

    CBMFileResult *extracted = cbm_extract_file(source, (int)strlen(source), CBM_LANG_RUST,
                                                "cbm_rust_macro", "main.rs", 0, NULL, NULL);
    ASSERT_NOT_NULL(extracted);
    const char *site = strstr(source, "println!(\"macro\")");
    const uint32_t start = site ? (uint32_t)(site - source) : 0;
    const uint32_t end = start + (uint32_t)strlen("println!(\"macro\")");
    bool semantic_owner = false;
    bool exact_gated_carrier = false;
    for (int i = 0; i < extracted->resolved_calls.count; i++) {
        const CBMResolvedCall *resolved = &extracted->resolved_calls.items[i];
        if (resolved->caller_qn && strstr(resolved->caller_qn, "run") && resolved->callee_qn &&
            strcmp(resolved->callee_qn, "std.macros.println") == 0 && resolved->strategy &&
            strcmp(resolved->strategy, "lsp_macro") == 0) {
            semantic_owner = true;
        }
    }
    for (int i = 0; i < extracted->calls.count; i++) {
        const CBMCall *call = &extracted->calls.items[i];
        if (call->enclosing_func_qn && strstr(call->enclosing_func_qn, "run") &&
            call->callee_name && strcmp(call->callee_name, "println") == 0 &&
            call->site_start_byte == start && call->site_end_byte == end &&
            call->requires_lsp_resolution) {
            exact_gated_carrier = true;
        }
    }
    cbm_free_result(extracted);

    cbm_file_info_t files[1] = {0};
    files[0].path = path;
    files[0].rel_path = (char *)"main.rs";
    files[0].language = CBM_LANG_RUST;
    cbm_gbuf_t *sequential = run_sequential("cbm_rust_macro", tmpdir, files, 1);
    cbm_gbuf_t *parallel = run_parallel("cbm_rust_macro", tmpdir, files, 1, 1);
    ASSERT_NOT_NULL(sequential);
    ASSERT_NOT_NULL(parallel);

    const bool sequential_nodes = find_unique_callable_node_by_tail(sequential, "main.run") &&
                                  find_unique_callable_node_by_tail(sequential, "main.println");
    const bool parallel_nodes = find_unique_callable_node_by_tail(parallel, "main.run") &&
                                find_unique_callable_node_by_tail(parallel, "main.println");
    const bool sequential_wrong =
        find_calls_edge_by_tails(sequential, "main.run", "main.println") != NULL;
    const bool parallel_wrong =
        find_calls_edge_by_tails(parallel, "main.run", "main.println") != NULL;
    if (!semantic_owner || !exact_gated_carrier || !sequential_nodes || !parallel_nodes ||
        sequential_wrong || parallel_wrong) {
        printf("  Rust macro fallback diagnostic: semantic=%d gated_carrier=%d seq_nodes=%d "
               "par_nodes=%d seq_wrong=%d par_wrong=%d\n",
               semantic_owner, exact_gated_carrier, sequential_nodes, parallel_nodes,
               sequential_wrong, parallel_wrong);
    }

    cbm_gbuf_free(sequential);
    cbm_gbuf_free(parallel);
    unlink(path);
    rmdir(tmpdir);
    ASSERT_TRUE(semantic_owner);
    ASSERT_TRUE(exact_gated_carrier);
    ASSERT_TRUE(sequential_nodes);
    ASSERT_TRUE(parallel_nodes);
    ASSERT_FALSE(sequential_wrong);
    ASSERT_FALSE(parallel_wrong);
    PASS();
}

/* Attribute proc-macros are source-level decorator references. Both pipelines
 * must represent them as DECORATES + USAGE and must not fabricate calls to
 * implementation details that are knowable only by executing the macro. */
TEST(parallel_rust_proc_macros_are_decorates_and_usage_only) {
    static const char source[] = "#[tokio::main]\nasync fn main() {}\n"
                                 "#[tracing::instrument]\nfn work() {}\n";
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_par_rust_proc_macro_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("mkdtemp failed");
    }
    char path[512];
    snprintf(path, sizeof(path), "%s/main.rs", tmpdir);
    FILE *file = cbm_fopen(path, "w");
    if (!file) {
        rmdir(tmpdir);
        FAIL("fopen main.rs failed");
    }
    fputs(source, file);
    fclose(file);

    cbm_file_info_t files[1] = {0};
    files[0].path = path;
    files[0].rel_path = (char *)"main.rs";
    files[0].language = CBM_LANG_RUST;
    cbm_gbuf_t *sequential = run_sequential("cbm_rust_proc_macro", tmpdir, files, 1);
    cbm_gbuf_t *parallel = run_parallel("cbm_rust_proc_macro", tmpdir, files, 1, 1);
    ASSERT_NOT_NULL(sequential);
    ASSERT_NOT_NULL(parallel);

    const bool seq_tokio_decorates = has_edge_from_callable_to_qn(
        sequential, "main.main", "<decorator:tokio::main>", "DECORATES");
    const bool seq_tokio_usage =
        has_edge_from_callable_to_qn(sequential, "main.main", "<decorator:tokio::main>", "USAGE");
    const bool par_tokio_decorates =
        has_edge_from_callable_to_qn(parallel, "main.main", "<decorator:tokio::main>", "DECORATES");
    const bool par_tokio_usage =
        has_edge_from_callable_to_qn(parallel, "main.main", "<decorator:tokio::main>", "USAGE");
    const bool seq_tracing_decorates = has_edge_from_callable_to_qn(
        sequential, "main.work", "<decorator:tracing::instrument>", "DECORATES");
    const bool seq_tracing_usage = has_edge_from_callable_to_qn(
        sequential, "main.work", "<decorator:tracing::instrument>", "USAGE");
    const bool par_tracing_decorates = has_edge_from_callable_to_qn(
        parallel, "main.work", "<decorator:tracing::instrument>", "DECORATES");
    const bool par_tracing_usage = has_edge_from_callable_to_qn(
        parallel, "main.work", "<decorator:tracing::instrument>", "USAGE");
    const bool seq_fake = callable_has_call_target_fragment(sequential, "main.main", "Runtime") ||
                          callable_has_call_target_fragment(sequential, "main.work", "Span.enter");
    const bool par_fake = callable_has_call_target_fragment(parallel, "main.main", "Runtime") ||
                          callable_has_call_target_fragment(parallel, "main.work", "Span.enter");

    if (!seq_tokio_decorates || !seq_tokio_usage || !par_tokio_decorates || !par_tokio_usage ||
        !seq_tracing_decorates || !seq_tracing_usage || !par_tracing_decorates ||
        !par_tracing_usage || seq_fake || par_fake) {
        printf("  Rust proc-macro policy diagnostic: tokio seq=%d/%d par=%d/%d "
               "tracing seq=%d/%d par=%d/%d fake=%d/%d\n",
               seq_tokio_decorates, seq_tokio_usage, par_tokio_decorates, par_tokio_usage,
               seq_tracing_decorates, seq_tracing_usage, par_tracing_decorates, par_tracing_usage,
               seq_fake, par_fake);
    }

    cbm_gbuf_free(sequential);
    cbm_gbuf_free(parallel);
    unlink(path);
    rmdir(tmpdir);
    ASSERT_TRUE(seq_tokio_decorates);
    ASSERT_TRUE(seq_tokio_usage);
    ASSERT_TRUE(par_tokio_decorates);
    ASSERT_TRUE(par_tokio_usage);
    ASSERT_TRUE(seq_tracing_decorates);
    ASSERT_TRUE(seq_tracing_usage);
    ASSERT_TRUE(par_tracing_decorates);
    ASSERT_TRUE(par_tracing_usage);
    ASSERT_FALSE(seq_fake);
    ASSERT_FALSE(par_fake);
    PASS();
}

static int assert_c_family_preprocessed_collision_graph(CBMLanguage language, const char *extension,
                                                        const char *project, const char *source,
                                                        const char *target_tail) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_par_c_origin_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("mkdtemp failed");
    }
    char rel_path[64];
    snprintf(rel_path, sizeof(rel_path), "main.%s", extension);
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", tmpdir, rel_path);
    FILE *file = cbm_fopen(path, "w");
    if (!file) {
        rmdir(tmpdir);
        FAIL("fopen C-family fixture failed");
    }
    fputs(source, file);
    fclose(file);

    cbm_file_info_t files[1] = {0};
    files[0].path = path;
    files[0].rel_path = rel_path;
    files[0].language = language;
    cbm_gbuf_t *sequential = run_sequential(project, tmpdir, files, 1);
    cbm_gbuf_t *parallel = run_parallel(project, tmpdir, files, 1, 1);
    ASSERT_NOT_NULL(sequential);
    ASSERT_NOT_NULL(parallel);

    const cbm_gbuf_edge_t *sequential_edge =
        find_calls_edge_by_tails(sequential, "main.occurrence_probe", target_tail);
    const cbm_gbuf_edge_t *parallel_edge =
        find_calls_edge_by_tails(parallel, "main.occurrence_probe", target_tail);
    if (!sequential_edge || !parallel_edge) {
        printf("  C-family origin diagnostic (%s): sequential=%s parallel=%s\n", extension,
               sequential_edge ? "present" : "absent", parallel_edge ? "present" : "absent");
    }
    ASSERT_NOT_NULL(sequential_edge);
    ASSERT_NOT_NULL(parallel_edge);

    cbm_gbuf_free(sequential);
    cbm_gbuf_free(parallel);
    unlink(path);
    rmdir(tmpdir);
    return 0;
}

TEST(parallel_c_preprocessed_coordinate_collision_preserves_hidden_target) {
    static const char source[] =
        "static int alpha_target(void){return 1;}\n"
        "static int bravo_target(void){return 2;}\n"
        "int occurrence_probe(void){\n"
        " int (*fp)(void) = alpha_target;\n"
        " /*xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx*/ fp ( );\n"
        "#define HIDDEN_FP_CALL() (fp = bravo_target, fp())\n"
        " return HIDDEN_FP_CALL();\n"
        "}\n";
    return assert_c_family_preprocessed_collision_graph(CBM_LANG_C, "c", "cbm_c_origin", source,
                                                        "main.bravo_target");
}

static int assert_cpp_like_preprocessed_collision_graph(CBMLanguage language, const char *extension,
                                                        const char *project) {
    static const char source[] =
        "struct Alpha { int render(){return 1;} };\n"
        "struct Bravo { int render(){return 2;} };\n"
        "int occurrence_probe(){\n"
        " Alpha alpha; Bravo bravo;\n"
        " /*xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx*/ alpha . render ( );\n"
        "#define HIDDEN_RENDER(x) x.render()\n"
        " return HIDDEN_RENDER(bravo);\n"
        "}\n";
    return assert_c_family_preprocessed_collision_graph(language, extension, project, source,
                                                        "Bravo.render");
}

TEST(parallel_cpp_preprocessed_coordinate_collision_preserves_hidden_target) {
    return assert_cpp_like_preprocessed_collision_graph(CBM_LANG_CPP, "cpp", "cbm_cpp_origin");
}

TEST(parallel_cuda_preprocessed_coordinate_collision_preserves_hidden_target) {
    return assert_cpp_like_preprocessed_collision_graph(CBM_LANG_CUDA, "cu", "cbm_cuda_origin");
}

TEST(parallel_java_kotlin_lsp_override_cross_file_emits_lsp_strategy_edges) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_par_jvm_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("mkdtemp failed");
    }

    char jpath[512];
    snprintf(jpath, sizeof(jpath), "%s/src/main/java/com/example/Example.java", tmpdir);
    char jdir[512];
    snprintf(jdir, sizeof(jdir), "%s/src/main/java/com/example", tmpdir);
    cbm_mkdir_p(jdir, 0755);
    FILE *jf = cbm_fopen(jpath, "w");
    if (!jf) {
        FAIL("fopen example.java failed");
    }
    fprintf(jf, "package com.example;\n"
                "\n"
                "class JavaCaller {\n"
                "    String call(KotlinService kotlinService) {\n"
                "        return kotlinService.ping(new JavaService());\n"
                "    }\n"
                "}\n"
                "\n"
                "class JavaService {\n"
                "    String pong() {\n"
                "        return \"pong\";\n"
                "    }\n"
                "}\n");
    fclose(jf);

    char kpath[512];
    snprintf(kpath, sizeof(kpath), "%s/src/main/kotlin/com/example/KotlinService.kt", tmpdir);
    char kdir[512];
    snprintf(kdir, sizeof(kdir), "%s/src/main/kotlin/com/example", tmpdir);
    cbm_mkdir_p(kdir, 0755);
    FILE *kf = cbm_fopen(kpath, "w");
    if (!kf) {
        unlink(jpath);
        rmdir(tmpdir);
        FAIL("fopen example.kt failed");
    }
    fprintf(kf, "package com.example\n"
                "\n"
                "class KotlinService {\n"
                "    fun ping(javaService: JavaService): String {\n"
                "        return javaService.pong()\n"
                "    }\n"
                "}\n");
    fclose(kf);

    cbm_file_info_t files[2] = {0};
    files[0].path = jpath;
    files[0].rel_path = (char *)"src/main/java/com/example/Example.java";
    files[0].language = CBM_LANG_JAVA;
    files[1].path = kpath;
    files[1].rel_path = (char *)"src/main/kotlin/com/example/KotlinService.kt";
    files[1].language = CBM_LANG_KOTLIN;

    cbm_gbuf_t *gbuf = run_parallel("com", tmpdir, files, 2, 2);
    ASSERT_NOT_NULL(gbuf);

    const cbm_gbuf_edge_t *java_to_kotlin =
        find_calls_edge_by_tails(gbuf, "JavaCaller.call", "KotlinService.ping");
    const cbm_gbuf_edge_t *kotlin_to_java =
        find_calls_edge_by_tails(gbuf, "KotlinService.ping", "JavaService.pong");

    ASSERT_NOT_NULL(java_to_kotlin);
    ASSERT_NOT_NULL(kotlin_to_java);
    ASSERT_NOT_NULL(java_to_kotlin->properties_json);
    ASSERT_NOT_NULL(kotlin_to_java->properties_json);
    ASSERT_NOT_NULL(strstr(java_to_kotlin->properties_json, "\"strategy\":\"lsp"));
    ASSERT_NOT_NULL(strstr(kotlin_to_java->properties_json, "\"strategy\":\"lsp"));
    ASSERT_TRUE(strstr(java_to_kotlin->properties_json, "\"strategy\":\"callee_suffix\"") == NULL);
    ASSERT_TRUE(strstr(kotlin_to_java->properties_json, "\"strategy\":\"callee_suffix\"") == NULL);

    cbm_gbuf_free(gbuf);
    unlink(kpath);
    unlink(jpath);
    rmdir(tmpdir);
    PASS();
}

/* Gate guard for the JVM-only unique-tail fallbacks (lsp_resolve.h).
 *
 * The tail fallbacks join LSP overrides across QN drift by unique
 * "Class.method" leaf. That is only sound where class-per-file package
 * semantics hold (Java/Kotlin); in any other language a single
 * wrong-module coincidence would fabricate a CALLS edge, so
 * cbm_pipeline_lsp_allow_tail_match must keep the fallbacks OFF there.
 *
 * NOTE: a natural end-to-end non-JVM coincidence fixture is impractical:
 * reaching the fallbacks requires the LSP and the textual extraction to
 * disagree on QN prefixes, which path-derived single-root languages do
 * not produce in a small fixture (that drift is precisely the JVM
 * mixed-source-root symptom the fallback exists for). So this test
 * exercises the gated branches directly: the SAME wrong-module
 * coincidence must resolve with the gate open (JVM) and must NOT with
 * the gate closed. If the gate were removed — fallbacks made
 * unconditional again — the gate-closed assertions below would fail. */
TEST(parallel_lsp_tail_match_fallbacks_gated_to_jvm) {
    /* Policy: exactly the JVM languages. */
    ASSERT_TRUE(cbm_pipeline_lsp_allow_tail_match(CBM_LANG_JAVA));
    ASSERT_TRUE(cbm_pipeline_lsp_allow_tail_match(CBM_LANG_KOTLIN));
    ASSERT_TRUE(!cbm_pipeline_lsp_allow_tail_match(CBM_LANG_PYTHON));
    ASSERT_TRUE(!cbm_pipeline_lsp_allow_tail_match(CBM_LANG_GO));
    ASSERT_TRUE(!cbm_pipeline_lsp_allow_tail_match(CBM_LANG_TYPESCRIPT));
    ASSERT_TRUE(!cbm_pipeline_lsp_allow_tail_match(CBM_LANG_CPP));

    /* Wrong-module coincidence: the resolved entry's caller shares only
     * the "Service.handle" tail with the textual call's enclosing
     * function, so the exact caller_qn pass misses and only the tail
     * fallback could join them. */
    CBMResolvedCall rc_item = {0};
    rc_item.caller_qn = "com.example.pkg.Service.handle";
    rc_item.callee_qn = "com.example.pkg.Helper.run";
    rc_item.strategy = "lsp";
    rc_item.confidence = 0.9f;
    CBMResolvedCallArray arr = {0};
    arr.items = &rc_item;
    arr.count = 1;
    arr.cap = 1;

    CBMCall call = {0};
    call.enclosing_func_qn = "proj.other_mod.Service.handle";
    call.callee_name = "helper.run";

    ASSERT_TRUE(cbm_pipeline_find_lsp_resolution(&arr, &call, false) == NULL);
    ASSERT_TRUE(cbm_pipeline_find_lsp_resolution(&arr, &call, true) == &rc_item);

    /* Kotlin top-level functions have one more legitimate QN drift: the graph
     * owner is file-shaped (`...Main.operatorCaller`) while the semantic owner
     * is package-shaped (`app.operatorCaller`). An exact source occurrence and
     * equal caller leaf may reconcile that JVM-only drift; the non-JVM policy
     * remains closed, a different caller leaf remains closed, and distinct
     * semantic targets at the same occurrence remain ambiguous. */
    CBMResolvedCall top_level_rows[2] = {0};
    top_level_rows[0].caller_qn = "app.operatorCaller";
    top_level_rows[0].callee_qn = "app.Box.plus";
    top_level_rows[0].strategy = "lsp_kt_operator";
    top_level_rows[0].confidence = 0.9f;
    top_level_rows[0].site_start_byte = 80;
    top_level_rows[0].site_end_byte = 92;
    top_level_rows[0].source_origin = CBM_SOURCE_ORIGIN_RAW;
    CBMResolvedCallArray top_level_arr = {0};
    top_level_arr.items = top_level_rows;
    top_level_arr.count = 1;
    top_level_arr.cap = 2;

    CBMCall top_level_call = {0};
    top_level_call.enclosing_func_qn = "proj.app.Main.operatorCaller";
    top_level_call.callee_name = "plus";
    top_level_call.site_start_byte = 80;
    top_level_call.site_end_byte = 92;
    top_level_call.source_origin = CBM_SOURCE_ORIGIN_RAW;
    top_level_call.requires_lsp_resolution = true;
    ASSERT_TRUE(cbm_pipeline_find_lsp_resolution(&top_level_arr, &top_level_call, false) == NULL);
    ASSERT_TRUE(cbm_pipeline_find_lsp_resolution(&top_level_arr, &top_level_call, true) ==
                &top_level_rows[0]);

    top_level_call.enclosing_func_qn = "proj.app.Main.differentCaller";
    ASSERT_TRUE(cbm_pipeline_find_lsp_resolution(&top_level_arr, &top_level_call, true) == NULL);
    top_level_call.enclosing_func_qn = "proj.app.Main.operatorCaller";
    top_level_rows[1] = top_level_rows[0];
    top_level_rows[1].callee_qn = "app.Other.plus";
    top_level_arr.count = 2;
    ASSERT_TRUE(cbm_pipeline_find_lsp_resolution(&top_level_arr, &top_level_call, true) == NULL);

    /* Target-node fallback: callee_qn misses both as-is and
     * project-prefixed; exactly one node coincidentally shares the
     * "Helper.run" tail in an unrelated module. */
    cbm_gbuf_t *tgbuf = cbm_gbuf_new("proj", "/tmp");
    ASSERT_NOT_NULL(tgbuf);
    int64_t nid = cbm_gbuf_upsert_node(tgbuf, "Method", "run", "proj.zeta.Helper.run",
                                       "zeta/helper.py", 1, 3, NULL);
    ASSERT_TRUE(nid != 0);
    ASSERT_TRUE(cbm_pipeline_lsp_target_node(tgbuf, "proj", "com.other.Helper.run", false) == NULL);
    const cbm_gbuf_node_t *jvm_hit =
        cbm_pipeline_lsp_target_node(tgbuf, "proj", "com.other.Helper.run", true);
    ASSERT_NOT_NULL(jvm_hit);
    ASSERT_TRUE(strcmp(jvm_hit->qualified_name, "proj.zeta.Helper.run") == 0);
    /* A synthetic external protocol target has no source-level evidence for
     * that Class.method-tail equivalence.  Its strict lookup must therefore
     * remain exact and fail closed on the same coincidence. */
    ASSERT_TRUE(cbm_pipeline_lsp_target_node_strict(tgbuf, "proj", "com.other.Helper.run", true) ==
                NULL);
    cbm_gbuf_free(tgbuf);
    PASS();
}

TEST(parallel_python_lsp_override_emits_lsp_strategy_edges) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_par_pylsp_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("mkdtemp failed");
    }

    /* Single-file scenario: pins the in-file LSP path where py_lsp
     * registers Greeter from the file's own defs, types `g = Greeter()`
     * as NAMED("…Greeter"), and resolves `g.hello()` to Greeter.hello
     * via attribute lookup. callee_qn matches the gbuf QN directly. The
     * cross-file equivalent is covered by
     * parallel_python_lsp_override_cross_file_emits_lsp_strategy_edges,
     * which exercises the project-prefix fallback in
     * cbm_pipeline_lsp_target_node. */
    char fpath0[512];
    snprintf(fpath0, sizeof(fpath0), "%s/app.py", tmpdir);
    FILE *f = fopen(fpath0, "w");
    if (!f) {
        FAIL("fopen app.py failed");
    }
    fprintf(f, "class Greeter:\n"
               "    def hello(self):\n"
               "        return 'hi'\n"
               "\n"
               "def main():\n"
               "    g = Greeter()\n"
               "    g.hello()\n");
    fclose(f);

    cbm_file_info_t files[1] = {0};
    files[0].path = fpath0;
    files[0].rel_path = (char *)"app.py";
    files[0].language = CBM_LANG_PYTHON;

    cbm_gbuf_t *gbuf = run_parallel("cbm_par_pylsp", tmpdir, files, 1, 1);
    ASSERT_NOT_NULL(gbuf);

    lsp_edge_count_ctx_t c = {0};
    cbm_gbuf_foreach_edge(gbuf, count_lsp_call_edges, &c);

    /* Sanity: extraction produced at least one call edge. */
    ASSERT_GT(c.total_calls, 0);
    /* The parallel pipeline must surface at least one LSP-attributed
     * CALLS edge. This proves the unified cbm_pipeline_find_lsp_resolution
     * (shared with pass_calls.c at floor 0.6) is actually consulted in
     * the parallel pipeline, and that the resulting edge is emitted with
     * the LSP strategy intact rather than overwritten by the registry
     * fallback. */
    ASSERT_GT(c.lsp_strategy_count, 0);

    cbm_gbuf_free(gbuf);

    unlink(fpath0);
    rmdir(tmpdir);
    PASS();
}

/* Cross-file regression for the QN-mismatch bug: py_lsp's per-file mode
 * emits resolved_calls.callee_qn as the raw import-module path (e.g.
 * `greeter.Greeter` from `from greeter import Greeter`) rather than the
 * project-qualified QN the gbuf stores (`<project>.greeter.Greeter`).
 * Before cbm_pipeline_lsp_target_node added the project-prefix fallback,
 * the LSP match succeeded (lsp_overrides counter incremented) but the
 * downstream cbm_gbuf_find_by_qn lookup missed silently, dropping the
 * edge. With the fallback in place, the cross-file `g.hello()` call is
 * attributed to <project>.greeter.Greeter.hello with an lsp_* strategy.
 *
 * Two-file scenario: greeter.py defines Greeter; app.py imports it and
 * calls hello() — same shape as the original failing reproduction. */
TEST(parallel_python_lsp_override_cross_file_emits_lsp_strategy_edges) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_par_pylsp_xf_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("mkdtemp failed");
    }

    char gpath[512];
    snprintf(gpath, sizeof(gpath), "%s/greeter.py", tmpdir);
    FILE *gf = fopen(gpath, "w");
    if (!gf) {
        FAIL("fopen greeter.py failed");
    }
    fprintf(gf, "class Greeter:\n"
                "    def hello(self):\n"
                "        return 'hi'\n");
    fclose(gf);

    char apath[512];
    snprintf(apath, sizeof(apath), "%s/app.py", tmpdir);
    FILE *af = fopen(apath, "w");
    if (!af) {
        unlink(gpath);
        rmdir(tmpdir);
        FAIL("fopen app.py failed");
    }
    fprintf(af, "from greeter import Greeter\n"
                "\n"
                "def main():\n"
                "    g = Greeter()\n"
                "    g.hello()\n");
    fclose(af);

    cbm_file_info_t files[2] = {0};
    files[0].path = gpath;
    files[0].rel_path = (char *)"greeter.py";
    files[0].language = CBM_LANG_PYTHON;
    files[1].path = apath;
    files[1].rel_path = (char *)"app.py";
    files[1].language = CBM_LANG_PYTHON;

    cbm_gbuf_t *gbuf = run_parallel("cbm_par_pylsp_xf", tmpdir, files, 2, 2);
    ASSERT_NOT_NULL(gbuf);

    lsp_edge_count_ctx_t c = {0};
    cbm_gbuf_foreach_edge(gbuf, count_lsp_call_edges, &c);

    ASSERT_GT(c.total_calls, 0);
    /* The cross-file LSP override must produce at least one lsp_*
     * CALLS edge. Without the project-prefix fallback in
     * cbm_pipeline_lsp_target_node this assertion would fail because the
     * raw module-path callee_qn doesn't match the project-qualified
     * gbuf node QN. */
    ASSERT_GT(c.lsp_strategy_count, 0);

    cbm_gbuf_free(gbuf);

    unlink(apath);
    unlink(gpath);
    rmdir(tmpdir);
    PASS();
}

/* RED/GREEN A — the graph-quality guarantee behind the low-RAM retention cap.
 *
 * The fused cross-file LSP step re-parses each file's source to resolve calls
 * whose receiver type lives in ANOTHER file (the per-file pass cannot). When
 * the retention cap drops a file's source, that resolution MUST still happen
 * via a bounded on-demand re-read; otherwise the cross-file CALLS edge is LOST.
 *
 * Fixture: a Java<->Kotlin pair with genuinely cross-language calls that only
 * the cross-file LSP resolves — JavaCaller.call -> KotlinService.ping (Java ->
 * Kotlin) and KotlinService.ping -> JavaService.pong (Kotlin -> Java). These
 * carry the "lsp" strategy and do NOT exist without the cross-file source,
 * unlike same-file or import-local Python calls which the per-file pass already
 * resolves (so counting lsp edges on those cannot detect the fallback).
 *
 * Three scenarios asserted GREEN with the re-read fallback in place:
 *   1. CONTROL   — default retention: both cross-file edges present. Proves the
 *                  fixture genuinely produces them (non-vacuity guard).
 *   2. NO-RETAIN — retain_sources=false: nothing retained -> edges survive only
 *                  via the re-read fallback.
 *   3. OVER-CAP  — per-file cap = 1 byte: every file dropped by the SIZE cap ->
 *                  edges survive only via the re-read fallback.
 * On main (no fallback) scenarios 2 and 3 LOSE both edges = RED; scenario 1
 * stays present = the non-vacuity control. */
TEST(parallel_cross_file_reread_preserves_unretained_edges) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_par_xf_reread_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("mkdtemp failed");
    }

    char jpath[512];
    snprintf(jpath, sizeof(jpath), "%s/src/main/java/com/example/Example.java", tmpdir);
    char jdir[512];
    snprintf(jdir, sizeof(jdir), "%s/src/main/java/com/example", tmpdir);
    cbm_mkdir_p(jdir, 0755);
    FILE *jf = fopen(jpath, "w");
    if (!jf) {
        FAIL("fopen Example.java failed");
    }
    fprintf(jf, "package com.example;\n"
                "\n"
                "class JavaCaller {\n"
                "    String call(KotlinService kotlinService) {\n"
                "        return kotlinService.ping(new JavaService());\n"
                "    }\n"
                "}\n"
                "\n"
                "class JavaService {\n"
                "    String pong() {\n"
                "        return \"pong\";\n"
                "    }\n"
                "}\n");
    fclose(jf);

    char kpath[512];
    snprintf(kpath, sizeof(kpath), "%s/src/main/kotlin/com/example/KotlinService.kt", tmpdir);
    char kdir[512];
    snprintf(kdir, sizeof(kdir), "%s/src/main/kotlin/com/example", tmpdir);
    cbm_mkdir_p(kdir, 0755);
    FILE *kf = fopen(kpath, "w");
    if (!kf) {
        FAIL("fopen KotlinService.kt failed");
    }
    fprintf(kf, "package com.example\n"
                "\n"
                "class KotlinService {\n"
                "    fun ping(javaService: JavaService): String {\n"
                "        return javaService.pong()\n"
                "    }\n"
                "}\n");
    fclose(kf);

    cbm_file_info_t files[2] = {0};
    files[0].path = jpath;
    files[0].rel_path = (char *)"src/main/java/com/example/Example.java";
    files[0].language = CBM_LANG_JAVA;
    files[1].path = kpath;
    files[1].rel_path = (char *)"src/main/kotlin/com/example/KotlinService.kt";
    files[1].language = CBM_LANG_KOTLIN;

    /* CONTROL (retained) + two drop scenarios that reach the cross-file edge
     * only via the on-demand re-read: NO-RETAIN disables retention entirely;
     * OVER-CAP sets a 1-byte per-file cap so every file is dropped by size. */
    const cbm_parallel_extract_opts_t no_retain = {
        .retain_sources = false,
        .retain_sources_set = true,
    };
    const cbm_parallel_extract_opts_t over_cap = {
        .retain_sources = true,
        .retain_sources_set = true,
        .retain_per_file_max_bytes = 1, /* 1 byte → every file dropped by the size cap */
    };
    const cbm_parallel_extract_opts_t *scenarios[3] = {NULL, &no_retain, &over_cap};

    for (int s = 0; s < 3; s++) {
        cbm_gbuf_t *gbuf = run_parallel_with_extract_opts("com", tmpdir, files, 2, 2, scenarios[s]);
        ASSERT_NOT_NULL(gbuf);

        const cbm_gbuf_edge_t *java_to_kotlin =
            find_calls_edge_by_tails(gbuf, "JavaCaller.call", "KotlinService.ping");
        const cbm_gbuf_edge_t *kotlin_to_java =
            find_calls_edge_by_tails(gbuf, "KotlinService.ping", "JavaService.pong");

        /* Both cross-file (Java↔Kotlin) CALLS edges must be present in EVERY
         * scenario. In the drop scenarios (s=1,2) the caller's source is NOT
         * retained, so these edges exist ONLY because resolve_worker re-reads
         * the source on demand. Without that fallback (main) they are LOST. */
        ASSERT_NOT_NULL(java_to_kotlin);
        ASSERT_NOT_NULL(kotlin_to_java);
        /* And they must come from the source-dependent cross-file LSP, not a
         * source-free suffix heuristic — proving the re-read actually ran. */
        ASSERT_NOT_NULL(java_to_kotlin->properties_json);
        ASSERT_NOT_NULL(strstr(java_to_kotlin->properties_json, "\"strategy\":\"lsp"));
        ASSERT_NOT_NULL(kotlin_to_java->properties_json);
        ASSERT_NOT_NULL(strstr(kotlin_to_java->properties_json, "\"strategy\":\"lsp"));

        cbm_gbuf_free(gbuf);
    }

    th_rmtree(tmpdir);
    PASS();
}

/* issue #294: gRPC service-name extraction must (a) preserve the canonical
 * proto service name (FooServiceClient → FooService, not Foo) and (b) only
 * match real stub/client types — ordinary receiver vars must NOT produce
 * phantom __grpc__ Routes. */
TEST(grpc_service_name_preserves_service_suffix_issue294) {
    char svc[256];
    char meth[256];

    /* Generated client class keeps the "Service" part of the name. */
    ASSERT_TRUE(extract_grpc_service_method("pb.NewFooServiceClient.GetBar", svc, sizeof(svc), meth,
                                            sizeof(meth)));
    ASSERT_STR_EQ(svc, "FooService");
    ASSERT_STR_EQ(meth, "GetBar");

    /* Java-style ...ServiceGrpc strips only "Grpc". */
    ASSERT_TRUE(extract_grpc_service_method("CartServiceGrpc.getCart", svc, sizeof(svc), meth,
                                            sizeof(meth)));
    ASSERT_STR_EQ(svc, "CartService");

    /* BlockingStub wins over Stub (longest-suffix-first). */
    ASSERT_TRUE(extract_grpc_service_method("CartServiceBlockingStub.getCart", svc, sizeof(svc),
                                            meth, sizeof(meth)));
    ASSERT_STR_EQ(svc, "CartService");
    PASS();
}

TEST(grpc_no_phantom_route_from_plain_var_issue294) {
    char svc[256];
    char meth[256];

    /* Ordinary receiver vars carry no gRPC stub suffix → must NOT match,
     * so no phantom __grpc__provider/... or __grpc__builder/... Route. */
    ASSERT_FALSE(
        extract_grpc_service_method("_provider.GetGroup", svc, sizeof(svc), meth, sizeof(meth)));
    ASSERT_FALSE(extract_grpc_service_method("_builder.AddSomeService", svc, sizeof(svc), meth,
                                             sizeof(meth)));
    ASSERT_FALSE(extract_grpc_service_method("logger.Info", svc, sizeof(svc), meth, sizeof(meth)));
    PASS();
}

/* ── Shared "::" normalization in cbm_pipeline_find_lsp_resolution (QA F3) ─
 *
 * The last-"::"-segment normalization in lsp_resolve.h widens matching for
 * qualified static callees (Perl `Pkg::sub`, C++ `Ns::fn`, etc.) across ALL
 * languages, not just Perl. These tests lock the intended behavior directly
 * against cbm_pipeline_find_lsp_resolution: (1) a qualified static call still
 * resolves to the right resolved entry, and (2) the theoretical
 * mis-attribution edge case (two same-named subs from different namespaces) is
 * bounded by caller-QN equality + the confidence floor. */
static CBMResolvedCall make_rc(const char *caller, const char *callee, float conf) {
    CBMResolvedCall rc = {0};
    memset(&rc, 0, sizeof(rc));
    rc.caller_qn = caller;
    rc.callee_qn = callee;
    rc.strategy = "test";
    rc.confidence = conf;
    return rc;
}

static CBMCall make_call(const char *enclosing, const char *callee_name) {
    CBMCall c;
    memset(&c, 0, sizeof(c));
    c.enclosing_func_qn = enclosing;
    c.callee_name = callee_name;
    return c;
}

TEST(lsp_bare_segment_skips_preprocessor_spacing) {
    /* simplecpp serializes macro-expanded member access with spaces around the
     * separator (for example `bravo . render`).  The shared semantic join must
     * still compare the identifier leaf, not the whitespace-prefixed suffix. */
    ASSERT_STR_EQ(cbm_lsp_bare_segment("bravo . render"), "render");
    ASSERT_STR_EQ(cbm_lsp_bare_segment("ptr -> run"), "run");
    PASS();
}

TEST(lsp_resolve_qualified_static_call_normalizes_colons) {
    /* A qualified static call `Pkg::sub` (callee_name keeps the package
     * prefix) must still match a resolved entry whose callee_qn short-name is
     * the bare `sub`. This is the cross-language "::"-normalization contract. */
    CBMResolvedCall items[] = {
        make_rc("proj.mod.caller", "proj.Pkg.sub", 0.9f),
    };
    CBMResolvedCallArray arr = {items, 1, 1};
    CBMCall call = make_call("proj.mod.caller", "Pkg::sub");
    const CBMResolvedCall *hit = cbm_pipeline_find_lsp_resolution(&arr, &call, false);
    ASSERT(hit != NULL);
    ASSERT(strcmp(hit->callee_qn, "proj.Pkg.sub") == 0);

    /* A bare call (no "::") to the same short name resolves identically —
     * normalization must not regress the common case. */
    CBMCall bare = make_call("proj.mod.caller", "sub");
    const CBMResolvedCall *bare_hit = cbm_pipeline_find_lsp_resolution(&arr, &bare, false);
    ASSERT(bare_hit != NULL);
    ASSERT(strcmp(bare_hit->callee_qn, "proj.Pkg.sub") == 0);
    PASS();
}

TEST(lsp_resolve_distinct_exact_caller_targets_fail_closed) {
    /* Two same-named targets from different namespaces at the same caller and
     * legacy occurrence are semantic ambiguity, not a confidence contest.
     * Choosing the higher score would fabricate one exact CALLS target. */
    CBMResolvedCall items[] = {
        make_rc("proj.mod.caller", "proj.A.foo", 0.7f),
        make_rc("proj.mod.caller", "proj.B.foo", 0.9f),
        /* Below the confidence floor: must be ignored entirely. */
        make_rc("proj.mod.caller", "proj.C.foo", 0.3f),
        /* Different caller: must never match regardless of short-name. */
        make_rc("proj.mod.other", "proj.D.foo", 0.95f),
    };
    CBMResolvedCallArray arr = {items, 4, 4};
    CBMCall call = make_call("proj.mod.caller", "B::foo");
    const CBMResolvedCall *hit = cbm_pipeline_find_lsp_resolution(&arr, &call, false);
    ASSERT_NULL(hit);

    /* The cross-caller high-confidence entry only matches its own caller. */
    CBMCall other = make_call("proj.mod.other", "D::foo");
    const CBMResolvedCall *other_hit = cbm_pipeline_find_lsp_resolution(&arr, &other, false);
    ASSERT(other_hit != NULL);
    ASSERT(strcmp(other_hit->callee_qn, "proj.D.foo") == 0);

    /* A caller with no qualifying entry resolves to nothing (no widening can
     * manufacture an edge across callers). */
    CBMCall absent = make_call("proj.mod.absent", "foo");
    ASSERT(cbm_pipeline_find_lsp_resolution(&arr, &absent, false) == NULL);
    PASS();
}

TEST(lsp_resolve_duplicate_exact_caller_rows_same_target_are_not_ambiguous) {
    CBMCall call = make_call("proj.mod.caller", "receiver.render");
    call.site_start_byte = 75;
    call.site_end_byte = 91;
    CBMResolvedCall low = make_rc("proj.mod.caller", "proj.Alpha.render", 0.75f);
    low.site_start_byte = call.site_start_byte;
    low.site_end_byte = call.site_end_byte;
    CBMResolvedCall high = low;
    high.confidence = 0.95f;
    CBMResolvedCall items[] = {low, high};
    CBMResolvedCallArray arr = {items, 2, 2};

    ASSERT(cbm_pipeline_find_lsp_resolution(&arr, &call, false) == &items[1]);
    PASS();
}

TEST(lsp_resolve_distinct_exact_caller_site_targets_are_ambiguous) {
    CBMCall call = make_call("proj.mod.caller", "receiver.render");
    call.site_start_byte = 92;
    call.site_end_byte = 108;
    call.requires_lsp_resolution = true;
    CBMResolvedCall alpha = make_rc("proj.mod.caller", "proj.Alpha.render", 0.75f);
    alpha.site_start_byte = call.site_start_byte;
    alpha.site_end_byte = call.site_end_byte;
    CBMResolvedCall beta = make_rc("proj.mod.caller", "proj.Beta.render", 0.99f);
    beta.site_start_byte = call.site_start_byte;
    beta.site_end_byte = call.site_end_byte;
    CBMResolvedCall items[] = {alpha, beta};
    CBMResolvedCallArray arr = {items, 2, 2};

    ASSERT_NULL(cbm_pipeline_find_lsp_resolution(&arr, &call, false));
    PASS();
}

/* Per-file and project-wide passes may spell one exact non-JVM invocation
 * target with and without the project prefix. Graph identity, not raw string
 * identity, decides whether those occurrence-identical rows conflict. */
TEST(lsp_resolve_project_prefixed_duplicate_is_not_ambiguous) {
    const char *project = "proj";
    cbm_gbuf_t *gbuf = cbm_gbuf_new(project, "/tmp");
    ASSERT_NOT_NULL(gbuf);
    int64_t target_id = cbm_gbuf_upsert_node(gbuf, "Function", "handler",
                                             "proj.mod.Target.handler", "target.py", 1, 1, "{}");
    ASSERT_GT(target_id, 0);

    CBMCall call = make_call("proj.mod.Caller.run", "handler");
    call.site_start_byte = 110;
    call.site_end_byte = 119;
    CBMResolvedCall raw = make_rc("proj.mod.Caller.run", "mod.Target.handler", 0.75f);
    raw.site_start_byte = call.site_start_byte;
    raw.site_end_byte = call.site_end_byte;
    CBMResolvedCall prefixed =
        make_rc("proj.mod.Caller.run", "proj.mod.Target.handler", 0.90f);
    prefixed.site_start_byte = call.site_start_byte;
    prefixed.site_end_byte = call.site_end_byte;
    CBMResolvedCall items[] = {raw, prefixed};
    CBMResolvedCallArray arr = {items, 2, 2};

    const CBMResolvedCall *hit =
        cbm_pipeline_find_lsp_resolution_in_graph(&arr, &call, false, gbuf, project);
    cbm_gbuf_free(gbuf);
    ASSERT(hit == &items[1]);
    PASS();
}

TEST(lsp_resolve_project_prefix_spellings_stay_ambiguous_when_both_nodes_exist) {
    const char *project = "proj";
    cbm_gbuf_t *gbuf = cbm_gbuf_new(project, "/tmp");
    ASSERT_NOT_NULL(gbuf);
    int64_t raw_id = cbm_gbuf_upsert_node(gbuf, "Function", "handler", "mod.Target.handler",
                                          "raw.py", 1, 1, "{}");
    int64_t prefixed_id = cbm_gbuf_upsert_node(gbuf, "Function", "handler",
                                               "proj.mod.Target.handler", "prefixed.py", 1, 1,
                                               "{}");
    ASSERT_GT(raw_id, 0);
    ASSERT_GT(prefixed_id, 0);
    ASSERT(raw_id != prefixed_id);

    CBMCall call = make_call("proj.mod.Caller.run", "handler");
    call.site_start_byte = 120;
    call.site_end_byte = 129;
    CBMResolvedCall raw = make_rc("proj.mod.Caller.run", "mod.Target.handler", 0.75f);
    raw.site_start_byte = call.site_start_byte;
    raw.site_end_byte = call.site_end_byte;
    CBMResolvedCall prefixed =
        make_rc("proj.mod.Caller.run", "proj.mod.Target.handler", 0.90f);
    prefixed.site_start_byte = call.site_start_byte;
    prefixed.site_end_byte = call.site_end_byte;
    CBMResolvedCall items[] = {raw, prefixed};
    CBMResolvedCallArray arr = {items, 2, 2};

    const CBMResolvedCall *hit =
        cbm_pipeline_find_lsp_resolution_in_graph(&arr, &call, false, gbuf, project);
    cbm_gbuf_free(gbuf);
    ASSERT_NULL(hit);
    PASS();
}

TEST(lsp_resolve_jvm_package_alias_of_materialized_target_is_not_ambiguous) {
    const char *project = "proj";
    cbm_gbuf_t *gbuf = cbm_gbuf_new(project, "/tmp");
    ASSERT_NOT_NULL(gbuf);
    int64_t target_id =
        cbm_gbuf_upsert_node(gbuf, "Method", "twice", "proj.Util.twice", "Util.java", 1, 1, "{}");
    ASSERT_GT(target_id, 0);

    CBMCall call = make_call("proj.Client.run", "twice");
    call.site_start_byte = 120;
    call.site_end_byte = 128;
    CBMResolvedCall local = make_rc("proj.Client.run", "proj.Util.twice", 0.92f);
    local.site_start_byte = call.site_start_byte;
    local.site_end_byte = call.site_end_byte;
    CBMResolvedCall package = make_rc("proj.Client.run", "demo.Util.twice", 0.92f);
    package.site_start_byte = call.site_start_byte;
    package.site_end_byte = call.site_end_byte;
    CBMResolvedCall items[] = {local, package};
    CBMResolvedCallArray arr = {items, 2, 2};

    const CBMResolvedCall *hit =
        cbm_pipeline_find_lsp_resolution_in_graph(&arr, &call, true, gbuf, project);
    cbm_gbuf_free(gbuf);
    ASSERT_NOT_NULL(hit);
    PASS();
}

TEST(lsp_resolve_jvm_two_tail_only_packages_stay_ambiguous) {
    const char *project = "proj";
    cbm_gbuf_t *gbuf = cbm_gbuf_new(project, "/tmp");
    ASSERT_NOT_NULL(gbuf);
    int64_t target_id = cbm_gbuf_upsert_node(gbuf, "Method", "run", "proj.generated.Service.run",
                                             "Service.java", 1, 1, "{}");
    ASSERT_GT(target_id, 0);

    CBMCall call = make_call("proj.Client.call", "run");
    call.site_start_byte = 130;
    call.site_end_byte = 138;
    CBMResolvedCall first = make_rc("proj.Client.call", "pkg1.Service.run", 0.80f);
    first.site_start_byte = call.site_start_byte;
    first.site_end_byte = call.site_end_byte;
    CBMResolvedCall second = make_rc("proj.Client.call", "pkg2.Service.run", 0.90f);
    second.site_start_byte = call.site_start_byte;
    second.site_end_byte = call.site_end_byte;
    CBMResolvedCall items[] = {first, second};
    CBMResolvedCallArray arr = {items, 2, 2};

    const CBMResolvedCall *hit =
        cbm_pipeline_find_lsp_resolution_in_graph(&arr, &call, true, gbuf, project);
    cbm_gbuf_free(gbuf);
    ASSERT_NULL(hit);
    PASS();
}

TEST(lsp_resolve_exact_site_beats_higher_confidence_legacy_record) {
    CBMCall call = make_call("proj.mod.caller", "receiver.render");
    call.site_start_byte = 100;
    call.site_end_byte = 117;

    CBMResolvedCall exact = make_rc("proj.mod.caller", "proj.Alpha.render", 0.80f);
    exact.site_start_byte = call.site_start_byte;
    exact.site_end_byte = call.site_end_byte;

    /* A legacy resolver record has no occurrence span. Even with greater
     * confidence it must not override the semantic record for this exact
     * carrier occurrence. */
    CBMResolvedCall legacy = make_rc("proj.mod.caller", "proj.Beta.render", 0.95f);
    CBMResolvedCall items[] = {exact, legacy};
    CBMResolvedCallArray arr = {items, 2, 2};

    const CBMResolvedCall *hit = cbm_pipeline_find_lsp_resolution(&arr, &call, false);
    ASSERT(hit == &items[0]);
    ASSERT(strcmp(hit->callee_qn, "proj.Alpha.render") == 0);
    PASS();
}

TEST(lsp_resolve_exact_site_beats_equal_confidence_legacy_record) {
    CBMCall call = make_call("proj.mod.caller", "receiver.render");
    call.site_start_byte = 200;
    call.site_end_byte = 217;

    /* Put the legacy record first to prove equal-confidence selection is not
     * an array-order accident: occurrence exactness is the primary key. */
    CBMResolvedCall legacy = make_rc("proj.mod.caller", "proj.Beta.render", 0.90f);
    CBMResolvedCall exact = make_rc("proj.mod.caller", "proj.Alpha.render", 0.90f);
    exact.site_start_byte = call.site_start_byte;
    exact.site_end_byte = call.site_end_byte;
    CBMResolvedCall items[] = {legacy, exact};
    CBMResolvedCallArray arr = {items, 2, 2};

    const CBMResolvedCall *hit = cbm_pipeline_find_lsp_resolution(&arr, &call, false);
    ASSERT(hit == &items[1]);
    ASSERT(strcmp(hit->callee_qn, "proj.Alpha.render") == 0);
    PASS();
}

TEST(lsp_resolve_malformed_span_is_not_legacy_compatible) {
    CBMCall call = make_call("proj.mod.caller", "receiver.render");
    call.site_start_byte = 300;
    call.site_end_byte = 317;

    /* A reversed/partial span is corrupt occurrence metadata, not a legacy
     * 0:0 record. It must be rejected even when it has greater confidence. */
    CBMResolvedCall malformed = make_rc("proj.mod.caller", "proj.Wrong.render", 0.99f);
    malformed.site_start_byte = 317;
    malformed.site_end_byte = 300;
    CBMResolvedCall legacy = make_rc("proj.mod.caller", "proj.Legacy.render", 0.75f);
    CBMResolvedCall items[] = {malformed, legacy};
    CBMResolvedCallArray arr = {items, 2, 2};

    const CBMResolvedCall *hit = cbm_pipeline_find_lsp_resolution(&arr, &call, false);
    ASSERT(hit == &items[1]);
    PASS();
}

TEST(lsp_resolve_below_floor_exact_does_not_block_legacy) {
    CBMCall call = make_call("proj.mod.caller", "receiver.render");
    call.site_start_byte = 400;
    call.site_end_byte = 417;

    CBMResolvedCall below_floor = make_rc("proj.mod.caller", "proj.Exact.render", 0.55f);
    below_floor.site_start_byte = call.site_start_byte;
    below_floor.site_end_byte = call.site_end_byte;
    CBMResolvedCall legacy = make_rc("proj.mod.caller", "proj.Legacy.render", 0.70f);
    CBMResolvedCall items[] = {below_floor, legacy};
    CBMResolvedCallArray arr = {items, 2, 2};

    ASSERT(cbm_pipeline_find_lsp_resolution(&arr, &call, false) == &items[1]);
    PASS();
}

static CBMCall make_tail_call(uint32_t start, uint32_t end) {
    CBMCall call = make_call("proj.raw.Service.handle", "helper.run");
    call.site_start_byte = start;
    call.site_end_byte = end;
    return call;
}

static CBMResolvedCall make_tail_rc(const char *callee, float confidence, uint32_t start,
                                    uint32_t end) {
    CBMResolvedCall resolved = make_rc("com.example.generated.Service.handle", callee, confidence);
    resolved.site_start_byte = start;
    resolved.site_end_byte = end;
    return resolved;
}

TEST(lsp_tail_exact_site_beats_legacy_regardless_of_order) {
    CBMCall call = make_tail_call(500, 512);
    CBMResolvedCall exact = make_tail_rc("com.example.Alpha.run", 0.80f, 500, 512);
    CBMResolvedCall legacy = make_tail_rc("com.example.Beta.run", 0.99f, 0, 0);
    CBMResolvedCall exact_first[] = {exact, legacy};
    CBMResolvedCall legacy_first[] = {legacy, exact};
    CBMResolvedCallArray first = {exact_first, 2, 2};
    CBMResolvedCallArray second = {legacy_first, 2, 2};

    ASSERT(cbm_pipeline_find_lsp_resolution(&first, &call, true) == &exact_first[0]);
    ASSERT(cbm_pipeline_find_lsp_resolution(&second, &call, true) == &legacy_first[1]);
    PASS();
}

TEST(lsp_tail_duplicate_exact_rows_same_target_are_not_ambiguous) {
    CBMCall call = make_tail_call(600, 612);
    CBMResolvedCall low = make_tail_rc("com.example.Helper.run", 0.75f, 600, 612);
    CBMResolvedCall high = make_tail_rc("com.example.Helper.run", 0.90f, 600, 612);
    CBMResolvedCall items[] = {low, high};
    CBMResolvedCallArray arr = {items, 2, 2};

    ASSERT(cbm_pipeline_find_lsp_resolution(&arr, &call, true) == &items[1]);
    PASS();
}

TEST(lsp_tail_distinct_exact_targets_are_ambiguous) {
    CBMCall call = make_tail_call(700, 712);
    CBMResolvedCall alpha = make_tail_rc("com.example.Alpha.run", 0.75f, 700, 712);
    CBMResolvedCall beta = make_tail_rc("com.example.Beta.run", 0.99f, 700, 712);
    CBMResolvedCall items[] = {alpha, beta};
    CBMResolvedCallArray arr = {items, 2, 2};

    ASSERT_NULL(cbm_pipeline_find_lsp_resolution(&arr, &call, true));
    PASS();
}

TEST(lsp_tail_duplicate_legacy_rows_same_target_are_not_ambiguous) {
    CBMCall call = make_tail_call(800, 812);
    CBMResolvedCall low = make_tail_rc("com.example.Helper.run", 0.75f, 0, 0);
    CBMResolvedCall high = make_tail_rc("com.example.Helper.run", 0.90f, 0, 0);
    CBMResolvedCall items[] = {low, high};
    CBMResolvedCallArray arr = {items, 2, 2};

    ASSERT(cbm_pipeline_find_lsp_resolution(&arr, &call, true) == &items[1]);
    PASS();
}

TEST(lsp_tail_distinct_legacy_targets_are_ambiguous) {
    CBMCall call = make_tail_call(900, 912);
    CBMResolvedCall alpha = make_tail_rc("com.example.Alpha.run", 0.75f, 0, 0);
    CBMResolvedCall beta = make_tail_rc("com.example.Beta.run", 0.99f, 0, 0);
    CBMResolvedCall items[] = {alpha, beta};
    CBMResolvedCallArray arr = {items, 2, 2};

    ASSERT_NULL(cbm_pipeline_find_lsp_resolution(&arr, &call, true));
    PASS();
}

static CBMUsage make_reference_usage(const char *caller, const char *name, uint32_t start,
                                     uint32_t end) {
    CBMUsage usage = {0};
    usage.enclosing_func_qn = caller;
    usage.ref_name = name;
    usage.kind = CBM_USAGE_CALL_REFERENCE;
    usage.site_start_byte = start;
    usage.site_end_byte = end;
    return usage;
}

static CBMResolvedCall make_reference_rc(const char *caller, const char *callee, float confidence,
                                         uint32_t start, uint32_t end) {
    CBMResolvedCall resolved = make_rc(caller, callee, confidence);
    resolved.kind = CBM_RESOLVED_CALL_REFERENCE;
    resolved.site_start_byte = start;
    resolved.site_end_byte = end;
    return resolved;
}

TEST(lsp_reference_duplicate_exact_rows_same_target_are_not_ambiguous) {
    CBMUsage usage = make_reference_usage("proj.mod.Caller.run", "handler", 1000, 1007);
    CBMResolvedCall low =
        make_reference_rc("proj.mod.Caller.run", "proj.Target.handler", 0.75f, 1000, 1007);
    CBMResolvedCall high =
        make_reference_rc("proj.mod.Caller.run", "proj.Target.handler", 0.90f, 1000, 1007);
    CBMResolvedCall items[] = {low, high};
    CBMResolvedCallArray arr = {items, 2, 2};

    ASSERT(cbm_pipeline_find_lsp_reference(&arr, &usage, false) == &items[1]);
    PASS();
}

/* Per-file LSP rows may carry a package-shaped target while a cross-file row
 * carries the same graph QN with the project prefix already attached.  Those
 * spellings identify one target and must not trip the distinct-target
 * ambiguity guard. */
TEST(lsp_reference_project_prefixed_duplicate_is_not_ambiguous) {
    const char *project = "proj";
    cbm_gbuf_t *gbuf = cbm_gbuf_new(project, "/tmp");
    ASSERT_NOT_NULL(gbuf);
    int64_t target_id = cbm_gbuf_upsert_node(gbuf, "Function", "handler", "proj.mod.Target.handler",
                                             "target.go", 1, 1, "{}");
    ASSERT_GT(target_id, 0);
    const cbm_gbuf_node_t *raw_target =
        cbm_pipeline_lsp_target_node(gbuf, project, "mod.Target.handler", false);
    const cbm_gbuf_node_t *prefixed_target =
        cbm_pipeline_lsp_target_node(gbuf, project, "proj.mod.Target.handler", false);
    ASSERT_NOT_NULL(raw_target);
    ASSERT_NOT_NULL(prefixed_target);
    ASSERT_EQ(raw_target->id, prefixed_target->id);

    CBMUsage usage = make_reference_usage("proj.mod.Caller.run", "handler", 1050, 1057);
    CBMResolvedCall raw =
        make_reference_rc("proj.mod.Caller.run", "mod.Target.handler", 0.75f, 1050, 1057);
    CBMResolvedCall prefixed =
        make_reference_rc("proj.mod.Caller.run", "proj.mod.Target.handler", 0.90f, 1050, 1057);
    CBMResolvedCall items[] = {raw, prefixed};
    CBMResolvedCallArray arr = {items, 2, 2};

    const CBMResolvedCall *hit =
        cbm_pipeline_find_lsp_reference_in_graph(&arr, &usage, false, gbuf, project);
    cbm_gbuf_free(gbuf);
    ASSERT(hit == &items[1]);
    PASS();
}

TEST(lsp_reference_project_prefix_spellings_stay_ambiguous_when_both_nodes_exist) {
    const char *project = "proj";
    cbm_gbuf_t *gbuf = cbm_gbuf_new(project, "/tmp");
    ASSERT_NOT_NULL(gbuf);
    int64_t raw_id = cbm_gbuf_upsert_node(gbuf, "Function", "handler", "mod.Target.handler",
                                          "raw.go", 1, 1, "{}");
    int64_t prefixed_id = cbm_gbuf_upsert_node(
        gbuf, "Function", "handler", "proj.mod.Target.handler", "prefixed.go", 1, 1, "{}");
    ASSERT_GT(raw_id, 0);
    ASSERT_GT(prefixed_id, 0);
    ASSERT(raw_id != prefixed_id);

    CBMUsage usage = make_reference_usage("proj.mod.Caller.run", "handler", 1070, 1077);
    CBMResolvedCall raw =
        make_reference_rc("proj.mod.Caller.run", "mod.Target.handler", 0.75f, 1070, 1077);
    CBMResolvedCall prefixed =
        make_reference_rc("proj.mod.Caller.run", "proj.mod.Target.handler", 0.90f, 1070, 1077);
    CBMResolvedCall items[] = {raw, prefixed};
    CBMResolvedCallArray arr = {items, 2, 2};

    const CBMResolvedCall *hit =
        cbm_pipeline_find_lsp_reference_in_graph(&arr, &usage, false, gbuf, project);
    cbm_gbuf_free(gbuf);
    ASSERT_NULL(hit);
    PASS();
}

/* A JVM tail lookup may recover one materialized graph node when package- and
 * path-shaped QNs drift, but it must not erase semantic disagreement between
 * two resolver rows. `pkg1.Service.handler` and `pkg2.Service.handler` are
 * distinct source targets even when each independently falls back to the same
 * unique `Service.handler` graph node. Collapsing them would let row order or
 * confidence fabricate an exact CALL_REFERENCE instead of failing closed. */
TEST(lsp_reference_jvm_distinct_packages_do_not_collapse_via_tail_node) {
    const char *project = "proj";
    cbm_gbuf_t *gbuf = cbm_gbuf_new(project, "/tmp");
    ASSERT_NOT_NULL(gbuf);
    int64_t materialized_id = cbm_gbuf_upsert_node(
        gbuf, "Method", "handler", "proj.generated.Service.handler", "Service.kt", 1, 1, "{}");
    ASSERT_GT(materialized_id, 0);

    CBMUsage usage = make_reference_usage("proj.consumer.Caller.run", "handler", 1080, 1087);
    CBMResolvedCall pkg1 =
        make_reference_rc("proj.consumer.Caller.run", "pkg1.Service.handler", 0.75f, 1080, 1087);
    CBMResolvedCall pkg2 =
        make_reference_rc("proj.consumer.Caller.run", "pkg2.Service.handler", 0.90f, 1080, 1087);
    CBMResolvedCall pkg1_first[] = {pkg1, pkg2};
    CBMResolvedCall pkg2_first[] = {pkg2, pkg1};
    CBMResolvedCallArray first = {pkg1_first, 2, 2};
    CBMResolvedCallArray second = {pkg2_first, 2, 2};

    const cbm_gbuf_node_t *pkg1_tail =
        cbm_pipeline_lsp_target_node(gbuf, project, pkg1.callee_qn, true);
    const cbm_gbuf_node_t *pkg2_tail =
        cbm_pipeline_lsp_target_node(gbuf, project, pkg2.callee_qn, true);
    bool same_tail_node = pkg1_tail && pkg2_tail && pkg1_tail->id == materialized_id &&
                          pkg2_tail->id == materialized_id;
    const CBMResolvedCall *first_hit =
        cbm_pipeline_find_lsp_reference_in_graph(&first, &usage, true, gbuf, project);
    const CBMResolvedCall *second_hit =
        cbm_pipeline_find_lsp_reference_in_graph(&second, &usage, true, gbuf, project);
    cbm_gbuf_free(gbuf);

    ASSERT_TRUE(same_tail_node);
    ASSERT_NULL(first_hit);
    ASSERT_NULL(second_hit);
    PASS();
}

TEST(lsp_reference_distinct_exact_targets_are_order_invariant) {
    CBMUsage usage = make_reference_usage("proj.mod.Caller.run", "handler", 1100, 1107);
    CBMResolvedCall alpha =
        make_reference_rc("proj.mod.Caller.run", "proj.Alpha.handler", 0.90f, 1100, 1107);
    CBMResolvedCall beta =
        make_reference_rc("proj.mod.Caller.run", "proj.Beta.handler", 0.90f, 1100, 1107);
    CBMResolvedCall alpha_first[] = {alpha, beta};
    CBMResolvedCall beta_first[] = {beta, alpha};
    CBMResolvedCallArray first = {alpha_first, 2, 2};
    CBMResolvedCallArray second = {beta_first, 2, 2};

    const CBMResolvedCall *first_hit = cbm_pipeline_find_lsp_reference(&first, &usage, false);
    const CBMResolvedCall *second_hit = cbm_pipeline_find_lsp_reference(&second, &usage, false);
    bool invariant = (!first_hit && !second_hit) ||
                     (first_hit && second_hit && first_hit->callee_qn && second_hit->callee_qn &&
                      strcmp(first_hit->callee_qn, second_hit->callee_qn) == 0);
    ASSERT_TRUE(invariant);
    PASS();
}

TEST(lsp_reference_duplicate_tail_rows_same_target_are_not_ambiguous) {
    CBMUsage usage = make_reference_usage("proj.raw.Service.handle", "handler", 1200, 1207);
    CBMResolvedCall low =
        make_reference_rc("com.generated.Service.handle", "com.Target.handler", 0.75f, 1200, 1207);
    CBMResolvedCall high =
        make_reference_rc("com.generated.Service.handle", "com.Target.handler", 0.90f, 1200, 1207);
    CBMResolvedCall items[] = {low, high};
    CBMResolvedCallArray arr = {items, 2, 2};

    ASSERT(cbm_pipeline_find_lsp_reference(&arr, &usage, true) == &items[1]);
    PASS();
}

TEST(lsp_reference_distinct_tail_targets_are_ambiguous) {
    CBMUsage usage = make_reference_usage("proj.raw.Service.handle", "handler", 1300, 1307);
    CBMResolvedCall alpha =
        make_reference_rc("com.generated.Service.handle", "com.Alpha.handler", 0.75f, 1300, 1307);
    CBMResolvedCall beta =
        make_reference_rc("com.generated.Service.handle", "com.Beta.handler", 0.99f, 1300, 1307);
    CBMResolvedCall items[] = {alpha, beta};
    CBMResolvedCallArray arr = {items, 2, 2};

    ASSERT_NULL(cbm_pipeline_find_lsp_reference(&arr, &usage, true));
    PASS();
}

TEST(lsp_target_node_supports_long_prefixed_qualified_name) {
    const char *project = "long_project";
    enum { CALLEE_LEN = 1100 };
    char *callee = malloc(CALLEE_LEN + 1U);
    char *qualified = malloc(strlen(project) + 1U + CALLEE_LEN + 1U);
    ASSERT_NOT_NULL(callee);
    ASSERT_NOT_NULL(qualified);
    memset(callee, 'n', CALLEE_LEN);
    callee[CALLEE_LEN] = '\0';
    snprintf(qualified, strlen(project) + 1U + CALLEE_LEN + 1U, "%s.%s", project, callee);

    cbm_gbuf_t *gbuf = cbm_gbuf_new(project, "/tmp");
    ASSERT_NOT_NULL(gbuf);
    int64_t id =
        cbm_gbuf_upsert_node(gbuf, "Function", "target", qualified, "target.c", 1, 1, "{}");
    ASSERT_GT(id, 0);
    const cbm_gbuf_node_t *found = cbm_pipeline_lsp_target_node(gbuf, project, callee, false);
    if (!found) {
        printf("  long target-QN diagnostic: project=%zu callee=%zu combined=%zu\n",
               strlen(project), strlen(callee), strlen(qualified));
    }
    bool same_node = found && found->id == id;
    cbm_gbuf_free(gbuf);
    free(qualified);
    free(callee);
    ASSERT_TRUE(same_node);
    PASS();
}

/* ── Suite Registration ──────────────────────────────────────────── */

SUITE(parallel) {
    RUN_TEST(usage_semantic_reference_candidate_trusts_marked_producer);
    RUN_TEST(lsp_bare_segment_skips_preprocessor_spacing);
    RUN_TEST(lsp_resolve_qualified_static_call_normalizes_colons);
    RUN_TEST(lsp_resolve_distinct_exact_caller_targets_fail_closed);
    RUN_TEST(lsp_resolve_duplicate_exact_caller_rows_same_target_are_not_ambiguous);
    RUN_TEST(lsp_resolve_distinct_exact_caller_site_targets_are_ambiguous);
    RUN_TEST(lsp_resolve_project_prefixed_duplicate_is_not_ambiguous);
    RUN_TEST(lsp_resolve_project_prefix_spellings_stay_ambiguous_when_both_nodes_exist);
    RUN_TEST(lsp_resolve_jvm_package_alias_of_materialized_target_is_not_ambiguous);
    RUN_TEST(lsp_resolve_jvm_two_tail_only_packages_stay_ambiguous);
    RUN_TEST(lsp_resolve_exact_site_beats_higher_confidence_legacy_record);
    RUN_TEST(lsp_resolve_exact_site_beats_equal_confidence_legacy_record);
    RUN_TEST(lsp_resolve_malformed_span_is_not_legacy_compatible);
    RUN_TEST(lsp_resolve_below_floor_exact_does_not_block_legacy);
    RUN_TEST(lsp_tail_exact_site_beats_legacy_regardless_of_order);
    RUN_TEST(lsp_tail_duplicate_exact_rows_same_target_are_not_ambiguous);
    RUN_TEST(lsp_tail_distinct_exact_targets_are_ambiguous);
    RUN_TEST(lsp_tail_duplicate_legacy_rows_same_target_are_not_ambiguous);
    RUN_TEST(lsp_tail_distinct_legacy_targets_are_ambiguous);
    RUN_TEST(lsp_reference_duplicate_exact_rows_same_target_are_not_ambiguous);
    RUN_TEST(lsp_reference_project_prefixed_duplicate_is_not_ambiguous);
    RUN_TEST(lsp_reference_project_prefix_spellings_stay_ambiguous_when_both_nodes_exist);
    RUN_TEST(lsp_reference_jvm_distinct_packages_do_not_collapse_via_tail_node);
    RUN_TEST(lsp_reference_distinct_exact_targets_are_order_invariant);
    RUN_TEST(lsp_reference_duplicate_tail_rows_same_target_are_not_ambiguous);
    RUN_TEST(lsp_reference_distinct_tail_targets_are_ambiguous);
    RUN_TEST(lsp_target_node_supports_long_prefixed_qualified_name);
    RUN_TEST(grpc_service_name_preserves_service_suffix_issue294);
    RUN_TEST(grpc_no_phantom_route_from_plain_var_issue294);
    /* Graph buffer merge/shared-ID tests */
    RUN_TEST(gbuf_shared_ids_unique);
    RUN_TEST(gbuf_merge_nodes);
    RUN_TEST(gbuf_merge_edges);
    RUN_TEST(gbuf_merge_empty_src);
    RUN_TEST(gbuf_merge_src_free_safe);
    RUN_TEST(gbuf_next_id_accessors);

    /* Parallel pipeline parity tests */
    RUN_TEST(parallel_node_count);
    RUN_TEST(parallel_python_lsp_override_emits_lsp_strategy_edges);
    RUN_TEST(parallel_lsp_index_exact_site_beats_legacy_record_in_graph);
    RUN_TEST(parallel_lsp_index_distinct_exact_targets_fail_closed_in_graph);
    RUN_TEST(parallel_lsp_index_exact_ambiguity_does_not_fall_through_to_legacy);
    RUN_TEST(parallel_lsp_long_exact_key_never_yields_legacy_target);
    RUN_TEST(parallel_lsp_exact_index_handles_repeated_synthetic_occurrences_without_linear_scan);
#if defined(CBM_CALL_REFERENCE_LOOKUP_TEST_API) && CBM_CALL_REFERENCE_LOOKUP_TEST_API
    RUN_TEST(parallel_call_reference_lookup_rows_grow_linearly);
#endif
    RUN_TEST(parallel_tsx_local_component_shadow_has_no_false_call);
    RUN_TEST(parallel_exact_semantic_noncallable_target_stays_usage);
    RUN_TEST(parallel_typescript_module_value_usage_respects_lexical_shadows);
    RUN_TEST(parallel_typescript_import_namespace_exact_parity);
    RUN_TEST(parallel_tsx_import_namespace_exact_parity);
    RUN_TEST(parallel_kotlin_external_protocol_does_not_use_project_class_method_tail);
    RUN_TEST(parallel_kotlin_nonbinary_operator_carriers_reach_graph);
    RUN_TEST(parallel_rust_known_macro_does_not_fallback_to_local_function);
    RUN_TEST(parallel_rust_proc_macros_are_decorates_and_usage_only);
    RUN_TEST(parallel_c_preprocessed_coordinate_collision_preserves_hidden_target);
    RUN_TEST(parallel_cpp_preprocessed_coordinate_collision_preserves_hidden_target);
    RUN_TEST(parallel_cuda_preprocessed_coordinate_collision_preserves_hidden_target);
    RUN_TEST(parallel_python_lsp_override_cross_file_emits_lsp_strategy_edges);
    RUN_TEST(parallel_cross_file_reread_preserves_unretained_edges);
    RUN_TEST(parallel_java_kotlin_lsp_override_cross_file_emits_lsp_strategy_edges);
    RUN_TEST(parallel_lsp_tail_match_fallbacks_gated_to_jvm);
    RUN_TEST(parallel_calls_parity);
    RUN_TEST(parallel_defines_parity);
    RUN_TEST(parallel_defines_method_parity);
    RUN_TEST(parallel_imports_parity);
    RUN_TEST(parallel_usage_parity);
    RUN_TEST(parallel_inherits_parity);
    RUN_TEST(parallel_implements_parity);
    RUN_TEST(parallel_total_edges);
    RUN_TEST(parallel_empty_files);
    RUN_TEST(parallel_args_json_no_overflow);

    /* Cleanup shared state */
    parity_teardown();
}
