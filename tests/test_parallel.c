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

/* ── Run parallel pipeline on files, returning gbuf ───────────────── */

static cbm_gbuf_t *run_parallel(const char *project, const char *repo_path, cbm_file_info_t *files,
                                int file_count, int worker_count) {
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

    _Atomic int64_t shared_ids;
    int64_t gbuf_next = cbm_gbuf_next_id(gbuf);
    atomic_init(&shared_ids, gbuf_next);

    CBMFileResult **result_cache = calloc(file_count, sizeof(CBMFileResult *));

    cbm_init();
    cbm_parallel_extract(&ctx, files, file_count, result_cache, &shared_ids, worker_count);
    cbm_gbuf_set_next_id(gbuf, atomic_load(&shared_ids));

    cbm_build_registry_from_cache(&ctx, files, file_count, result_cache);

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

/* ── Suite Registration ──────────────────────────────────────────── */

SUITE(parallel) {
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
    RUN_TEST(parallel_python_lsp_override_cross_file_emits_lsp_strategy_edges);
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
