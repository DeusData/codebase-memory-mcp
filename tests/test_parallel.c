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
#include "pipeline/lsp_resolve.h"
#include "pipeline/pass_lsp_cross.h"
#include "pipeline/lsp_resolve.h"
#include "pipeline/worker_pool.h"
#include "graph_buffer/graph_buffer.h"
#include "discover/discover.h"
#include "foundation/platform.h"
#include "foundation/log.h"
#include "cbm.h"
#include <store/store.h>
#include <sqlite3.h>

#include <stdarg.h>
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

static void cbm_init_parallel_worker(int idx, void *ctx_ptr) {
    int *rcs = (int *)ctx_ptr;
    rcs[idx] = cbm_init();
}

TEST(parallel_cbm_init_concurrent_idempotent) {
    enum { INIT_CALLS = 32, INIT_WORKERS = 4 };
    int rcs[INIT_CALLS];
    memset(rcs, 0x7f, sizeof(rcs));

    cbm_parallel_for_opts_t opts = {.max_workers = INIT_WORKERS, .force_pthreads = false};
    cbm_parallel_for(INIT_CALLS, cbm_init_parallel_worker, rcs, opts);

    for (int i = 0; i < INIT_CALLS; i++) {
        ASSERT_EQ(rcs[i], 0);
    }
    PASS();
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

static cbm_gbuf_t *run_parallel_with_extract_opts(const char *project, const char *repo_path,
                                                  cbm_file_info_t *files, int file_count,
                                                  int worker_count,
                                                  const cbm_parallel_extract_opts_t *extract_opts) {
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

    CBMFileResult **result_cache = calloc((size_t)file_count, sizeof(CBMFileResult *));

    cbm_init();
    if (extract_opts) {
        cbm_parallel_extract_ex(&ctx, files, file_count, result_cache, &shared_ids, worker_count,
                                extract_opts);
    } else {
        cbm_parallel_extract(&ctx, files, file_count, result_cache, &shared_ids, worker_count);
    }
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

TEST(extraction_errors_fail_parallel_and_sequential_paths) {
    char dir[256];
    snprintf(dir, sizeof(dir), "/tmp/cbm_extract_error_XXXXXX");
    ASSERT_TRUE(cbm_mkdtemp(dir) != NULL);

    char path[512];
    snprintf(path, sizeof(path), "%s/input.txt", dir);
    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fclose(f);

    cbm_file_info_t file = {
        .path = path,
        .rel_path = (char *)"input.txt",
        .language = CBM_LANG_PYTHON,
    };
    atomic_int cancelled;
    atomic_init(&cancelled, 0);
    cbm_gbuf_t *gbuf = cbm_gbuf_new("extract-error", dir);
    cbm_registry_t *registry = cbm_registry_new();
    ASSERT_NOT_NULL(gbuf);
    ASSERT_NOT_NULL(registry);
    cbm_pipeline_ctx_t ctx = {
        .project_name = "extract-error",
        .repo_path = dir,
        .gbuf = gbuf,
        .registry = registry,
        .cancelled = &cancelled,
        .pkgmap_preseeded = true,
    };

    CBMFileResult *cache[1] = {NULL};
    _Atomic int64_t shared_ids;
    atomic_init(&shared_ids, 1);
    ASSERT_EQ(cbm_parallel_extract(&ctx, &file, 1, cache, &shared_ids, 1), 0);
    ASSERT_NOT_NULL(cache[0]);
    ASSERT_FALSE(cache[0]->has_error);
    int nodes_after_empty = cbm_gbuf_node_count(gbuf);
    cbm_free_result(cache[0]);
    cache[0] = NULL;

    f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fputs("content\n", f);
    fclose(f);
    file.language = (CBMLanguage)-1;
    ASSERT_NEQ(cbm_parallel_extract(&ctx, &file, 1, cache, &shared_ids, 1), 0);
    ASSERT_NULL(cache[0]);
    ASSERT_EQ(cbm_gbuf_node_count(gbuf), nodes_after_empty);

    ASSERT_NEQ(cbm_pipeline_pass_definitions(&ctx, &file, 1), 0);
    ASSERT_EQ(cbm_gbuf_node_count(gbuf), nodes_after_empty);

    cbm_registry_free(registry);
    cbm_gbuf_free(gbuf);
    unlink(path);
    rmdir(dir);
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

typedef struct {
    int self_get_calls;
} self_get_call_ctx_t;

static void count_self_get_call_edges(const cbm_gbuf_edge_t *edge, void *ud) {
    self_get_call_ctx_t *c = ud;
    if (!edge || !edge->type || strcmp(edge->type, "CALLS") != 0) {
        return;
    }
    if (edge->source_id != edge->target_id) {
        return;
    }
    if (edge->properties_json && strstr(edge->properties_json, "\"callee\":\"ec.get\"")) {
        c->self_get_calls++;
    }
}

static int count_extracted_calls_named(const CBMFileResult *result, const char *callee_name) {
    int count = 0;
    if (!result || !callee_name) {
        return 0;
    }
    for (int i = 0; i < result->calls.count; i++) {
        const char *got = result->calls.items[i].callee_name;
        if (got && strcmp(got, callee_name) == 0) {
            count++;
        }
    }
    return count;
}

TEST(parallel_unresolved_route_suffix_does_not_emit_self_call) {
    char dir[256];
    snprintf(dir, sizeof(dir), "/tmp/cbm_route_suffix_XXXXXX");
    ASSERT_TRUE(cbm_mkdtemp(dir) != NULL);

    const char *source = "def FilterPanel(ec, e):\n"
                         "    return ec.get(e.type)\n";
    CBMFileResult *extracted =
        cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "cbm_route_suffix",
                         "app.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(extracted);
    ASSERT_GT(count_extracted_calls_named(extracted, "ec.get"), 0);
    cbm_free_result(extracted);

    char path[512];
    snprintf(path, sizeof(path), "%s/app.py", dir);
    ASSERT_EQ(th_write_file(path, source), 0);

    cbm_file_info_t files[1] = {0};
    files[0].path = path;
    files[0].rel_path = (char *)"app.py";
    files[0].language = CBM_LANG_PYTHON;

    cbm_gbuf_t *gbuf = run_parallel("cbm_route_suffix", dir, files, 1, 1);
    ASSERT_NOT_NULL(gbuf);

    self_get_call_ctx_t c = {0};
    cbm_gbuf_foreach_edge(gbuf, count_self_get_call_edges, &c);
    ASSERT_EQ(c.self_get_calls, 0);

    cbm_gbuf_free(gbuf);
    th_rmtree(dir);
    PASS();
}

typedef struct {
    const char *url_path;
    int route_registration_calls;
} route_registration_count_ctx_t;

static void count_route_registration_edges(const cbm_gbuf_edge_t *edge, void *ud) {
    route_registration_count_ctx_t *c = ud;
    if (!edge || !edge->type || strcmp(edge->type, "CALLS") != 0 || !edge->properties_json) {
        return;
    }
    if (strstr(edge->properties_json, "\"via\":\"route_registration\"") &&
        strstr(edge->properties_json, c->url_path)) {
        c->route_registration_calls++;
    }
}

static int count_route_registration_for_path(cbm_gbuf_t *gbuf, const char *url_path) {
    route_registration_count_ctx_t c = {.url_path = url_path, .route_registration_calls = 0};
    cbm_gbuf_foreach_edge(gbuf, count_route_registration_edges, &c);
    return c.route_registration_calls;
}

static void count_exception_edges(const cbm_gbuf_edge_t *edge, void *ud) {
    int *count = (int *)ud;
    if (!edge || !edge->type || !count) {
        return;
    }
    if (strcmp(edge->type, "THROWS") == 0 || strcmp(edge->type, "RAISES") == 0) {
        (*count)++;
    }
}

static int exception_edge_count(cbm_gbuf_t *gbuf) {
    int count = 0;
    cbm_gbuf_foreach_edge(gbuf, count_exception_edges, &count);
    return count;
}

TEST(parallel_top_level_raise_matches_sequential_no_file_fallback) {
    char dir[CBM_PATH_MAX];
    int n = snprintf(dir, sizeof(dir), "%s/cbm_top_raise_XXXXXX", cbm_tmpdir());
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(dir));
    ASSERT_TRUE(cbm_mkdtemp(dir) != NULL);

    const char *source =
        "class HTTPException(Exception):\n"
        "    pass\n\n"
        "raise HTTPException()\n";

    char path[CBM_PATH_MAX];
    n = snprintf(path, sizeof(path), "%s/app.py", dir);
    ASSERT_GT(n, 0);
    ASSERT_LT((size_t)n, sizeof(path));
    ASSERT_EQ(th_write_file(path, source), 0);

    cbm_file_info_t files[1] = {0};
    files[0].path = path;
    files[0].rel_path = (char *)"app.py";
    files[0].language = CBM_LANG_PYTHON;

    cbm_gbuf_t *seq = run_sequential("cbm_top_raise", dir, files, 1);
    cbm_gbuf_t *par = run_parallel("cbm_top_raise", dir, files, 1, 1);
    ASSERT_NOT_NULL(seq);
    ASSERT_NOT_NULL(par);

    ASSERT_EQ(exception_edge_count(seq), 0);
    ASSERT_EQ(exception_edge_count(par), 0);

    cbm_gbuf_free(seq);
    cbm_gbuf_free(par);
    th_rmtree(dir);
    PASS();
}

TEST(parallel_fastapi_websocket_route_registration_matches_sequential) {
    char dir[256];
    snprintf(dir, sizeof(dir), "/tmp/cbm_ws_routes_XXXXXX");
    ASSERT_TRUE(cbm_mkdtemp(dir) != NULL);

    const char *source =
        "from fastapi import APIRouter, WebSocket\n\n"
        "router = APIRouter()\n\n"
        "@router.websocket('/custom_error/')\n"
        "async def router_ws_custom_error(websocket: WebSocket):\n"
        "    raise RuntimeError('boom')\n\n"
        "@router.websocket_route('/router')\n"
        "async def routerindex(websocket: WebSocket):\n"
        "    await websocket.accept()\n";

    char path[512];
    snprintf(path, sizeof(path), "%s/app.py", dir);
    ASSERT_EQ(th_write_file(path, source), 0);

    cbm_file_info_t files[1] = {0};
    files[0].path = path;
    files[0].rel_path = (char *)"app.py";
    files[0].language = CBM_LANG_PYTHON;

    cbm_gbuf_t *seq = run_sequential("cbm_ws_routes", dir, files, 1);
    cbm_gbuf_t *par = run_parallel("cbm_ws_routes", dir, files, 1, 1);
    ASSERT_NOT_NULL(seq);
    ASSERT_NOT_NULL(par);

    ASSERT_EQ(count_route_registration_for_path(seq, "/custom_error/"), 1);
    ASSERT_EQ(count_route_registration_for_path(seq, "/router"), 1);
    ASSERT_EQ(count_route_registration_for_path(par, "/custom_error/"), 1);
    ASSERT_EQ(count_route_registration_for_path(par, "/router"), 1);

    cbm_gbuf_free(seq);
    cbm_gbuf_free(par);
    th_rmtree(dir);
    PASS();
}

/* ── Production pipeline worker-count parity ─────────────────────── */

enum {
    PARITY_REPO_FILE_COUNT = 64,
    PARITY_EXPECTED_FILE_HASHES = PARITY_REPO_FILE_COUNT + 2,
    PARITY_DB_PATH_COUNT = 4,
    PARITY_PATH_BUF = CBM_SZ_512,
    PARITY_SOURCE_BUF = CBM_SZ_4K,
    PARITY_REP_QN_COUNT = 4,
};

typedef struct {
    int nodes;
    int edges;
    int file_hashes;
    int calls;
    int imports;
    int usage;
    int semantic;
    int representative_qns;
} pipeline_db_counts_t;

static int parity_format(char *dst, size_t dst_sz, const char *fmt, ...) {
    if (!dst || dst_sz == 0 || !fmt) {
        return CBM_NOT_FOUND;
    }
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(dst, dst_sz, fmt, ap);
    va_end(ap);
    return n >= 0 && (size_t)n < dst_sz ? 0 : CBM_NOT_FOUND;
}

static void remove_sqlite_family(const char *db_path) {
    if (!db_path || !db_path[0]) {
        return;
    }
    cbm_unlink(db_path);
    char sidecar[PARITY_PATH_BUF];
    if (parity_format(sidecar, sizeof(sidecar), "%s-wal", db_path) == 0) {
        cbm_unlink(sidecar);
    }
    if (parity_format(sidecar, sizeof(sidecar), "%s-shm", db_path) == 0) {
        cbm_unlink(sidecar);
    }
}

static int sqlite_integrity_ok(const char *db_path) {
    sqlite3 *db = NULL;
    sqlite3_stmt *stmt = NULL;
    int ok = 0;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK && db &&
        sqlite3_prepare_v2(db, "PRAGMA integrity_check;", CBM_NOT_FOUND, &stmt, NULL) ==
            SQLITE_OK &&
        sqlite3_step(stmt) == SQLITE_ROW) {
        const char *msg = (const char *)sqlite3_column_text(stmt, 0);
        ok = msg && strcmp(msg, "ok") == 0;
    }
    if (stmt) {
        sqlite3_finalize(stmt);
    }
    if (db) {
        sqlite3_close(db);
    }
    return ok;
}

static int write_worker_parity_repo(char *repo_dir, size_t repo_dir_sz) {
    if (!repo_dir || repo_dir_sz == 0) {
        return CBM_NOT_FOUND;
    }
    if (parity_format(repo_dir, repo_dir_sz, "%s/cbm_pipe_parity_XXXXXX", cbm_tmpdir()) != 0) {
        return CBM_NOT_FOUND;
    }
    if (!cbm_mkdtemp(repo_dir)) {
        return CBM_NOT_FOUND;
    }

    char path[PARITY_PATH_BUF];
    char src[PARITY_SOURCE_BUF];

    if (parity_format(path, sizeof(path), "%s/common.py", repo_dir) != 0) {
        return CBM_NOT_FOUND;
    }
    if (th_write_file(path,
                      "def shared(value):\n"
                      "    return value + 1\n"
                      "\n"
                      "class Shared:\n"
                      "    def touch(self, value):\n"
                      "        return shared(value)\n") != 0) {
        return CBM_NOT_FOUND;
    }

    for (int i = 0; i < PARITY_REPO_FILE_COUNT; i++) {
        if (parity_format(path, sizeof(path), "%s/mod_%02d.py", repo_dir, i) != 0) {
            return CBM_NOT_FOUND;
        }
        int prev = (i + PARITY_REPO_FILE_COUNT - 1) % PARITY_REPO_FILE_COUNT;
        if (parity_format(src, sizeof(src),
                          "from common import Shared, shared\n"
                          "from mod_%02d import func_%02d\n"
                          "\n"
                          "class Worker%02d:\n"
                          "    def method_%02d(self, value):\n"
                          "        helper = Shared()\n"
                          "        return helper.touch(shared(value))\n"
                          "\n"
                          "def func_%02d(value):\n"
                          "    item = Worker%02d()\n"
                          "    return item.method_%02d(value)\n"
                          "\n"
                          "def chain_%02d(value):\n"
                          "    return func_%02d(value) + func_%02d(value) + shared(value)\n",
                          prev, prev, i, i, i, i, i, i, i, prev) != 0) {
            return CBM_NOT_FOUND;
        }
        if (th_write_file(path, src) != 0) {
            return CBM_NOT_FOUND;
        }
    }

    if (parity_format(path, sizeof(path), "%s/app.py", repo_dir) != 0) {
        return CBM_NOT_FOUND;
    }
    FILE *f = fopen(path, "w");
    if (!f) {
        return CBM_NOT_FOUND;
    }
    int write_ok = 1;
    for (int i = 0; i < PARITY_REPO_FILE_COUNT; i++) {
        write_ok = write_ok && fprintf(f, "from mod_%02d import chain_%02d\n", i, i) >= 0;
    }
    write_ok = write_ok && fputs("\ndef main():\n    total = 0\n", f) >= 0;
    for (int i = 0; i < PARITY_REPO_FILE_COUNT; i++) {
        write_ok = write_ok && fprintf(f, "    total += chain_%02d(%d)\n", i, i) >= 0;
    }
    write_ok = write_ok && fputs("    return total\n", f) >= 0;
    if (fclose(f) != 0) {
        write_ok = 0;
    }
    return write_ok ? 0 : CBM_NOT_FOUND;
}

static int count_representative_qns(cbm_store_t *store) {
    static const char *qns[PARITY_REP_QN_COUNT] = {
        "pipe-parity.common.shared",
        "pipe-parity.common.Shared.touch",
        "pipe-parity.mod_00.func_00",
        "pipe-parity.app.main",
    };
    int found = 0;
    for (int i = 0; i < PARITY_REP_QN_COUNT; i++) {
        cbm_node_t node = {0};
        if (cbm_store_find_node_by_qn(store, "pipe-parity", qns[i], &node) == CBM_STORE_OK) {
            found++;
            cbm_node_free_fields(&node);
        }
    }
    return found;
}

static int run_pipeline_worker_case(const char *repo_dir, const char *db_path, int workers,
                                    pipeline_db_counts_t *out) {
    if (!repo_dir || !db_path || !out) {
        return CBM_NOT_FOUND;
    }
    char worker_buf[CBM_SZ_32];
    if (parity_format(worker_buf, sizeof(worker_buf), "%d", workers) != 0) {
        return CBM_NOT_FOUND;
    }
    if (workers > 0) {
        cbm_setenv("CBM_WORKERS", worker_buf, 1);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }

    remove_sqlite_family(db_path);

    cbm_pipeline_t *p = cbm_pipeline_new(repo_dir, db_path, CBM_MODE_FULL);
    if (!p) {
        return CBM_NOT_FOUND;
    }
    cbm_pipeline_set_project_name(p, "pipe-parity");
    int rc = cbm_pipeline_run(p);
    cbm_pipeline_free(p);
    if (rc != 0 || !sqlite_integrity_ok(db_path)) {
        return CBM_NOT_FOUND;
    }

    cbm_store_t *store = cbm_store_open_path_query(db_path);
    if (!store) {
        return CBM_NOT_FOUND;
    }
    cbm_file_hash_t *hashes = NULL;
    int hash_count = 0;
    int hash_rc = cbm_store_get_file_hashes(store, "pipe-parity", &hashes, &hash_count);
    out->nodes = cbm_store_count_nodes(store, "pipe-parity");
    out->edges = cbm_store_count_edges(store, "pipe-parity");
    out->calls = cbm_store_count_edges_by_type(store, "pipe-parity", "CALLS");
    out->imports = cbm_store_count_edges_by_type(store, "pipe-parity", "IMPORTS");
    out->usage = cbm_store_count_edges_by_type(store, "pipe-parity", "USAGE");
    out->semantic = cbm_store_count_edges_by_type(store, "pipe-parity", "SEMANTICALLY_RELATED");
    out->file_hashes = hash_rc == CBM_STORE_OK ? hash_count : CBM_STORE_ERR;
    out->representative_qns = count_representative_qns(store);
    cbm_store_free_file_hashes(hashes, hash_count);

    cbm_project_t project = {0};
    int project_rc = cbm_store_get_project(store, "pipe-parity", &project);
    int project_root_ok =
        project_rc == CBM_STORE_OK && project.root_path && strcmp(project.root_path, repo_dir) == 0;
    cbm_project_free_fields(&project);

    cbm_store_close(store);

    return (project_root_ok && out->nodes > 0 && out->edges > 0 &&
            out->file_hashes == PARITY_EXPECTED_FILE_HASHES && out->calls > 0 &&
            out->imports > 0 && out->representative_qns == PARITY_REP_QN_COUNT)
               ? 0
               : CBM_NOT_FOUND;
}

static int assert_pipeline_counts_equal(const pipeline_db_counts_t *want,
                                        const pipeline_db_counts_t *got) {
    if (!want || !got) {
        return CBM_NOT_FOUND;
    }
    return want->nodes == got->nodes && want->edges == got->edges &&
           want->file_hashes == got->file_hashes && want->calls == got->calls &&
           want->imports == got->imports && want->usage == got->usage &&
           want->semantic == got->semantic && want->representative_qns == got->representative_qns
               ? 0
               : CBM_NOT_FOUND;
}

TEST(parallel_full_pipeline_worker_count_parity_64_files) {
    char saved_workers[CBM_SZ_32] = {0};
    bool had_workers =
        cbm_safe_getenv("CBM_WORKERS", saved_workers, sizeof(saved_workers), NULL) != NULL;

    char repo_dir[PARITY_PATH_BUF] = {0};
    int rc = write_worker_parity_repo(repo_dir, sizeof(repo_dir));

    const int workers[PARITY_DB_PATH_COUNT] = {1, 2, 4, 0};
    char db_paths[PARITY_DB_PATH_COUNT][PARITY_PATH_BUF] = {{0}};
    pipeline_db_counts_t counts[PARITY_DB_PATH_COUNT] = {{0}};
    if (rc == 0) {
        for (int i = 0; i < PARITY_DB_PATH_COUNT; i++) {
            if (parity_format(db_paths[i], sizeof(db_paths[i]), "%s/pipe-parity-%d.db", repo_dir,
                              i) != 0) {
                rc = CBM_NOT_FOUND;
                break;
            }
            if (run_pipeline_worker_case(repo_dir, db_paths[i], workers[i], &counts[i]) != 0) {
                rc = CBM_NOT_FOUND;
                break;
            }
        }
    }

    if (rc == 0) {
        for (int i = 1; i < PARITY_DB_PATH_COUNT; i++) {
            if (assert_pipeline_counts_equal(&counts[0], &counts[i]) != 0) {
                rc = CBM_NOT_FOUND;
                break;
            }
        }
    }

    for (int i = 0; i < PARITY_DB_PATH_COUNT; i++) {
        remove_sqlite_family(db_paths[i]);
    }
    if (repo_dir[0]) {
        th_rmtree(repo_dir);
    }
    if (had_workers) {
        cbm_setenv("CBM_WORKERS", saved_workers, 1);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }

    ASSERT_EQ(rc, 0);
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

TEST(lsp_resolution_matches_cpp_segments_and_reason_joins) {
    CBMResolvedCall items[] = {
        {.caller_qn = "proj.C.run",
         .callee_qn = "proj.C.doWork",
         .strategy = "lsp_type_dispatch",
         .confidence = 0.90f,
         .reason = NULL},
        {.caller_qn = "proj.C.run",
         .callee_qn = "proj.target",
         .strategy = "lsp_func_ptr",
         .confidence = 0.85f,
         .reason = "fp"},
        {.caller_qn = "proj.C.run",
         .callee_qn = "proj.C.~C",
         .strategy = "lsp_destructor",
         .confidence = 0.90f,
         .reason = "ptr"},
    };
    CBMResolvedCallArray arr = {.items = items, .count = 3, .cap = 3};

    CBMCall member_call = {.enclosing_func_qn = "proj.C.run", .callee_name = "obj->doWork"};
    ASSERT(cbm_pipeline_find_lsp_resolution(&arr, &member_call, false) == &items[0]);

    CBMCall scoped_call = {.enclosing_func_qn = "proj.C.run", .callee_name = "ns::doWork"};
    ASSERT(cbm_pipeline_find_lsp_resolution(&arr, &scoped_call, false) == &items[0]);

    CBMCall fp_call = {.enclosing_func_qn = "proj.C.run", .callee_name = "fp"};
    ASSERT(cbm_pipeline_find_lsp_resolution(&arr, &fp_call, false) == &items[1]);

    CBMCall dtor_call = {.enclosing_func_qn = "proj.C.run", .callee_name = "ptr"};
    ASSERT(cbm_pipeline_find_lsp_resolution(&arr, &dtor_call, false) == &items[2]);

    PASS();
}

TEST(lsp_resolution_index_matches_linear_cpp_semantics) {
    CBMResolvedCall items[] = {
        {.caller_qn = "proj.C.run",
         .callee_qn = "proj.C.doWork",
         .strategy = "lsp_type_dispatch",
         .confidence = 0.90f,
         .reason = NULL},
        {.caller_qn = "proj.C.run",
         .callee_qn = "proj.target",
         .strategy = "lsp_func_ptr",
         .confidence = 0.85f,
         .reason = "fp"},
    };
    CBMResolvedCallArray arr = {.items = items, .count = 2, .cap = 2};
    cbm_lsp_resolution_index_t idx = {0};
    cbm_lsp_resolution_index_build(&idx, &arr, 2, 0.0);
    ASSERT_TRUE(idx.complete);

    CBMCall member_call = {.enclosing_func_qn = "proj.C.run", .callee_name = "obj->doWork"};
    ASSERT(cbm_lsp_resolution_index_find(&idx, &arr, &member_call, 0.0, false) == &items[0]);

    CBMCall fp_call = {.enclosing_func_qn = "proj.C.run", .callee_name = "fp"};
    ASSERT(cbm_lsp_resolution_index_find(&idx, &arr, &fp_call, 0.0, false) == &items[1]);

    cbm_lsp_resolution_index_free(&idx);
    PASS();
}

TEST(lsp_resolution_index_overlong_key_falls_back_to_linear) {
    char caller[CBM_SZ_1K + CBM_SZ_128];
    memset(caller, 'a', sizeof(caller) - 1);
    caller[sizeof(caller) - 1] = '\0';

    CBMResolvedCall item = {.caller_qn = caller,
                            .callee_qn = "proj.target",
                            .strategy = "lsp_direct",
                            .confidence = 0.95f,
                            .reason = NULL};
    CBMResolvedCallArray arr = {.items = &item, .count = 1, .cap = 1};
    cbm_lsp_resolution_index_t idx = {0};
    cbm_lsp_resolution_index_build(&idx, &arr, 1, 0.0);
    ASSERT_FALSE(idx.complete);

    CBMCall call = {.enclosing_func_qn = caller, .callee_name = "target"};
    ASSERT(cbm_lsp_resolution_index_find(&idx, &arr, &call, 0.0, false) == &item);

    cbm_lsp_resolution_index_free(&idx);
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

static bool resolved_call_contains(const CBMResolvedCallArray *arr, const char *caller_sub,
                                   const char *callee_sub) {
    if (!arr || !caller_sub || !callee_sub) {
        return false;
    }
    for (int i = 0; i < arr->count; i++) {
        const CBMResolvedCall *rc = &arr->items[i];
        if (rc->caller_qn && strstr(rc->caller_qn, caller_sub) && rc->callee_qn &&
            strstr(rc->callee_qn, callee_sub)) {
            return true;
        }
    }
    return false;
}

typedef struct {
    const cbm_gbuf_t *gbuf;
    const char *source_sub;
    const char *target_sub;
    const char *props_sub;
    bool found;
} call_edge_contains_ctx_t;

static void call_edge_contains_visit(const cbm_gbuf_edge_t *edge, void *ud) {
    call_edge_contains_ctx_t *ctx = ud;
    if (!ctx || ctx->found || !edge || !edge->type || strcmp(edge->type, "CALLS") != 0) {
        return;
    }
    const cbm_gbuf_node_t *source = cbm_gbuf_find_by_id(ctx->gbuf, edge->source_id);
    const cbm_gbuf_node_t *target = cbm_gbuf_find_by_id(ctx->gbuf, edge->target_id);
    if (!source || !target || !source->qualified_name || !target->qualified_name) {
        return;
    }
    if (strstr(source->qualified_name, ctx->source_sub) &&
        strstr(target->qualified_name, ctx->target_sub) &&
        (!ctx->props_sub ||
         (edge->properties_json && strstr(edge->properties_json, ctx->props_sub)))) {
        ctx->found = true;
    }
}

static bool call_edge_contains(const cbm_gbuf_t *gbuf, const char *source_sub,
                               const char *target_sub, const char *props_sub) {
    if (!gbuf || !source_sub || !target_sub) {
        return false;
    }
    call_edge_contains_ctx_t ctx = {
        .gbuf = gbuf,
        .source_sub = source_sub,
        .target_sub = target_sub,
        .props_sub = props_sub,
    };
    cbm_gbuf_foreach_edge(gbuf, call_edge_contains_visit, &ctx);
    return ctx.found;
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
    FILE *jf = fopen(jpath, "w");
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
    FILE *kf = fopen(kpath, "w");
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

TEST(parallel_cross_lsp_pruning_requires_matching_call_resolution) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_par_pylsp_prune_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("mkdtemp failed");
    }

    char rpath[512];
    snprintf(rpath, sizeof(rpath), "%s/routing.py", tmpdir);
    FILE *rf = fopen(rpath, "w");
    if (!rf) {
        rmdir(tmpdir);
        FAIL("fopen routing.py failed");
    }
    fprintf(rf, "class APIRouter:\n"
                "    def add_api_route(self):\n"
                "        return None\n"
                "    def include_router(self):\n"
                "        self.add_api_route()\n");
    fclose(rf);

    cbm_file_info_t files[1] = {0};
    files[0].path = rpath;
    files[0].rel_path = (char *)"routing.py";
    files[0].language = CBM_LANG_PYTHON;

    cbm_gbuf_t *gbuf = cbm_gbuf_new("cbm_par_pylsp_prune", tmpdir);
    cbm_registry_t *reg = cbm_registry_new();
    CBMFileResult **result_cache = calloc(1, sizeof(*result_cache));
    ASSERT_NOT_NULL(gbuf);
    ASSERT_NOT_NULL(reg);
    ASSERT_NOT_NULL(result_cache);

    atomic_int cancelled;
    atomic_init(&cancelled, 0);
    cbm_pipeline_ctx_t ctx = {.project_name = "cbm_par_pylsp_prune",
                              .repo_path = tmpdir,
                              .gbuf = gbuf,
                              .registry = reg,
                              .cancelled = &cancelled};
    _Atomic int64_t shared_ids;
    atomic_init(&shared_ids, cbm_gbuf_next_id(gbuf));

    cbm_init();
    ASSERT_EQ(cbm_parallel_extract(&ctx, files, 1, result_cache, &shared_ids, 1), 0);
    cbm_gbuf_set_next_id(gbuf, atomic_load(&shared_ids));
    ASSERT_NOT_NULL(result_cache[0]);
    ASSERT_GT(result_cache[0]->calls.count, 0);

    result_cache[0]->resolved_calls.count = 0;
    CBMResolvedCall unrelated = {.caller_qn = "cbm_par_pylsp_prune.routing.unrelated",
                                 .callee_qn = "cbm_par_pylsp_prune.routing.APIRouter.unrelated",
                                 .strategy = "lsp_method",
                                 .confidence = 0.95f};
    cbm_resolvedcall_push(&result_cache[0]->resolved_calls, &result_cache[0]->arena, unrelated);
    ASSERT_EQ(result_cache[0]->resolved_calls.count, result_cache[0]->calls.count);

    ASSERT_EQ(cbm_build_registry_from_cache(&ctx, files, 1, result_cache), 0);

    char **def_modules = calloc(1, sizeof(*def_modules));
    int def_count = 0;
    CBMLSPDef *all_defs =
        cbm_pxc_collect_all_defs(result_cache, files, 1, ctx.project_name, def_modules, &def_count);
    CBMModuleDefIndex *module_def_index =
        all_defs ? cbm_pxc_build_module_def_index(all_defs, def_count) : NULL;
    ASSERT_NOT_NULL(all_defs);

    ASSERT_EQ(cbm_parallel_resolve(&ctx, files, 1, result_cache, &shared_ids, 1, all_defs,
                                   def_count, def_modules, module_def_index,
                                   NULL /* cross_registries */),
              0);
    cbm_gbuf_set_next_id(gbuf, atomic_load(&shared_ids));

    ASSERT_TRUE(resolved_call_contains(&result_cache[0]->resolved_calls, "include_router",
                                       "add_api_route"));
    lsp_edge_count_ctx_t lsp_edges = {0};
    cbm_gbuf_foreach_edge(gbuf, count_lsp_call_edges, &lsp_edges);
    ASSERT_GT(lsp_edges.total_calls, 0);
    ASSERT_GT(lsp_edges.lsp_strategy_count, 0);
    ASSERT_TRUE(call_edge_contains(gbuf, "APIRouter.include_router", "APIRouter.add_api_route",
                                   "\"strategy\":\"lsp_method\""));

    cbm_pxc_free_module_def_index(module_def_index);
    free(all_defs);
    if (def_modules) {
        free(def_modules[0]);
        free(def_modules);
    }
    cbm_free_result(result_cache[0]);
    free(result_cache);
    cbm_registry_free(reg);
    cbm_gbuf_free(gbuf);
    unlink(rpath);
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
    CBMResolvedCall rc;
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

TEST(lsp_resolve_misattribution_is_bounded) {
    /* Two same-named subs from different namespaces (A::foo, B::foo) resolved
     * within the same enclosing function. Both resolved short-names normalize
     * to `foo`, so a textual `B::foo` matches both by short-name — the
     * theoretical mis-attribution. The function bounds this: it returns the
     * highest-confidence match (deterministic, never both), and the bound is
     * enforced by caller-QN equality + the confidence floor. */
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
    ASSERT(hit != NULL);
    /* Highest-confidence qualifying entry wins; the cross-caller 0.95 entry is
     * excluded by caller-QN equality, the 0.3 entry by the floor. */
    ASSERT(strcmp(hit->callee_qn, "proj.B.foo") == 0);

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

/* ── Suite Registration ──────────────────────────────────────────── */

SUITE(parallel) {
    RUN_TEST(parallel_cbm_init_concurrent_idempotent);
    RUN_TEST(lsp_resolve_qualified_static_call_normalizes_colons);
    RUN_TEST(lsp_resolve_misattribution_is_bounded);
    RUN_TEST(grpc_service_name_preserves_service_suffix_issue294);
    RUN_TEST(grpc_no_phantom_route_from_plain_var_issue294);
    /* Graph buffer merge/shared-ID tests */
    RUN_TEST(gbuf_shared_ids_unique);
    RUN_TEST(gbuf_merge_nodes);
    RUN_TEST(gbuf_merge_edges);
    RUN_TEST(gbuf_merge_empty_src);
    RUN_TEST(gbuf_merge_src_free_safe);
    RUN_TEST(gbuf_next_id_accessors);
    RUN_TEST(lsp_resolution_matches_cpp_segments_and_reason_joins);
    RUN_TEST(lsp_resolution_index_matches_linear_cpp_semantics);
    RUN_TEST(lsp_resolution_index_overlong_key_falls_back_to_linear);

    /* Parallel pipeline parity tests */
    RUN_TEST(parallel_node_count);
    RUN_TEST(parallel_python_lsp_override_emits_lsp_strategy_edges);
    RUN_TEST(parallel_python_lsp_override_cross_file_emits_lsp_strategy_edges);
    RUN_TEST(parallel_cross_lsp_pruning_requires_matching_call_resolution);
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
    RUN_TEST(parallel_full_pipeline_worker_count_parity_64_files);
    RUN_TEST(parallel_empty_files);
    RUN_TEST(extraction_errors_fail_parallel_and_sequential_paths);
    RUN_TEST(parallel_args_json_no_overflow);
    RUN_TEST(parallel_unresolved_route_suffix_does_not_emit_self_call);
    RUN_TEST(parallel_top_level_raise_matches_sequential_no_file_fallback);
    RUN_TEST(parallel_fastapi_websocket_route_registration_matches_sequential);

    /* Cleanup shared state */
    parity_teardown();
}
