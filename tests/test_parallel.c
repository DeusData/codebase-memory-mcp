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

    /* Parallel pipeline parity tests */
    RUN_TEST(parallel_node_count);
    RUN_TEST(parallel_python_lsp_override_emits_lsp_strategy_edges);
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
