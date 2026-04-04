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
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
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
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    (void)system(cmd);
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

    cbm_parallel_resolve(&ctx, files, file_count, result_cache, &shared_ids, worker_count);
    cbm_gbuf_set_next_id(gbuf, atomic_load(&shared_ids));

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
        SKIP("setup failed");
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
        SKIP("setup failed");
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(parallel_defines_parity) {
    int rc = assert_edge_type_parity("DEFINES");
    if (rc == -1)
        SKIP("setup failed");
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(parallel_defines_method_parity) {
    int rc = assert_edge_type_parity("DEFINES_METHOD");
    if (rc == -1)
        SKIP("setup failed");
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(parallel_imports_parity) {
    int rc = assert_edge_type_parity("IMPORTS");
    if (rc == -1)
        SKIP("setup failed");
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(parallel_usage_parity) {
    int rc = assert_edge_type_parity("USAGE");
    if (rc == -1)
        SKIP("setup failed");
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(parallel_inherits_parity) {
    int rc = assert_edge_type_parity("INHERITS");
    if (rc == -1)
        SKIP("setup failed");
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(parallel_implements_parity) {
    int rc = assert_edge_type_parity("IMPLEMENTS");
    if (rc == -1)
        SKIP("setup failed");
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(parallel_total_edges) {
    if (ensure_parity_setup() != 0)
        SKIP("setup failed");
    int seq = cbm_gbuf_edge_count(g_seq_gbuf);
    int par = cbm_gbuf_edge_count(g_par_gbuf);
    ASSERT_GT(seq, 0);
    ASSERT_EQ(seq, par);
    PASS();
}

/* ── GDScript Parallel Parity Tests ─────────────────────────────── */

static char g_gd_tmpdir[512];
static cbm_gbuf_t *g_gd_seq_gbuf = NULL;
static cbm_gbuf_t *g_gd_par_gbuf = NULL;
static int g_gd_parity_setup_done = 0;

static int setup_gdscript_repo(void) {
    snprintf(g_gd_tmpdir, sizeof(g_gd_tmpdir), "/tmp/cbm_gd_par_XXXXXX");
    if (!cbm_mkdtemp(g_gd_tmpdir))
        return -1;

    char path[512];

    /* actors/ */
    snprintf(path, sizeof(path), "%s/actors", g_gd_tmpdir);
    cbm_mkdir(path);

    /* base.gd */
    snprintf(path, sizeof(path), "%s/actors/base.gd", g_gd_tmpdir);
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "class_name Base\nsignal hit\nfunc ping():\n    pass\n");
    fclose(f);

    /* receiver.gd */
    snprintf(path, sizeof(path), "%s/actors/receiver.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "class_name Receiver\nsignal hit\n");
    fclose(f);

    /* weapon.gd */
    snprintf(path, sizeof(path), "%s/actors/weapon.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "class_name Weapon\n");
    fclose(f);

    /* player.gd */
    snprintf(path, sizeof(path), "%s/actors/player.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f,
        "class_name Player\n"
        "const BaseAlias = preload(\"res://actors/base.gd\")\n"
        "const ReceiverClass = preload(\"res://actors/receiver.gd\")\n"
        "extends BaseAlias\n"
        "signal hit\n"
        "const Weapon = preload(\"res://actors/weapon.gd\")\n"
        "var WeaponRel = load(\"weapon.gd\")\n"
        "const Scene = preload(\"res://actors/player.tscn\")\n"
        "func attack():\n"
        "    emit_signal(\"hit\")\n"
        "    self.hit.emit()\n"
        "    hit.connect(_on_hit)\n"
        "    var r = ReceiverClass.new()\n"
        "    r.hit.connect(_on_receiver_hit)\n"
        "    r.hit.emit()\n"
        "    helper()\n"
        "\n"
        "func helper():\n"
        "    pass\n");
    fclose(f);

    /* player_path_extends.gd */
    snprintf(path, sizeof(path), "%s/actors/player_path_extends.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "class_name PlayerPath\nextends \"res://actors/base.gd\"\nfunc attack_path():\n    pass\n");
    fclose(f);

    /* player_named_extends.gd */
    snprintf(path, sizeof(path), "%s/actors/player_named_extends.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "class_name PlayerNamed\nextends Base\nfunc attack_named():\n    pass\n");
    fclose(f);

    /* player_preload_extends.gd */
    snprintf(path, sizeof(path), "%s/actors/player_preload_extends.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "class_name PlayerPreloadPath\nextends preload(\"res://actors/base.gd\")\nfunc attack_preload_path():\n    pass\n");
    fclose(f);

    /* nameless_script.gd */
    snprintf(path, sizeof(path), "%s/actors/nameless_script.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "extends \"res://actors/base.gd\"\nsignal ghost_hit\nfunc ghost_attack():\n    emit_signal(\"ghost_hit\")\n");
    fclose(f);

    /* dynamic_receiver.gd */
    snprintf(path, sizeof(path), "%s/actors/dynamic_receiver.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "class_name DynamicReceiverCase\nfunc attack_dynamic(target):\n    target.hit.emit()\n");
    fclose(f);

    /* builtin_base.gd */
    snprintf(path, sizeof(path), "%s/actors/builtin_base.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "class_name BuiltinBaseCase\nextends Node2D\nfunc tick_builtin():\n    pass\n");
    fclose(f);

    /* player.tscn (non-code asset) */
    snprintf(path, sizeof(path), "%s/actors/player.tscn", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "[gd_scene load_steps=2 format=3]\n");
    fclose(f);

    return 0;
}

static void teardown_gdscript_repo(void) {
    if (g_gd_tmpdir[0]) {
        char cmd[1024];
        snprintf(cmd, sizeof(cmd), "rm -rf '%s'", g_gd_tmpdir);
        (void)system(cmd);
    }
    g_gd_tmpdir[0] = '\0';
}

static int ensure_gd_parity_setup(void) {
    if (g_gd_parity_setup_done)
        return 0;

    if (setup_gdscript_repo() != 0)
        return -1;

    cbm_discover_opts_t opts = {.mode = CBM_MODE_FULL};
    cbm_file_info_t *files = NULL;
    int file_count = 0;
    if (cbm_discover(g_gd_tmpdir, &opts, &files, &file_count) != 0)
        return -1;

    const char *project = "gd-par-test";

    g_gd_seq_gbuf = run_sequential(project, g_gd_tmpdir, files, file_count);
    g_gd_par_gbuf = run_parallel(project, g_gd_tmpdir, files, file_count, 2);

    cbm_discover_free(files, file_count);
    g_gd_parity_setup_done = 1;
    return 0;
}

static void gd_parity_teardown(void) {
    if (g_gd_seq_gbuf) {
        cbm_gbuf_free(g_gd_seq_gbuf);
        g_gd_seq_gbuf = NULL;
    }
    if (g_gd_par_gbuf) {
        cbm_gbuf_free(g_gd_par_gbuf);
        g_gd_par_gbuf = NULL;
    }
    teardown_gdscript_repo();
    g_gd_parity_setup_done = 0;
}

static int assert_gd_edge_type_parity(const char *type) {
    if (ensure_gd_parity_setup() != 0)
        return -1;
    int seq = cbm_gbuf_edge_count_by_type(g_gd_seq_gbuf, type);
    int par = cbm_gbuf_edge_count_by_type(g_gd_par_gbuf, type);
    if (seq != par) {
        printf("  FAIL: GDScript %s edges: seq=%d par=%d\n", type, seq, par);
        return 1;
    }
    return 0;
}

TEST(gdscript_parallel_calls_parity) {
    int rc = assert_gd_edge_type_parity("CALLS");
    if (rc == -1)
        SKIP("setup failed");
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(gdscript_parallel_inherits_parity) {
    int rc = assert_gd_edge_type_parity("INHERITS");
    if (rc == -1)
        SKIP("setup failed");
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(gdscript_parallel_imports_parity) {
    int rc = assert_gd_edge_type_parity("IMPORTS");
    if (rc == -1)
        SKIP("setup failed");
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(gdscript_parallel_defines_method_parity) {
    int rc = assert_gd_edge_type_parity("DEFINES_METHOD");
    if (rc == -1)
        SKIP("setup failed");
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(gdscript_parallel_defines_parity) {
    int rc = assert_gd_edge_type_parity("DEFINES");
    if (rc == -1)
        SKIP("setup failed");
    ASSERT_EQ(rc, 0);
    PASS();
}

TEST(gdscript_parallel_node_count) {
    if (ensure_gd_parity_setup() != 0)
        SKIP("setup failed");
    int seq = cbm_gbuf_node_count(g_gd_seq_gbuf);
    int par = cbm_gbuf_node_count(g_gd_par_gbuf);
    ASSERT_GT(seq, 0);
    ASSERT_EQ(seq, par);
    PASS();
}

TEST(gdscript_parallel_total_edges) {
    if (ensure_gd_parity_setup() != 0)
        SKIP("setup failed");
    int seq = cbm_gbuf_edge_count(g_gd_seq_gbuf);
    int par = cbm_gbuf_edge_count(g_gd_par_gbuf);
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

/* ── Suite Registration ──────────────────────────────────────────── */

SUITE(parallel) {
    /* Graph buffer merge/shared-ID tests */
    RUN_TEST(gbuf_shared_ids_unique);
    RUN_TEST(gbuf_merge_nodes);
    RUN_TEST(gbuf_merge_edges);
    RUN_TEST(gbuf_merge_empty_src);
    RUN_TEST(gbuf_merge_src_free_safe);
    RUN_TEST(gbuf_next_id_accessors);

    /* Parallel pipeline parity tests */
    RUN_TEST(parallel_node_count);
    RUN_TEST(parallel_calls_parity);
    RUN_TEST(parallel_defines_parity);
    RUN_TEST(parallel_defines_method_parity);
    RUN_TEST(parallel_imports_parity);
    RUN_TEST(parallel_usage_parity);
    RUN_TEST(parallel_inherits_parity);
    RUN_TEST(parallel_implements_parity);
    RUN_TEST(parallel_total_edges);
    RUN_TEST(parallel_empty_files);

    /* GDScript parallel parity tests */
    RUN_TEST(gdscript_parallel_node_count);
    RUN_TEST(gdscript_parallel_calls_parity);
    RUN_TEST(gdscript_parallel_inherits_parity);
    RUN_TEST(gdscript_parallel_imports_parity);
    RUN_TEST(gdscript_parallel_defines_method_parity);
    RUN_TEST(gdscript_parallel_defines_parity);
    RUN_TEST(gdscript_parallel_total_edges);

    /* Cleanup shared state */
    parity_teardown();
    gd_parity_teardown();
}
