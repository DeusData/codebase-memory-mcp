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

#define GD_PARITY_PROJECT "gd-par-test"

typedef enum {
    CBM_PIPELINE_MODE_NONE = 0,
    CBM_PIPELINE_MODE_AUTO,
    CBM_PIPELINE_MODE_SEQUENTIAL,
    CBM_PIPELINE_MODE_PARALLEL,
} cbm_pipeline_mode;

enum {
    CBM_PIPELINE_FORCE_PARALLEL_UNAVAILABLE = 2,
};

typedef struct {
    cbm_pipeline_mode requested_mode;
    cbm_pipeline_mode selected_mode;
    int worker_count;
    int file_count;
    bool forced;
    bool invalid_override;
} cbm_pipeline_mode_selection_t;

int cbm_pipeline_select_mode(const char *requested_mode, int worker_count, int file_count,
                             cbm_pipeline_mode_selection_t *selection);

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

static void seed_gbuf_file_nodes(cbm_gbuf_t *gbuf, const cbm_file_info_t *files, int file_count,
                                const char *project_name) {
    for (int i = 0; i < file_count; i++) {
        const char *rel = files[i].rel_path;
        if (!rel)
            continue;

        char *file_qn = cbm_pipeline_fqn_compute(project_name, rel, "__file__");
        if (!file_qn)
            continue;

        const char *slash = strrchr(rel, '/');
        const char *basename = slash ? slash + SKIP_ONE : rel;
        const char *ext = strrchr(basename, '.');

        char props[CBM_SZ_128];
        snprintf(props, sizeof(props), "{\"extension\":\"%s\"}", ext ? ext : "");
        cbm_gbuf_upsert_node(gbuf, "File", basename, file_qn, rel, 0, 0, props);
        free(file_qn);
    }
}

static cbm_gbuf_t *run_sequential(const char *project, const char *repo_path,
                                  cbm_file_info_t *files, int file_count) {
    cbm_gbuf_t *gbuf = cbm_gbuf_new(project, repo_path);
    seed_gbuf_file_nodes(gbuf, files, file_count, project);
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
    seed_gbuf_file_nodes(gbuf, files, file_count, project);
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

TEST(pipeline_mode_select_auto_rule) {
    cbm_pipeline_mode_selection_t selection = {0};

    ASSERT_EQ(cbm_pipeline_select_mode(NULL, 2, 51, &selection), 0);
    ASSERT_EQ(selection.requested_mode, CBM_PIPELINE_MODE_AUTO);
    ASSERT_EQ(selection.selected_mode, CBM_PIPELINE_MODE_PARALLEL);
    ASSERT_FALSE(selection.forced);
    ASSERT_FALSE(selection.invalid_override);

    ASSERT_EQ(cbm_pipeline_select_mode("auto", 2, 50, &selection), 0);
    ASSERT_EQ(selection.requested_mode, CBM_PIPELINE_MODE_AUTO);
    ASSERT_EQ(selection.selected_mode, CBM_PIPELINE_MODE_SEQUENTIAL);
    ASSERT_FALSE(selection.forced);
    ASSERT_FALSE(selection.invalid_override);
    PASS();
}

TEST(pipeline_mode_select_forced_sequential) {
    cbm_pipeline_mode_selection_t selection = {0};

    ASSERT_EQ(cbm_pipeline_select_mode("sequential", 4, 500, &selection), 0);
    ASSERT_EQ(selection.requested_mode, CBM_PIPELINE_MODE_SEQUENTIAL);
    ASSERT_EQ(selection.selected_mode, CBM_PIPELINE_MODE_SEQUENTIAL);
    ASSERT_TRUE(selection.forced);
    ASSERT_FALSE(selection.invalid_override);
    PASS();
}

TEST(pipeline_mode_select_forced_parallel_requires_workers) {
    cbm_pipeline_mode_selection_t selection = {0};

    ASSERT_EQ(cbm_pipeline_select_mode("parallel", 1, 5, &selection),
              CBM_PIPELINE_FORCE_PARALLEL_UNAVAILABLE);
    ASSERT_EQ(selection.requested_mode, CBM_PIPELINE_MODE_PARALLEL);
    ASSERT_EQ(selection.selected_mode, CBM_PIPELINE_MODE_NONE);
    ASSERT_TRUE(selection.forced);
    ASSERT_FALSE(selection.invalid_override);
    PASS();
}

TEST(pipeline_mode_select_invalid_override_defaults_auto) {
    cbm_pipeline_mode_selection_t selection = {0};

    ASSERT_EQ(cbm_pipeline_select_mode("bogus", 2, 51, &selection), 0);
    ASSERT_EQ(selection.requested_mode, CBM_PIPELINE_MODE_AUTO);
    ASSERT_EQ(selection.selected_mode, CBM_PIPELINE_MODE_PARALLEL);
    ASSERT_FALSE(selection.forced);
    ASSERT_TRUE(selection.invalid_override);
    PASS();
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
        "    pass\n"
        "\n"
        "func shadow_param(hit):\n"
        "    hit.connect(_on_hit)\n"
        "\n"
        "func _on_hit():\n"
        "    pass\n"
        "\n"
        "func _on_receiver_hit():\n"
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

    /* addon fixture for nested relative preload regression */
    snprintf(path, sizeof(path), "%s/addons", g_gd_tmpdir);
    cbm_mkdir(path);

    snprintf(path, sizeof(path), "%s/addons/simple_import_plugin", g_gd_tmpdir);
    cbm_mkdir(path);

    snprintf(path, sizeof(path), "%s/addons/simple_import_plugin/plugin.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f)
        return -1;
    fprintf(f,
            "extends EditorPlugin\n"
            "func _enter_tree():\n"
            "    add_child(preload(\"import.gd\").new())\n");
    fclose(f);

    snprintf(path, sizeof(path), "%s/addons/simple_import_plugin/import.gd", g_gd_tmpdir);
    f = fopen(path, "w");
    if (!f)
        return -1;
    fprintf(f, "class_name ImportScript\nfunc hello():\n    pass\n");
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

static int gdbuf_edge_signature(const cbm_gbuf_t *gbuf, const cbm_gbuf_edge_t *edge, int target_file_key,
                               char *out, size_t out_size) {
    const cbm_gbuf_node_t *source = cbm_gbuf_find_by_id(gbuf, edge->source_id);
    const cbm_gbuf_node_t *target = cbm_gbuf_find_by_id(gbuf, edge->target_id);
    if (!source || !target)
        return -1;

    const char *source_key = source->qualified_name && source->qualified_name[0]
                                 ? source->qualified_name
                                 : (source->file_path ? source->file_path : "");

    const char *target_key = NULL;
    if (target_file_key) {
        target_key = target->file_path && target->file_path[0] ? target->file_path : target->qualified_name;
    } else {
        target_key = target->qualified_name;
    }

    if (!target_key)
        target_key = "";

    snprintf(out, out_size, "%s|%s", source_key, target_key);
    return 0;
}

static int gdbuf_count_matching_signatures(const cbm_gbuf_t *gbuf, const cbm_gbuf_edge_t **edges,
                                          int edge_count, int target_file_key,
                                          const char *signature) {
    int matches = 0;
    char edge_sig[1024];
    for (int i = 0; i < edge_count; i++) {
        if (!edges[i])
            continue;
        if (gdbuf_edge_signature(gbuf, edges[i], target_file_key, edge_sig, sizeof(edge_sig)) != 0)
            continue;
        if (strcmp(edge_sig, signature) == 0)
            matches++;
    }
    return matches;
}

static int signature_seen(const char **seen, int seen_count, const char *signature) {
    for (int i = 0; i < seen_count; i++) {
        if (seen[i] && strcmp(seen[i], signature) == 0)
            return 1;
    }
    return 0;
}

static int assert_gd_edge_signature_parity(const char *type, int target_file_key) {
    if (ensure_gd_parity_setup() != 0)
        return -1;

    const cbm_gbuf_edge_t **seq_edges = NULL;
    const cbm_gbuf_edge_t **par_edges = NULL;
    int seq_count = 0;
    int par_count = 0;

    if (cbm_gbuf_find_edges_by_type(g_gd_seq_gbuf, type, &seq_edges, &seq_count) != 0)
        return -1;
    if (cbm_gbuf_find_edges_by_type(g_gd_par_gbuf, type, &par_edges, &par_count) != 0)
        return -1;

    if (seq_count != par_count) {
        printf("  FAIL: GDScript %s edges: seq=%d par=%d\n", type, seq_count, par_count);
        return 1;
    }

    char **seen = calloc((size_t)(seq_count > 0 ? seq_count : 1), sizeof(char *));
    if (!seen && seq_count > 0)
        return -1;
    int seen_count = 0;

    for (int i = 0; i < seq_count; i++) {
        if (!seq_edges[i])
            continue;

        char sig[1024];
        if (gdbuf_edge_signature(g_gd_seq_gbuf, seq_edges[i], target_file_key, sig, sizeof(sig)) != 0) {
            for (int j = 0; j < seen_count; j++)
                free(seen[j]);
            free(seen);
            return 1;
        }

        if (signature_seen((const char **)seen, seen_count, sig))
            continue;

        int seq_seen = gdbuf_count_matching_signatures(g_gd_seq_gbuf, seq_edges, seq_count,
                                                     target_file_key, sig);
        int par_seen = gdbuf_count_matching_signatures(g_gd_par_gbuf, par_edges, par_count,
                                                     target_file_key, sig);
        if (seq_seen != par_seen) {
            printf("  FAIL: GDScript %s signature mismatch: %s seq=%d par=%d\n", type, sig,
                   seq_seen, par_seen);
            for (int j = 0; j < seen_count; j++)
                free(seen[j]);
            free(seen);
            return 1;
        }

        seen[seen_count++] = strdup(sig);
    }

    for (int i = 0; i < seen_count; i++)
        free(seen[i]);
    free(seen);
    return 0;
}

static int assert_gd_edge_type_parity(const char *type) {
    return assert_gd_edge_signature_parity(type, 0);
}

static int count_import_edge(const cbm_gbuf_t *gbuf, const char *source_rel, const char *target_rel) {
    char source_qn[512];
    char *computed_source_qn = cbm_pipeline_fqn_compute(GD_PARITY_PROJECT, source_rel, "__file__");
    if (!computed_source_qn)
        return -1;

    snprintf(source_qn, sizeof(source_qn), "%s", computed_source_qn);
    free(computed_source_qn);

    const cbm_gbuf_node_t *source = cbm_gbuf_find_by_qn(gbuf, source_qn);
    if (!source) {
        return -1;
    }

    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    if (cbm_gbuf_find_edges_by_source_type(gbuf, source->id, "IMPORTS", &edges, &edge_count) != 0) {
        return -1;
    }

    int count = 0;
    for (int i = 0; i < edge_count; i++) {
        if (!edges[i])
            continue;
        const cbm_gbuf_node_t *target = cbm_gbuf_find_by_id(gbuf, edges[i]->target_id);
        if (target && target->file_path && strcmp(target->file_path, target_rel) == 0) {
            count++;
        }
    }

    return count;
}

static int gdbuf_count_edges_to_target_qn(const cbm_gbuf_t *gbuf, const char *source_qn,
                                         const char *type, const char *target_qn) {
    const cbm_gbuf_node_t *source = cbm_gbuf_find_by_qn(gbuf, source_qn);
    if (!source)
        return -1;

    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    if (cbm_gbuf_find_edges_by_source_type(gbuf, source->id, type, &edges, &edge_count) != 0) {
        return -1;
    }

    int count = 0;
    for (int i = 0; i < edge_count; i++) {
        if (!edges[i])
            continue;
        const cbm_gbuf_node_t *target = cbm_gbuf_find_by_id(gbuf, edges[i]->target_id);
        if (target && target->qualified_name && strcmp(target->qualified_name, target_qn) == 0) {
            count++;
        }
    }

    return count;
}

static int gdbuf_count_edges_of_type(const cbm_gbuf_t *gbuf, const char *source_qn,
                                    const char *type) {
    const cbm_gbuf_node_t *source = cbm_gbuf_find_by_qn(gbuf, source_qn);
    if (!source)
        return -1;

    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    if (cbm_gbuf_find_edges_by_source_type(gbuf, source->id, type, &edges, &edge_count) != 0) {
        return -1;
    }

    return edge_count;
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
    int rc = assert_gd_edge_signature_parity("IMPORTS", 1);
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

TEST(gdscript_parallel_imports_nested_preload_relative_path_regression) {
    if (ensure_gd_parity_setup() != 0)
        SKIP("setup failed");

    int seq = count_import_edge(g_gd_seq_gbuf, "addons/simple_import_plugin/plugin.gd",
                               "addons/simple_import_plugin/import.gd");
    int par = count_import_edge(g_gd_par_gbuf, "addons/simple_import_plugin/plugin.gd",
                               "addons/simple_import_plugin/import.gd");

    ASSERT_GT(seq, 0);
    ASSERT_EQ(seq, par);
    PASS();
}

TEST(gdscript_parallel_calls_same_script_helper) {
    if (ensure_gd_parity_setup() != 0)
        SKIP("setup failed");

    char player_qn[512], attack_qn[512], helper_qn[512];
    snprintf(player_qn, sizeof(player_qn), "%s.actors.player.Player", GD_PARITY_PROJECT);
    snprintf(attack_qn, sizeof(attack_qn), "%s.attack", player_qn);
    snprintf(helper_qn, sizeof(helper_qn), "%s.helper", player_qn);

    int seq = gdbuf_count_edges_to_target_qn(g_gd_seq_gbuf, attack_qn, "CALLS", helper_qn);
    int par = gdbuf_count_edges_to_target_qn(g_gd_par_gbuf, attack_qn, "CALLS", helper_qn);

    ASSERT_GTE(seq, 0);
    ASSERT_GTE(par, 0);
    ASSERT_EQ(seq, 1);
    ASSERT_EQ(seq, par);
    PASS();
}

TEST(gdscript_parallel_calls_signal_targets_player_receiver) {
    if (ensure_gd_parity_setup() != 0)
        SKIP("setup failed");

    char player_qn[512], attack_qn[512], shadow_qn[512], helper_qn[512], player_signal_qn[512],
        receiver_signal_qn[512];
    char nameless_qn[512], ghost_attack_qn[512], ghost_signal_qn[512];

    snprintf(player_qn, sizeof(player_qn), "%s.actors.player.Player", GD_PARITY_PROJECT);
    snprintf(attack_qn, sizeof(attack_qn), "%s.attack", player_qn);
    snprintf(shadow_qn, sizeof(shadow_qn), "%s.shadow_param", player_qn);
    snprintf(helper_qn, sizeof(helper_qn), "%s.helper", player_qn);
    snprintf(player_signal_qn, sizeof(player_signal_qn), "%s.signal.hit", player_qn);
    snprintf(receiver_signal_qn, sizeof(receiver_signal_qn), "%s.actors.receiver.Receiver.signal.hit",
             GD_PARITY_PROJECT);
    snprintf(nameless_qn, sizeof(nameless_qn), "%s.actors.nameless_script.__script__",
             GD_PARITY_PROJECT);
    snprintf(ghost_attack_qn, sizeof(ghost_attack_qn), "%s.ghost_attack", nameless_qn);
    snprintf(ghost_signal_qn, sizeof(ghost_signal_qn), "%s.signal.ghost_hit", nameless_qn);

    int seq_attack_player_signal = gdbuf_count_edges_to_target_qn(g_gd_seq_gbuf, attack_qn, "CALLS",
                                                                 player_signal_qn);
    int par_attack_player_signal = gdbuf_count_edges_to_target_qn(g_gd_par_gbuf, attack_qn, "CALLS",
                                                                player_signal_qn);
    int seq_attack_receiver_signal = gdbuf_count_edges_to_target_qn(g_gd_seq_gbuf, attack_qn, "CALLS",
                                                                  receiver_signal_qn);
    int par_attack_receiver_signal = gdbuf_count_edges_to_target_qn(g_gd_par_gbuf, attack_qn, "CALLS",
                                                                 receiver_signal_qn);
    int seq_attack_helper = gdbuf_count_edges_to_target_qn(g_gd_seq_gbuf, attack_qn, "CALLS", helper_qn);
    int par_attack_helper = gdbuf_count_edges_to_target_qn(g_gd_par_gbuf, attack_qn, "CALLS", helper_qn);
    int seq_shadow_player_signal = gdbuf_count_edges_to_target_qn(g_gd_seq_gbuf, shadow_qn, "CALLS",
                                                                player_signal_qn);
    int par_shadow_player_signal = gdbuf_count_edges_to_target_qn(g_gd_par_gbuf, shadow_qn, "CALLS",
                                                                player_signal_qn);
    int seq_ghost_attack_signal = gdbuf_count_edges_to_target_qn(g_gd_seq_gbuf, ghost_attack_qn, "CALLS",
                                                                ghost_signal_qn);
    int par_ghost_attack_signal = gdbuf_count_edges_to_target_qn(g_gd_par_gbuf, ghost_attack_qn, "CALLS",
                                                                ghost_signal_qn);

    ASSERT_GTE(seq_attack_player_signal, 0);
    ASSERT_GTE(par_attack_player_signal, 0);
    ASSERT_GTE(seq_attack_receiver_signal, 0);
    ASSERT_GTE(par_attack_receiver_signal, 0);
    ASSERT_GTE(seq_attack_helper, 0);
    ASSERT_GTE(par_attack_helper, 0);
    ASSERT_GTE(seq_shadow_player_signal, 0);
    ASSERT_GTE(par_shadow_player_signal, 0);
    ASSERT_GTE(seq_ghost_attack_signal, 0);
    ASSERT_GTE(par_ghost_attack_signal, 0);

    ASSERT_EQ(seq_attack_player_signal, 1);
    ASSERT_EQ(seq_attack_receiver_signal, 1);
    ASSERT_EQ(seq_attack_helper, 1);
    ASSERT_EQ(seq_ghost_attack_signal, 1);
    ASSERT_EQ(seq_shadow_player_signal, 0);

    ASSERT_EQ(seq_attack_player_signal, par_attack_player_signal);
    ASSERT_EQ(seq_attack_receiver_signal, par_attack_receiver_signal);
    ASSERT_EQ(seq_attack_helper, par_attack_helper);
    ASSERT_EQ(seq_ghost_attack_signal, par_ghost_attack_signal);
    ASSERT_EQ(seq_shadow_player_signal, par_shadow_player_signal);

    PASS();
}

TEST(gdscript_parallel_nameless_script_anchor_parity) {
    if (ensure_gd_parity_setup() != 0)
        SKIP("setup failed");

    char nameless_qn[512];
    snprintf(nameless_qn, sizeof(nameless_qn), "%s.actors.nameless_script.__script__",
             GD_PARITY_PROJECT);
    const cbm_gbuf_node_t *nameless_seq = cbm_gbuf_find_by_qn(g_gd_seq_gbuf, nameless_qn);
    const cbm_gbuf_node_t *nameless_par = cbm_gbuf_find_by_qn(g_gd_par_gbuf, nameless_qn);

    ASSERT_NOT_NULL(nameless_seq);
    ASSERT_NOT_NULL(nameless_par);

    ASSERT_TRUE(strstr(nameless_qn, "__script__") != NULL);

    char ghost_attack_qn[512], ghost_signal_qn[512];
    snprintf(ghost_attack_qn, sizeof(ghost_attack_qn), "%s.ghost_attack", nameless_qn);
    snprintf(ghost_signal_qn, sizeof(ghost_signal_qn), "%s.signal.ghost_hit", nameless_qn);

    int seq = gdbuf_count_edges_to_target_qn(g_gd_seq_gbuf, ghost_attack_qn, "CALLS",
                                            ghost_signal_qn);
    int par = gdbuf_count_edges_to_target_qn(g_gd_par_gbuf, ghost_attack_qn, "CALLS",
                                            ghost_signal_qn);

    ASSERT_GTE(seq, 0);
    ASSERT_GTE(par, 0);
    int seq_defines_method = gdbuf_count_edges_to_target_qn(g_gd_seq_gbuf, nameless_qn, "DEFINES_METHOD",
                                                          ghost_attack_qn);
    int par_defines_method = gdbuf_count_edges_to_target_qn(g_gd_par_gbuf, nameless_qn, "DEFINES_METHOD",
                                                          ghost_attack_qn);

    ASSERT_GTE(seq_defines_method, 0);
    ASSERT_GTE(par_defines_method, 0);
    ASSERT_EQ(seq_defines_method, 1);
    ASSERT_EQ(par_defines_method, 1);
    ASSERT_EQ(seq_defines_method, par_defines_method);
    ASSERT_EQ(seq, 1);
    ASSERT_EQ(par, 1);
    ASSERT_EQ(seq, par);
    PASS();
}

TEST(gdscript_parallel_dynamic_receiver_unresolved_parity) {
    if (ensure_gd_parity_setup() != 0)
        SKIP("setup failed");

    char dynamic_qn[512], attack_dynamic_qn[512];
    snprintf(dynamic_qn, sizeof(dynamic_qn), "%s.actors.dynamic_receiver.DynamicReceiverCase",
             GD_PARITY_PROJECT);
    snprintf(attack_dynamic_qn, sizeof(attack_dynamic_qn), "%s.attack_dynamic", dynamic_qn);

    int seq = gdbuf_count_edges_of_type(g_gd_seq_gbuf, attack_dynamic_qn, "CALLS");
    int par = gdbuf_count_edges_of_type(g_gd_par_gbuf, attack_dynamic_qn, "CALLS");

    ASSERT_GTE(seq, 0);
    ASSERT_GTE(par, 0);
    ASSERT_EQ(seq, 0);
    ASSERT_EQ(par, 0);
    ASSERT_EQ(seq, par);
    PASS();
}

TEST(gdscript_parallel_builtin_base_no_inherits_metadata) {
    if (ensure_gd_parity_setup() != 0)
        SKIP("setup failed");

    char builtin_qn[512];
    snprintf(builtin_qn, sizeof(builtin_qn), "%s.actors.builtin_base.BuiltinBaseCase",
             GD_PARITY_PROJECT);
    const cbm_gbuf_node_t *seq_builtin = cbm_gbuf_find_by_qn(g_gd_seq_gbuf, builtin_qn);
    const cbm_gbuf_node_t *par_builtin = cbm_gbuf_find_by_qn(g_gd_par_gbuf, builtin_qn);

    ASSERT_NOT_NULL(seq_builtin);
    ASSERT_NOT_NULL(par_builtin);
    ASSERT_NOT_NULL(seq_builtin->properties_json);
    ASSERT_NOT_NULL(par_builtin->properties_json);
    ASSERT_TRUE(strstr(seq_builtin->properties_json, "\"base_classes\"") != NULL);
    ASSERT_TRUE(strstr(par_builtin->properties_json, "\"base_classes\"") != NULL);
    ASSERT_TRUE(strstr(seq_builtin->properties_json, "\"Node2D\"") != NULL);
    ASSERT_TRUE(strstr(par_builtin->properties_json, "\"Node2D\"") != NULL);

    int seq_inherits = gdbuf_count_edges_of_type(g_gd_seq_gbuf, builtin_qn, "INHERITS");
    int par_inherits = gdbuf_count_edges_of_type(g_gd_par_gbuf, builtin_qn, "INHERITS");
    ASSERT_GTE(seq_inherits, 0);
    ASSERT_GTE(par_inherits, 0);
    ASSERT_EQ(seq_inherits, 0);
    ASSERT_EQ(par_inherits, 0);
    ASSERT_EQ(seq_inherits, par_inherits);
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

    RUN_TEST(pipeline_mode_select_auto_rule);
    RUN_TEST(pipeline_mode_select_forced_sequential);
    RUN_TEST(pipeline_mode_select_forced_parallel_requires_workers);
    RUN_TEST(pipeline_mode_select_invalid_override_defaults_auto);

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
    RUN_TEST(gdscript_parallel_imports_nested_preload_relative_path_regression);
    RUN_TEST(gdscript_parallel_calls_same_script_helper);
    RUN_TEST(gdscript_parallel_calls_signal_targets_player_receiver);
    RUN_TEST(gdscript_parallel_nameless_script_anchor_parity);
    RUN_TEST(gdscript_parallel_dynamic_receiver_unresolved_parity);
    RUN_TEST(gdscript_parallel_builtin_base_no_inherits_metadata);
    RUN_TEST(gdscript_parallel_defines_method_parity);
    RUN_TEST(gdscript_parallel_defines_parity);
    RUN_TEST(gdscript_parallel_total_edges);

    /* Cleanup shared state */
    parity_teardown();
    gd_parity_teardown();
}
