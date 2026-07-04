/*
 * test_ui.c — Tests for the graph visualization UI module.
 *
 * Covers: config persistence, embedded asset lookup, layout engine.
 */
#include "../src/foundation/compat.h"
#include "../src/foundation/compat_fs.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "ui/config.h"
#include "ui/embedded_assets.h"
#include "ui/layout3d.h"
#include "store/store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _WIN32
#include <unistd.h>
#include <sys/wait.h>
#endif

/* ── Config tests ─────────────────────────────────────────────── */

TEST(config_load_defaults) {
    /* Loading with no config file should give defaults */
    cbm_ui_config_t cfg;
    cfg.ui_enabled = true; /* set non-default to verify load overwrites */
    cfg.ui_port = 1234;

    /* Use a temp HOME to avoid touching real config */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_test_config_XXXXXX");
    char *td = cbm_mkdtemp(tmpdir);
    ASSERT_NOT_NULL(td);

    char *old_home = getenv("HOME") ? strdup(getenv("HOME")) : NULL;
    cbm_setenv("HOME", td, 1);

    cbm_ui_config_load(&cfg);

    ASSERT_FALSE(cfg.ui_enabled);
    ASSERT_EQ(cfg.ui_port, 9749);

    /* Restore HOME */
    if (old_home) {
        cbm_setenv("HOME", old_home, 1);
        free(old_home);
    }

    PASS();
}

TEST(config_save_and_reload) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_test_config_XXXXXX");
    char *td = cbm_mkdtemp(tmpdir);
    ASSERT_NOT_NULL(td);

    char *old_home = getenv("HOME") ? strdup(getenv("HOME")) : NULL;
    cbm_setenv("HOME", td, 1);

    /* Save */
    cbm_ui_config_t cfg = {.ui_enabled = true, .ui_port = 8080};
    cbm_ui_config_save(&cfg);

    /* Reload */
    cbm_ui_config_t loaded;
    cbm_ui_config_load(&loaded);

    ASSERT_TRUE(loaded.ui_enabled);
    ASSERT_EQ(loaded.ui_port, 8080);

    if (old_home) {
        cbm_setenv("HOME", old_home, 1);
        free(old_home);
    }

    PASS();
}

TEST(config_overwrite) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_test_config_XXXXXX");
    char *td = cbm_mkdtemp(tmpdir);
    ASSERT_NOT_NULL(td);

    char *old_home = getenv("HOME") ? strdup(getenv("HOME")) : NULL;
    cbm_setenv("HOME", td, 1);

    /* Save with ui_enabled=true */
    cbm_ui_config_t cfg1 = {.ui_enabled = true, .ui_port = 9749};
    cbm_ui_config_save(&cfg1);

    /* Overwrite with ui_enabled=false */
    cbm_ui_config_t cfg2 = {.ui_enabled = false, .ui_port = 9749};
    cbm_ui_config_save(&cfg2);

    /* Reload should show false */
    cbm_ui_config_t loaded;
    cbm_ui_config_load(&loaded);
    ASSERT_FALSE(loaded.ui_enabled);

    if (old_home) {
        cbm_setenv("HOME", old_home, 1);
        free(old_home);
    }

    PASS();
}

TEST(config_corrupt_file) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_test_config_XXXXXX");
    char *td = cbm_mkdtemp(tmpdir);
    ASSERT_NOT_NULL(td);

    char *old_home = getenv("HOME") ? strdup(getenv("HOME")) : NULL;
    cbm_setenv("HOME", td, 1);

    /* Write garbage to config path */
    char path[1024];
    cbm_ui_config_path(path, (int)sizeof(path));

    /* Ensure directory exists (portable — no system("mkdir -p")) */
    char dir[1024];
    snprintf(dir, sizeof(dir), "%s/.cache/codebase-memory-mcp", td);
    cbm_mkdir_p(dir, 0755);

    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "this is not json!!!");
    fclose(f);

    /* Should load defaults, not crash */
    cbm_ui_config_t cfg;
    cbm_ui_config_load(&cfg);
    ASSERT_FALSE(cfg.ui_enabled);
    ASSERT_EQ(cfg.ui_port, 9749);

    if (old_home) {
        cbm_setenv("HOME", old_home, 1);
        free(old_home);
    }

    PASS();
}

TEST(config_missing_fields) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_test_config_XXXXXX");
    char *td = cbm_mkdtemp(tmpdir);
    ASSERT_NOT_NULL(td);

    char *old_home = getenv("HOME") ? strdup(getenv("HOME")) : NULL;
    cbm_setenv("HOME", td, 1);

    /* Write JSON with only ui_port */
    char path[1024];
    cbm_ui_config_path(path, (int)sizeof(path));

    char dir[1024];
    snprintf(dir, sizeof(dir), "%s/.cache/codebase-memory-mcp", td);
    cbm_mkdir_p(dir, 0755);

    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "{\"ui_port\": 5555}");
    fclose(f);

    cbm_ui_config_t cfg;
    cbm_ui_config_load(&cfg);
    ASSERT_FALSE(cfg.ui_enabled); /* defaults for missing field */
    ASSERT_EQ(cfg.ui_port, 5555); /* present field loaded */

    if (old_home) {
        cbm_setenv("HOME", old_home, 1);
        free(old_home);
    }

    PASS();
}

/* ── Embedded asset tests ─────────────────────────────────────── */

TEST(embedded_lookup_not_found) {
    /* With stub, everything should return NULL */
    const cbm_embedded_file_t *f = cbm_embedded_lookup("/nonexistent");
    ASSERT_NULL(f);
    PASS();
}

TEST(embedded_stub_count) {
    /* Stub should have 0 files */
    ASSERT_EQ(CBM_EMBEDDED_FILE_COUNT, 0);
    PASS();
}

/* ── Layout tests ─────────────────────────────────────────────── */

TEST(layout_empty_graph) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    /* No nodes in store → empty result */
    cbm_layout_result_t *r =
        cbm_layout_compute(store, "test-project", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->node_count, 0);
    ASSERT_EQ(r->edge_count, 0);

    cbm_layout_free(r);
    cbm_store_close(store);
    PASS();
}

TEST(layout_single_node) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    cbm_store_upsert_project(store, "test", "/tmp/test");
    cbm_node_t node = {
        .project = "test",
        .label = "Function",
        .name = "main",
        .qualified_name = "test::main",
        .file_path = "main.c",
        .start_line = 1,
        .end_line = 10,
    };
    int64_t id = cbm_store_upsert_node(store, &node);
    ASSERT_GT(id, 0);

    cbm_layout_result_t *r = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->node_count, 1);
    ASSERT_STR_EQ(r->nodes[0].name, "main");
    ASSERT_EQ(r->total_nodes, 1);

    cbm_layout_free(r);
    cbm_store_close(store);
    PASS();
}

TEST(layout_two_connected) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    cbm_store_upsert_project(store, "test", "/tmp/test");

    cbm_node_t n1 = {.project = "test",
                     .label = "Function",
                     .name = "foo",
                     .qualified_name = "test::foo",
                     .file_path = "a.c",
                     .start_line = 1,
                     .end_line = 5};
    cbm_node_t n2 = {.project = "test",
                     .label = "Function",
                     .name = "bar",
                     .qualified_name = "test::bar",
                     .file_path = "b.c",
                     .start_line = 1,
                     .end_line = 5};
    int64_t id1 = cbm_store_upsert_node(store, &n1);
    int64_t id2 = cbm_store_upsert_node(store, &n2);

    cbm_edge_t edge = {.project = "test", .source_id = id1, .target_id = id2, .type = "CALLS"};
    cbm_store_insert_edge(store, &edge);

    cbm_layout_result_t *r = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->node_count, 2);

    /* Nodes should be positioned apart (not at same point) */
    float dx = r->nodes[0].x - r->nodes[1].x;
    float dy = r->nodes[0].y - r->nodes[1].y;
    float dz = r->nodes[0].z - r->nodes[1].z;
    float dist = sqrtf(dx * dx + dy * dy + dz * dz);
    ASSERT_GT((long long)(dist * 100), 0);

    ASSERT_EQ(r->edge_count, 1);

    cbm_layout_free(r);
    cbm_store_close(store);
    PASS();
}

TEST(layout_respects_max_nodes) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    cbm_store_upsert_project(store, "test", "/tmp/test");

    /* Insert 20 nodes */
    for (int i = 0; i < 20; i++) {
        char name[32], qn[64];
        snprintf(name, sizeof(name), "fn%d", i);
        snprintf(qn, sizeof(qn), "test::fn%d", i);
        cbm_node_t n = {.project = "test",
                        .label = "Function",
                        .name = name,
                        .qualified_name = qn,
                        .file_path = "a.c",
                        .start_line = i,
                        .end_line = i + 1};
        cbm_store_upsert_node(store, &n);
    }

    /* max_nodes=5 should return at most 5 */
    cbm_layout_result_t *r = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 5);
    ASSERT_NOT_NULL(r);
    ASSERT_LTE(r->node_count, 5);
    ASSERT_EQ(r->total_nodes, 20);

    cbm_layout_free(r);
    cbm_store_close(store);
    PASS();
}

TEST(layout_clamps_render_cap_from_env) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    const char *old_raw = getenv("CBM_UI_MAX_RENDER_NODES");
    char *old_cap = old_raw ? strdup(old_raw) : NULL;
    cbm_setenv("CBM_UI_MAX_RENDER_NODES", "25", 1);

    cbm_store_upsert_project(store, "test", "/tmp/test");

    for (int i = 0; i < 40; i++) {
        char name[32], qn[64];
        snprintf(name, sizeof(name), "fn%d", i);
        snprintf(qn, sizeof(qn), "test::fn%d", i);
        cbm_node_t n = {.project = "test",
                        .label = "Function",
                        .name = name,
                        .qualified_name = qn,
                        .file_path = "a.c",
                        .start_line = i,
                        .end_line = i + 1};
        cbm_store_upsert_node(store, &n);
    }

    cbm_layout_result_t *r = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 50000);
    ASSERT_NOT_NULL(r);
    ASSERT_LTE(r->node_count, 25);
    ASSERT_EQ(r->total_nodes, 40);

    cbm_layout_free(r);
    cbm_store_close(store);
    if (old_cap) {
        cbm_setenv("CBM_UI_MAX_RENDER_NODES", old_cap, 1);
        free(old_cap);
    } else {
        cbm_unsetenv("CBM_UI_MAX_RENDER_NODES");
    }
    PASS();
}

TEST(layout_deterministic) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    cbm_store_upsert_project(store, "test", "/tmp/test");

    cbm_node_t n1 = {.project = "test",
                     .label = "Function",
                     .name = "alpha",
                     .qualified_name = "test::alpha",
                     .file_path = "a.c",
                     .start_line = 1,
                     .end_line = 5};
    cbm_node_t n2 = {.project = "test",
                     .label = "Function",
                     .name = "beta",
                     .qualified_name = "test::beta",
                     .file_path = "b.c",
                     .start_line = 1,
                     .end_line = 5};
    cbm_store_upsert_node(store, &n1);
    cbm_store_upsert_node(store, &n2);

    /* Run twice, check positions match */
    cbm_layout_result_t *r1 = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    cbm_layout_result_t *r2 = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NOT_NULL(r1);
    ASSERT_NOT_NULL(r2);
    ASSERT_EQ(r1->node_count, r2->node_count);

    for (int i = 0; i < r1->node_count; i++) {
        ASSERT_FLOAT_EQ(r1->nodes[i].x, r2->nodes[i].x, 0.001);
        ASSERT_FLOAT_EQ(r1->nodes[i].y, r2->nodes[i].y, 0.001);
        ASSERT_FLOAT_EQ(r1->nodes[i].z, r2->nodes[i].z, 0.001);
    }

    cbm_layout_free(r1);
    cbm_layout_free(r2);
    cbm_store_close(store);
    PASS();
}

TEST(layout_to_json) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    cbm_store_upsert_project(store, "test", "/tmp/test");

    cbm_node_t n = {.project = "test",
                    .label = "Function",
                    .name = "hello",
                    .qualified_name = "test::hello",
                    .file_path = "a.c",
                    .start_line = 1,
                    .end_line = 5};
    cbm_store_upsert_node(store, &n);

    cbm_layout_result_t *r = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NOT_NULL(r);

    char *json = cbm_layout_to_json(r);
    ASSERT_NOT_NULL(json);

    /* Should contain key fields */
    ASSERT(strstr(json, "\"nodes\"") != NULL);
    ASSERT(strstr(json, "\"edges\"") != NULL);
    ASSERT(strstr(json, "\"total_nodes\"") != NULL);
    ASSERT(strstr(json, "\"hello\"") != NULL);
    ASSERT(strstr(json, "\"Function\"") != NULL);

    free(json);
    cbm_layout_free(r);
    cbm_store_close(store);
    PASS();
}

TEST(layout_null_inputs) {
    /* NULL store → NULL result */
    cbm_layout_result_t *r = cbm_layout_compute(NULL, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NULL(r);

    /* NULL project → NULL result */
    cbm_store_t *store = cbm_store_open_memory();
    r = cbm_layout_compute(store, NULL, CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NULL(r);

    /* cbm_layout_free(NULL) should not crash */
    cbm_layout_free(NULL);

    /* cbm_layout_to_json(NULL) should return NULL */
    char *json = cbm_layout_to_json(NULL);
    ASSERT_NULL(json);

    cbm_store_close(store);
    PASS();
}

/* ── Octree recursion guard (distilled from PR #821; refs #498/#726/#402) ── */

/* Bodies that share a position made octree_insert subdivide forever — the
 * cell around them shrinks but never separates them, so one octree cell is
 * calloc'd per level until the process dies (stack overflow) or freezes the
 * machine allocating (the 34GB-swap reports). Fixed by the depth/half-size
 * floor in src/ui/layout3d.c (OCTREE_MAX_DEPTH / OCTREE_MIN_HALF).
 *
 * Coincident positions are reachable through the public layout API: layout3d
 * anchors each node by fnv1a(file cluster key) and jitters it with a PRNG
 * seeded by fnv1a(qualified_name). The three QNs below are distinct strings
 * with IDENTICAL 32-bit FNV-1a hashes (0x06bb012e, found by offline brute
 * force), so in the same file they get bit-identical positions on every
 * platform (integer hashing only — no libm in the coincidence path).
 *
 * A literal sub-ULP-separated pair cannot be constructed through the public
 * API: same-anchor positions are quantized to exact multiples of the jitter
 * quantum (5/4096 — exactly 20 ULP at anchor magnitude ~600), and
 * cross-anchor separations depend on the platform's cosf/sinf bits. Exact
 * coincidence is the API-reachable degenerate input, and it necessarily
 * drives the recursion through the sub-ULP regime: half_size falls below
 * ULP(center) with the bodies still unseparated, freezing child centers
 * while cells keep being allocated.
 */
#if !defined(_WIN32)
/* Child body: builds the store and runs the layout so a crash or hang cannot
 * take down the runner (alarm bounds a hang, fork isolates a SIGSEGV).
 * Deliberately NO memory rlimit: under a rlimit a failing calloc makes
 * octree_insert silently truncate and the UNFIXED code would complete —
 * turning this guard vacuously green. The alarm alone bounds the runaway.
 * Exit codes: 0 ok, 2 store setup, 3 layout NULL, 4 node count/lookup,
 * 5 fixture no longer coincident, 6 non-finite coordinate. Never returns. */
static void layout_octree_guard_child(void) {
    alarm(5); /* post-fix the whole child runs in milliseconds */
    cbm_store_t *store = cbm_store_open_memory();
    if (!store)
        _exit(2);
    if (cbm_store_upsert_project(store, "test", "/tmp/test") != CBM_STORE_OK)
        _exit(2);

    /* Distinct QNs, one fnv1a hash — coincident after anchor + jitter. */
    static const char *cqn[3] = {"test::octree_c5988474", "test::octree_c11394919",
                                 "test::octree_c33141700"};
    for (int i = 0; i < 3; i++) {
        char name[32];
        snprintf(name, sizeof(name), "co%d", i);
        cbm_node_t n = {.project = "test",
                        .label = "Function",
                        .name = name,
                        .qualified_name = cqn[i],
                        .file_path = "pkg/sub/mod/a.c",
                        .start_line = i + 1,
                        .end_line = i + 2};
        if (cbm_store_upsert_node(store, &n) <= 0)
            _exit(2);
    }
    /* A few normally-spread nodes so the octree root box has realistic
     * (non-degenerate) extent, as in the reported repositories. */
    for (int i = 0; i < 3; i++) {
        char name[32], qn[64], fp[32];
        snprintf(name, sizeof(name), "fn%d", i);
        snprintf(qn, sizeof(qn), "test::spread_fn%d", i);
        snprintf(fp, sizeof(fp), "dir%d/f%d.c", i, i);
        cbm_node_t n = {.project = "test",
                        .label = "Function",
                        .name = name,
                        .qualified_name = qn,
                        .file_path = fp,
                        .start_line = 1,
                        .end_line = 2};
        if (cbm_store_upsert_node(store, &n) <= 0)
            _exit(2);
    }

    cbm_layout_result_t *r = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    if (!r)
        _exit(3);
    if (r->node_count != 6)
        _exit(4);

    /* The colliding QNs must actually be coincident — identical output
     * coordinates (identical seeds → identical positions, and coincident
     * bodies receive identical forces every iteration, so they stay
     * together). If a seeding change ever breaks this, the fixture no longer
     * reproduces the bug: fail loudly instead of going vacuously green. */
    int ci[3], nc = 0;
    for (int i = 0; i < r->node_count && nc < 3; i++) {
        if (r->nodes[i].qualified_name &&
            strncmp(r->nodes[i].qualified_name, "test::octree_c", 14) == 0)
            ci[nc++] = i;
    }
    if (nc != 3)
        _exit(4);
    for (int k = 1; k < 3; k++) {
        if (r->nodes[ci[k]].x != r->nodes[ci[0]].x || r->nodes[ci[k]].y != r->nodes[ci[0]].y ||
            r->nodes[ci[k]].z != r->nodes[ci[0]].z)
            _exit(5);
    }
    for (int i = 0; i < r->node_count; i++) {
        if (!isfinite(r->nodes[i].x) || !isfinite(r->nodes[i].y) || !isfinite(r->nodes[i].z))
            _exit(6);
    }

    cbm_layout_free(r);
    cbm_store_close(store);
    _exit(0);
}
#endif

TEST(layout_coincident_nodes_bounded) {
#if defined(_WIN32)
    SKIP_PLATFORM("fork/alarm not available; POSIX-only bounded-hang reproduction");
#else
    fflush(NULL);
    pid_t pid = fork();
    if (pid < 0)
        FAIL("fork() failed");
    if (pid == 0)
        layout_octree_guard_child(); /* never returns */

    int status = 0;
    (void)waitpid(pid, &status, 0);

    /* Unfixed code dies here: SIGSEGV (unbounded recursion overflowing the
     * stack) or SIGALRM (tail-call-optimized allocation runaway cut off by
     * the child's alarm). Fixed code exits 0 well within the budget. */
    ASSERT_FALSE(WIFSIGNALED(status));
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
    PASS();
#endif
}

/* ── Suite ────────────────────────────────────────────────────── */

SUITE(ui) {
    /* Config */
    RUN_TEST(config_load_defaults);
    RUN_TEST(config_save_and_reload);
    RUN_TEST(config_overwrite);
    RUN_TEST(config_corrupt_file);
    RUN_TEST(config_missing_fields);

    /* Embedded assets (stub) */
    RUN_TEST(embedded_lookup_not_found);
    RUN_TEST(embedded_stub_count);

    /* Layout engine */
    RUN_TEST(layout_empty_graph);
    RUN_TEST(layout_single_node);
    RUN_TEST(layout_two_connected);
    RUN_TEST(layout_respects_max_nodes);
    RUN_TEST(layout_clamps_render_cap_from_env);
    RUN_TEST(layout_deterministic);
    RUN_TEST(layout_to_json);
    RUN_TEST(layout_null_inputs);
    RUN_TEST(layout_coincident_nodes_bounded);
}
