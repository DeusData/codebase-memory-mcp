/*
 * test_graph_mem.c — the in-memory adjacency layer (store/graph_mem.h).
 *
 * The contract is byte-identical output: a read-only query store answers
 * BFS and degree filters from the layer, a writable store of the same file
 * keeps the SQL path, and every row, order, hop, degree, total and flag must
 * match. The fixture mixes a seeded random graph with the shapes that break a
 * naive BFS: ties at one hop, a cycle back to the start, a self-loop, a
 * diamond, duplicate IMPORTS edges, a cross-project edge and edgeless nodes.
 */
#include "test_framework.h"
#include "test_helpers.h"
#include <foundation/compat_thread.h>
#include <foundation/mem.h>
#include <foundation/mem_core.h>
#include <mcp/mcp.h>
#include <store/graph_mem.h>
#include <store/store.h>
#include <sqlite3.h>
#include <stdio.h>
#include <string.h>

enum {
    GMT_NODES = 40,
    GMT_RANDOM_EDGES = 140,
    GMT_THREADS = 4,
    GMT_SEED = 20261014,
    GMT_PATH_MAX = 512,
};

typedef struct {
    char dir[GMT_PATH_MAX];
    char db[GMT_PATH_MAX];
    int64_t ids[GMT_NODES];
    int64_t other_id; /* the cross-project node */
} gmt_fixture_t;

static const char *const gmt_types[] = {"CALLS", "USAGE", "DATA_FLOWS", "IMPORTS"};

static uint32_t gmt_next(uint32_t *state) {
    *state = *state * 1103515245U + 12345U;
    return *state >> 8;
}

static int64_t gmt_node(cbm_store_t *s, const char *project, int i, const char *name) {
    char qn[64];
    char file[32];
    snprintf(qn, sizeof(qn), "%s.mod%d.n%02d", project, i % 5, i);
    snprintf(file, sizeof(file), "src/m%d.c", i % 5);
    cbm_node_t n = {.project = project,
                    .label = (i % 7 == 3) ? "Method" : "Function",
                    .name = name,
                    .qualified_name = qn,
                    .file_path = file,
                    .start_line = i,
                    .end_line = i + 3};
    return cbm_store_upsert_node(s, &n);
}

static void gmt_edge(cbm_store_t *s, int64_t src, int64_t dst, const char *type,
                     const char *props) {
    cbm_edge_t e = {
        .project = "p", .source_id = src, .target_id = dst, .type = type, .properties_json = props};
    (void)cbm_store_insert_edge(s, &e);
}

/* Build the fixture file and stamp a generation, the way a publish does. */
static bool gmt_fixture_build(gmt_fixture_t *f) {
    char *tmp = th_mktempdir("cbm_graph_mem");
    if (!tmp) {
        return false;
    }
    snprintf(f->dir, sizeof(f->dir), "%s", tmp);
    snprintf(f->db, sizeof(f->db), "%s/graph.db", f->dir);
    cbm_store_t *s = cbm_store_open_path(f->db);
    if (!s || cbm_store_upsert_project(s, "p", "/p") != CBM_STORE_OK ||
        cbm_store_upsert_project(s, "q", "/q") != CBM_STORE_OK) {
        cbm_store_close(s);
        return false;
    }
    for (int i = 0; i < GMT_NODES; i++) {
        char name[16];
        /* Every fifth node shares one name: search's (name, id) order has ties. */
        snprintf(name, sizeof(name), (i % 5 == 4) ? "dup" : "n%02d", i);
        f->ids[i] = gmt_node(s, "p", i, name);
    }
    f->other_id = gmt_node(s, "q", 99, "other");
    const int64_t *id = f->ids;
    /* chain with a cycle back to the start (0 at hop 3), a self-loop, a diamond */
    gmt_edge(s, id[0], id[1], "CALLS", NULL);
    gmt_edge(s, id[1], id[2], "CALLS", NULL);
    gmt_edge(s, id[2], id[0], "CALLS", NULL);
    gmt_edge(s, id[2], id[3], "CALLS", NULL);
    gmt_edge(s, id[5], id[5], "CALLS", NULL);
    gmt_edge(s, id[0], id[6], "CALLS", NULL);
    gmt_edge(s, id[0], id[7], "CALLS", NULL);
    gmt_edge(s, id[6], id[8], "CALLS", NULL);
    gmt_edge(s, id[7], id[8], "CALLS", NULL);
    /* two IMPORTS rows for one (source, target): distinct local names */
    gmt_edge(s, id[3], id[4], "IMPORTS", "{\"local_name\":\"a\"}");
    gmt_edge(s, id[3], id[4], "IMPORTS", "{\"local_name\":\"b\"}");
    /* a cross-project hop: BFS does not filter by project */
    gmt_edge(s, id[8], f->other_id, "CALLS", NULL);
    gmt_edge(s, f->other_id, id[9], "CALLS", NULL);
    /* a hub for the degree filters */
    for (int i = 10; i < 30; i++) {
        gmt_edge(s, id[i], id[1], (i % 2) ? "CALLS" : "USAGE", NULL);
    }
    /* seeded random edges over the first 34 nodes; 34..39 stay edgeless */
    uint32_t state = GMT_SEED;
    for (int k = 0; k < GMT_RANDOM_EDGES; k++) {
        int a = (int)(gmt_next(&state) % 34U);
        int b = (int)(gmt_next(&state) % 34U);
        const char *type = gmt_types[gmt_next(&state) % 4U];
        gmt_edge(s, id[a], id[b], type, "{\"w\":1}");
    }
    /* Stamp and seal, as a publish does: a sealed rollback-journal file is
     * the only kind of DB the layer serves. */
    bool ok = cbm_store_generation_advance(s) == CBM_STORE_OK &&
              cbm_store_prepare_for_publish(s) == CBM_STORE_OK;
    cbm_store_close(s);
    return ok;
}

static bool gmt_reseal(const gmt_fixture_t *f) {
    cbm_store_t *w = cbm_store_open_path(f->db);
    bool ok = w && cbm_store_prepare_for_publish(w) == CBM_STORE_OK;
    cbm_store_close(w);
    return ok;
}

static void gmt_fixture_free(gmt_fixture_t *f) {
    cbm_graph_mem_drop_idle();
    th_rmtree(f->dir);
}

static bool gmt_str_eq(const char *a, const char *b) {
    return (a == NULL && b == NULL) || (a && b && strcmp(a, b) == 0);
}

static bool gmt_node_eq(const cbm_node_t *a, const cbm_node_t *b) {
    return a->id == b->id && gmt_str_eq(a->project, b->project) && gmt_str_eq(a->label, b->label) &&
           gmt_str_eq(a->name, b->name) && gmt_str_eq(a->qualified_name, b->qualified_name) &&
           gmt_str_eq(a->file_path, b->file_path) && a->start_line == b->start_line &&
           a->end_line == b->end_line && gmt_str_eq(a->properties_json, b->properties_json);
}

static bool gmt_edge_eq(const cbm_edge_info_t *a, const cbm_edge_info_t *b) {
    return gmt_str_eq(a->from_name, b->from_name) && gmt_str_eq(a->to_name, b->to_name) &&
           gmt_str_eq(a->type, b->type) && a->confidence == b->confidence &&
           a->source_id == b->source_id && a->target_id == b->target_id &&
           gmt_str_eq(a->properties_json, b->properties_json);
}

static bool gmt_traverse_eq(const cbm_traverse_result_t *a, const cbm_traverse_result_t *b) {
    if (!gmt_node_eq(&a->root, &b->root) || a->visited_count != b->visited_count ||
        a->edge_count != b->edge_count || a->truncated != b->truncated ||
        a->edges_truncated != b->edges_truncated) {
        return false;
    }
    for (int i = 0; i < a->visited_count; i++) {
        if (a->visited[i].hop != b->visited[i].hop ||
            !gmt_node_eq(&a->visited[i].node, &b->visited[i].node)) {
            return false;
        }
    }
    for (int i = 0; i < a->edge_count; i++) {
        if (!gmt_edge_eq(&a->edges[i], &b->edges[i])) {
            return false;
        }
    }
    return true;
}

typedef struct {
    const char *types[3];
    int count;
} gmt_typeset_t;

static const gmt_typeset_t gmt_typesets[] = {
    {{NULL}, 0}, /* the default: CALLS */
    {{"CALLS", "USAGE"}, 2},
    {{"DATA_FLOWS"}, 1},
    {{"IMPORTS"}, 1},
    {{"NO_SUCH_TYPE", "CALLS"}, 2},
};

/* One BFS shape on both stores; prints the shape that diverged. */
static bool gmt_bfs_same(cbm_store_t *sql, cbm_store_t *mem, int64_t start, const char *dir,
                         const gmt_typeset_t *ts, int depth, int max_results, int max_edges) {
    cbm_traverse_result_t a = {0};
    cbm_traverse_result_t b = {0};
    const char **types = (const char **)ts->types;
    int rca = cbm_store_bfs_with_edge_limit(sql, start, dir, types, ts->count, depth, max_results,
                                            max_edges, &a);
    int rcb = cbm_store_bfs_with_edge_limit(mem, start, dir, types, ts->count, depth, max_results,
                                            max_edges, &b);
    bool same = rca == rcb && gmt_traverse_eq(&a, &b);
    if (!same) {
        (void)fprintf(stderr,
                      "  graph_mem divergence: start=%lld dir=%s types=%d depth=%d max=%d "
                      "edges=%d rc=%d/%d visited=%d/%d\n",
                      (long long)start, dir, ts->count, depth, max_results, max_edges, rca, rcb,
                      a.visited_count, b.visited_count);
    }
    cbm_store_traverse_free(&a);
    cbm_store_traverse_free(&b);
    return same;
}

/* The runner initializes no memory budget unless the mem suite ran first, and
 * without one no layer is ever built. Each test pins a budget the 3 KB fixture
 * layer fits, collects its observations, and restores the budget BEFORE
 * asserting (the hook's contract: a failed assert must not leak the budget). */
static size_t gmt_budget_pin(size_t bytes) {
    size_t saved = cbm_mem_budget();
    cbm_mem_set_budget_for_tests(bytes);
    return saved;
}

static size_t gmt_budget_roomy(void) {
    return cbm_mem_charged() + ((size_t)1 << 30);
}

static int gmt_visited(cbm_store_t *s, int64_t start) {
    cbm_traverse_result_t tr = {0};
    int n = cbm_store_bfs(s, start, "outbound", NULL, 0, 3, 100, &tr) == CBM_STORE_OK
                ? tr.visited_count
                : -1;
    cbm_store_traverse_free(&tr);
    return n;
}

/* One request: open a query store, walk from `start`, close. */
static int gmt_request(const char *db, int64_t start) {
    cbm_store_t *q = cbm_store_open_path_query(db);
    int n = q ? gmt_visited(q, start) : -1;
    cbm_store_close(q);
    return n;
}

/* The SQL reference: a read-only store like the one under test (a writable
 * open would switch the sealed file to WAL), pinned to the SQL path because
 * its one layer attempt runs under a budget no layer fits. */
static cbm_store_t *gmt_open_sql_reference(const gmt_fixture_t *f) {
    size_t saved = gmt_budget_pin(64);
    cbm_store_t *s = cbm_store_open_path_query(f->db);
    if (s) {
        (void)gmt_visited(s, f->ids[0]);
    }
    cbm_mem_set_budget_for_tests(saved);
    return s;
}

typedef struct {
    int shapes;
    int diverged;
    uint64_t builds;
    bool start_back; /* the cycle 0->1->2->0 returns node 0 at hop 3 */
} gmt_bfs_run_t;

static void gmt_bfs_all_shapes(const gmt_fixture_t *f, cbm_store_t *sql, cbm_store_t *mem,
                               gmt_bfs_run_t *run) {
    static const char *const dirs[] = {"outbound", "inbound"};
    static const int depths[] = {0, 1, 2, 3, 6};
    for (int n = 0; n <= GMT_NODES; n++) {
        int64_t start = n < GMT_NODES ? f->ids[n] : f->other_id;
        for (int d = 0; d < 2; d++) {
            for (size_t t = 0; t < sizeof(gmt_typesets) / sizeof(gmt_typesets[0]); t++) {
                for (size_t k = 0; k < sizeof(depths) / sizeof(depths[0]); k++) {
                    const gmt_typeset_t *ts = &gmt_typesets[t];
                    /* lean, with edge data, and saturated (probe row + edge ceiling) */
                    run->diverged += !gmt_bfs_same(sql, mem, start, dirs[d], ts, depths[k], 100, 0);
                    run->diverged +=
                        !gmt_bfs_same(sql, mem, start, dirs[d], ts, depths[k], 100, 1000);
                    run->diverged += !gmt_bfs_same(sql, mem, start, dirs[d], ts, depths[k], 3, 4);
                    run->shapes += 3;
                }
            }
        }
    }
    cbm_traverse_result_t tr = {0};
    if (cbm_store_bfs(mem, f->ids[0], "outbound", NULL, 0, 3, 100, &tr) == CBM_STORE_OK) {
        for (int i = 0; i < tr.visited_count; i++) {
            if (tr.visited[i].node.id == f->ids[0]) {
                run->start_back = tr.visited[i].hop == 3;
            }
        }
    }
    cbm_store_traverse_free(&tr);
}

TEST(graph_mem_bfs_matches_sql_on_every_shape) {
    gmt_fixture_t f;
    ASSERT_TRUE(gmt_fixture_build(&f));
    uint64_t builds = cbm_graph_mem_build_count();
    cbm_store_t *sql = gmt_open_sql_reference(&f);
    size_t saved = gmt_budget_pin(gmt_budget_roomy());
    cbm_store_t *mem = cbm_store_open_path_query(f.db);
    gmt_bfs_run_t run = {0};
    if (sql && mem) {
        gmt_bfs_all_shapes(&f, sql, mem, &run);
    }
    run.builds = cbm_graph_mem_build_count() - builds;
    cbm_mem_set_budget_for_tests(saved);
    cbm_store_close(sql);
    cbm_store_close(mem);
    gmt_fixture_free(&f);

    ASSERT_GT(run.shapes, 0);
    ASSERT_EQ(run.diverged, 0);
    /* The layer really answered: one build, for the store under test only. */
    ASSERT_EQ(run.builds, 1);
    ASSERT_TRUE(run.start_back);
    PASS();
}

static bool gmt_search_same(cbm_store_t *sql, cbm_store_t *mem, cbm_search_params_t p) {
    cbm_search_output_t a = {0};
    cbm_search_output_t b = {0};
    int rca = cbm_store_search(sql, &p, &a);
    int rcb = cbm_store_search(mem, &p, &b);
    bool same = rca == rcb && a.total == b.total && a.count == b.count;
    for (int i = 0; same && i < a.count; i++) {
        same = gmt_node_eq(&a.results[i].node, &b.results[i].node) &&
               a.results[i].in_degree == b.results[i].in_degree &&
               a.results[i].out_degree == b.results[i].out_degree;
    }
    if (!same) {
        (void)fprintf(stderr,
                      "  graph_mem search divergence: min=%d max=%d limit=%d offset=%d "
                      "total=%d/%d count=%d/%d\n",
                      p.min_degree, p.max_degree, p.limit, p.offset, a.total, b.total, a.count,
                      b.count);
    }
    cbm_store_search_free(&a);
    cbm_store_search_free(&b);
    return same;
}

static int gmt_search_all_shapes(cbm_store_t *sql, cbm_store_t *mem) {
    static const int ranges[][2] = {{1, -1}, {3, -1}, {10, -1}, {0, 0},  {-1, 0},
                                    {-1, 2}, {2, 5},  {0, -1},  {50, -1}};
    static const char *const labels[] = {NULL, "Function"};
    int diverged = 0;
    for (size_t r = 0; r < sizeof(ranges) / sizeof(ranges[0]); r++) {
        for (int l = 0; l < 2; l++) {
            for (int page = 0; page < 3; page++) {
                cbm_search_params_t p = {.project = "p",
                                         .label = labels[l],
                                         .min_degree = ranges[r][0],
                                         .max_degree = ranges[r][1],
                                         .limit = page == 2 ? 100 : 4,
                                         .offset = page == 1 ? 3 : 0};
                diverged += !gmt_search_same(sql, mem, p);
            }
        }
    }
    cbm_search_params_t named = {
        .project = "p", .name_pattern = "dup", .min_degree = 1, .max_degree = -1, .limit = 10};
    diverged += !gmt_search_same(sql, mem, named);
    return diverged;
}

TEST(graph_mem_degree_search_matches_sql) {
    gmt_fixture_t f;
    ASSERT_TRUE(gmt_fixture_build(&f));
    uint64_t builds = cbm_graph_mem_build_count();
    cbm_store_t *sql = gmt_open_sql_reference(&f);
    size_t saved = gmt_budget_pin(gmt_budget_roomy());

    /* No degree filter: the layer is neither needed nor built. */
    cbm_store_t *plain = cbm_store_open_path_query(f.db);
    cbm_search_params_t nofilter = {.project = "p", .min_degree = -1, .max_degree = -1};
    cbm_search_output_t out = {0};
    int plain_rc = plain ? cbm_store_search(plain, &nofilter, &out) : CBM_STORE_ERR;
    cbm_store_search_free(&out);
    cbm_store_close(plain);
    uint64_t plain_builds = cbm_graph_mem_build_count() - builds;

    cbm_store_t *mem = cbm_store_open_path_query(f.db);
    int diverged = sql && mem ? gmt_search_all_shapes(sql, mem) : -1;
    uint64_t filter_builds = cbm_graph_mem_build_count() - builds;
    cbm_mem_set_budget_for_tests(saved);
    cbm_store_close(sql);
    cbm_store_close(mem);
    gmt_fixture_free(&f);

    ASSERT_EQ(plain_rc, CBM_STORE_OK);
    ASSERT_EQ(plain_builds, 0);
    ASSERT_EQ(diverged, 0);
    ASSERT_EQ(filter_builds, 1);
    PASS();
}

TEST(graph_mem_generation_change_invalidates) {
    gmt_fixture_t f;
    ASSERT_TRUE(gmt_fixture_build(&f));
    uint64_t builds = cbm_graph_mem_build_count();
    int64_t leaf = f.ids[36]; /* edgeless */
    size_t saved = gmt_budget_pin(gmt_budget_roomy());

    int first = gmt_request(f.db, leaf);
    uint64_t first_builds = cbm_graph_mem_build_count() - builds;
    /* A later request store on the same version reuses the layer. */
    int second = gmt_request(f.db, leaf);
    uint64_t second_builds = cbm_graph_mem_build_count() - builds;

    /* A mutation stamped with a new generation and sealed: a publish. */
    cbm_store_t *w = cbm_store_open_path(f.db);
    if (w) {
        gmt_edge(w, leaf, f.ids[37], "CALLS", NULL);
    }
    bool published = w && cbm_store_generation_advance(w) == CBM_STORE_OK &&
                     cbm_store_prepare_for_publish(w) == CBM_STORE_OK;
    cbm_store_close(w);

    cbm_store_t *q3 = cbm_store_open_path_query(f.db);
    int third = q3 ? gmt_visited(q3, leaf) : -1;
    cbm_store_t *sql = gmt_open_sql_reference(&f);
    int third_sql = sql ? gmt_visited(sql, leaf) : -1;
    bool same =
        sql && q3 && gmt_bfs_same(sql, q3, f.ids[0], "outbound", &gmt_typesets[1], 6, 100, 1000);
    uint64_t third_builds = cbm_graph_mem_build_count() - builds;
    /* The old version's layer is retired: one layer for this file. */
    int cached = cbm_graph_mem_cached_count();
    cbm_store_close(sql);
    cbm_store_close(q3);
    cbm_mem_set_budget_for_tests(saved);
    gmt_fixture_free(&f);
    int cached_after = cbm_graph_mem_cached_count();

    ASSERT_EQ(first, 0);
    ASSERT_EQ(first_builds, 1);
    ASSERT_EQ(second, 0);
    ASSERT_EQ(second_builds, 1);
    ASSERT_TRUE(published);
    ASSERT_EQ(third, 1);
    ASSERT_EQ(third_sql, 1);
    ASSERT_TRUE(same);
    ASSERT_EQ(third_builds, 2);
    ASSERT_EQ(cached, 1);
    ASSERT_EQ(cached_after, 0);
    PASS();
}

/* The cross-repo shape: edges written straight into a published file, with
 * NO new generation. A layer of the old content must never answer for it —
 * not while the file is in WAL mode, and not after it is sealed again. */
TEST(graph_mem_in_place_write_never_serves_stale) {
    gmt_fixture_t f;
    ASSERT_TRUE(gmt_fixture_build(&f));
    uint64_t builds = cbm_graph_mem_build_count();
    int64_t leaf = f.ids[36]; /* edgeless */
    size_t saved = gmt_budget_pin(gmt_budget_roomy());

    int before = gmt_request(f.db, leaf);
    uint64_t before_builds = cbm_graph_mem_build_count() - builds;

    /* The writer stays open across two commits, so no checkpoint lands
     * between them: the main file — header included — is the same for both.
     * Only refusing WAL mode keeps the second request from a stale answer. */
    cbm_store_t *w = cbm_store_open_path(f.db);
    if (w) {
        gmt_edge(w, leaf, f.ids[37], "CALLS", NULL);
    }
    int wal_one = gmt_request(f.db, leaf);
    if (w) {
        gmt_edge(w, leaf, f.ids[38], "CALLS", NULL);
    }
    int wal_two = gmt_request(f.db, leaf);
    uint64_t wal_builds = cbm_graph_mem_build_count() - builds;
    cbm_store_close(w);

    bool resealed = gmt_reseal(&f);
    int after = gmt_request(f.db, leaf);
    uint64_t after_builds = cbm_graph_mem_build_count() - builds;
    int cached = cbm_graph_mem_cached_count();
    cbm_mem_set_budget_for_tests(saved);
    gmt_fixture_free(&f);

    ASSERT_NOT_NULL(w);
    ASSERT_EQ(before, 0);
    ASSERT_EQ(before_builds, 1);
    /* WAL mode: the SQL path answers, with each new edge. */
    ASSERT_EQ(wal_one, 1);
    ASSERT_EQ(wal_two, 2);
    ASSERT_EQ(wal_builds, 1);
    /* Sealed again under the SAME generation: a new version, a new layer. */
    ASSERT_TRUE(resealed);
    ASSERT_EQ(after, 2);
    ASSERT_EQ(after_builds, 2);
    ASSERT_EQ(cached, 1);
    PASS();
}

TEST(graph_mem_over_budget_keeps_sql_path) {
    gmt_fixture_t f;
    ASSERT_TRUE(gmt_fixture_build(&f));
    uint64_t builds = cbm_graph_mem_build_count();
    cbm_store_t *sql = gmt_open_sql_reference(&f);
    /* A budget whose quarter cannot hold the layer: no build, SQL answers. The
     * file stays sealed, so the budget is the only reason to refuse. */
    size_t saved = gmt_budget_pin(64);
    cbm_store_t *mem = cbm_store_open_path_query(f.db);
    bool same =
        sql && mem && gmt_bfs_same(sql, mem, f.ids[0], "outbound", &gmt_typesets[1], 3, 100, 1000);
    cbm_search_params_t p = {.project = "p", .min_degree = 3, .max_degree = -1, .limit = 50};
    bool same_search = sql && mem && gmt_search_same(sql, mem, p);
    uint64_t builds_after = cbm_graph_mem_build_count();
    int cached = cbm_graph_mem_cached_count();
    cbm_mem_set_budget_for_tests(saved);
    cbm_store_close(sql);
    cbm_store_close(mem);
    gmt_fixture_free(&f);

    ASSERT_TRUE(same);
    ASSERT_TRUE(same_search);
    ASSERT_EQ(builds_after, builds);
    ASSERT_EQ(cached, 0);
    PASS();
}

TEST(graph_mem_legacy_generation_keeps_sql_path) {
    gmt_fixture_t f;
    ASSERT_TRUE(gmt_fixture_build(&f));
    /* A DB that predates generation tracking cannot detect staleness. The raw
     * connection leaves the file in rollback mode: legacy is the only reason
     * to refuse. */
    sqlite3 *raw = NULL;
    ASSERT_EQ(sqlite3_open(f.db, &raw), SQLITE_OK);
    ASSERT_EQ(sqlite3_exec(raw, "DROP TABLE store_meta;", NULL, NULL, NULL), SQLITE_OK);
    sqlite3_close(raw);
    uint64_t builds = cbm_graph_mem_build_count();
    size_t saved = gmt_budget_pin(gmt_budget_roomy());
    int visited = gmt_request(f.db, f.ids[0]);
    uint64_t builds_after = cbm_graph_mem_build_count();
    cbm_mem_set_budget_for_tests(saved);
    gmt_fixture_free(&f);

    ASSERT_GT(visited, 0);
    ASSERT_EQ(builds_after, builds);
    PASS();
}

typedef struct {
    const char *db;
    int64_t start;
    const cbm_traverse_result_t *expect;
    bool same;
} gmt_thread_arg_t;

static void *gmt_thread_main(void *arg) {
    gmt_thread_arg_t *t = arg;
    cbm_store_t *s = cbm_store_open_path_query(t->db);
    cbm_traverse_result_t got = {0};
    t->same = s && cbm_store_bfs(s, t->start, "inbound", NULL, 0, 6, 100, &got) == CBM_STORE_OK &&
              gmt_traverse_eq(t->expect, &got);
    cbm_store_traverse_free(&got);
    cbm_store_close(s);
    return NULL;
}

TEST(graph_mem_concurrent_requests_build_once) {
    gmt_fixture_t f;
    ASSERT_TRUE(gmt_fixture_build(&f));
    uint64_t builds = cbm_graph_mem_build_count();
    cbm_store_t *sql = gmt_open_sql_reference(&f);
    cbm_traverse_result_t expect = {0};
    int expect_rc =
        sql ? cbm_store_bfs(sql, f.ids[1], "inbound", NULL, 0, 6, 100, &expect) : CBM_STORE_ERR;
    size_t saved = gmt_budget_pin(gmt_budget_roomy());
    cbm_thread_t threads[GMT_THREADS];
    gmt_thread_arg_t args[GMT_THREADS];
    int started = 0;
    for (int i = 0; i < GMT_THREADS; i++) {
        args[i] = (gmt_thread_arg_t){.db = f.db, .start = f.ids[1], .expect = &expect};
        started += cbm_thread_create(&threads[i], 0, gmt_thread_main, &args[i]) == 0;
    }
    int joined = 0;
    for (int i = 0; i < started; i++) {
        joined += cbm_thread_join(&threads[i]) == 0;
    }
    uint64_t built = cbm_graph_mem_build_count() - builds;
    cbm_mem_set_budget_for_tests(saved);
    int same = 0;
    for (int i = 0; i < started; i++) {
        same += args[i].same;
    }
    cbm_store_traverse_free(&expect);
    cbm_store_close(sql);
    gmt_fixture_free(&f);

    ASSERT_EQ(expect_rc, CBM_STORE_OK);
    ASSERT_EQ(started, GMT_THREADS);
    ASSERT_EQ(joined, GMT_THREADS);
    ASSERT_EQ(same, GMT_THREADS);
    /* A request meeting another's build takes SQL; nobody builds twice. */
    ASSERT_EQ(built, 1);
    PASS();
}

/* Idle eviction reads this clock through the seam: tests move time instead
 * of waiting for it. */
static uint64_t g_gmt_now_ms;

static uint64_t gmt_fake_clock(void) {
    return g_gmt_now_ms;
}

static void gmt_clock_pin(uint64_t now_ms) {
    g_gmt_now_ms = now_ms;
    cbm_graph_mem_set_clock_for_testing(gmt_fake_clock);
}

TEST(graph_mem_idle_layer_evicted_after_idle_time) {
    gmt_fixture_t f;
    ASSERT_TRUE(gmt_fixture_build(&f));
    uint64_t builds = cbm_graph_mem_build_count();
    size_t saved = gmt_budget_pin(gmt_budget_roomy());
    const uint64_t t0 = 1000000;
    gmt_clock_pin(t0);
    cbm_mem_class_flush_thread();
    size_t class_before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_GRAPH_CACHE);

    int first = gmt_request(f.db, f.ids[0]); /* built and released at t0 */
    cbm_mem_class_flush_thread();
    size_t class_cached = cbm_mem_class_live_bytes(CBM_MEM_CLASS_GRAPH_CACHE);
    g_gmt_now_ms = t0 + CBM_GRAPH_MEM_IDLE_EVICT_MS - 1;
    cbm_graph_mem_evict_expired();
    int cached_just_before = cbm_graph_mem_cached_count();
    g_gmt_now_ms = t0 + CBM_GRAPH_MEM_IDLE_EVICT_MS;
    cbm_graph_mem_evict_expired();
    int cached_at_expiry = cbm_graph_mem_cached_count();
    size_t bytes_at_expiry = cbm_graph_mem_cached_bytes();
    cbm_mem_class_flush_thread();
    size_t class_evicted = cbm_mem_class_live_bytes(CBM_MEM_CLASS_GRAPH_CACHE);
    /* The next walk builds again: eviction is a cache miss, not an error. */
    int again = gmt_request(f.db, f.ids[0]);
    uint64_t built = cbm_graph_mem_build_count() - builds;

    cbm_graph_mem_set_clock_for_testing(NULL);
    cbm_mem_set_budget_for_tests(saved);
    gmt_fixture_free(&f);

    ASSERT_GT(first, 0);
    /* The layer is charged to its own class and given back in full. */
    ASSERT_GT(class_cached, class_before);
    ASSERT_EQ(class_evicted, class_before);
    ASSERT_EQ(cached_just_before, 1);
    ASSERT_EQ(cached_at_expiry, 0);
    ASSERT_EQ(bytes_at_expiry, 0);
    ASSERT_EQ(again, first);
    ASSERT_EQ(built, 2);
    PASS();
}

/* Idle time counts from the LAST release: a layer a request still holds is
 * never evicted, however old its build. */
TEST(graph_mem_held_layer_is_not_idle) {
    gmt_fixture_t f;
    ASSERT_TRUE(gmt_fixture_build(&f));
    size_t saved = gmt_budget_pin(gmt_budget_roomy());
    const uint64_t t0 = 5000000;
    gmt_clock_pin(t0);

    cbm_store_t *held = cbm_store_open_path_query(f.db);
    int walked = held ? gmt_visited(held, f.ids[0]) : -1; /* acquired at t0, still held */
    g_gmt_now_ms = t0 + 3 * (uint64_t)CBM_GRAPH_MEM_IDLE_EVICT_MS;
    cbm_graph_mem_evict_expired();
    int cached_while_held = cbm_graph_mem_cached_count();
    const uint64_t released_at = g_gmt_now_ms;
    cbm_store_close(held); /* released now: idle from here */
    cbm_graph_mem_evict_expired();
    int cached_after_release = cbm_graph_mem_cached_count();
    g_gmt_now_ms = released_at + CBM_GRAPH_MEM_IDLE_EVICT_MS;
    cbm_graph_mem_evict_expired();
    int cached_after_idle = cbm_graph_mem_cached_count();

    cbm_graph_mem_set_clock_for_testing(NULL);
    cbm_mem_set_budget_for_tests(saved);
    gmt_fixture_free(&f);

    ASSERT_GT(walked, 0);
    ASSERT_EQ(cached_while_held, 1);
    ASSERT_EQ(cached_after_release, 1);
    ASSERT_EQ(cached_after_idle, 0);
    PASS();
}

/* The daemon has no timer: the next request is the clock. Any MCP request —
 * here one for a tool that does not exist — sweeps expired layers on its way
 * out, and a walk on another DB sweeps them on its way in. */
TEST(graph_mem_next_request_evicts_expired_layers) {
    gmt_fixture_t f;
    gmt_fixture_t g;
    ASSERT_TRUE(gmt_fixture_build(&f));
    ASSERT_TRUE(gmt_fixture_build(&g));
    size_t saved = gmt_budget_pin(gmt_budget_roomy());
    const uint64_t t0 = 9000000;
    gmt_clock_pin(t0);

    (void)gmt_request(f.db, f.ids[0]);
    g_gmt_now_ms = t0 + CBM_GRAPH_MEM_IDLE_EVICT_MS;
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    char *reply = srv ? cbm_mcp_handle_tool(srv, "no_such_tool", "{}") : NULL;
    int cached_after_mcp = cbm_graph_mem_cached_count();
    free(reply);
    cbm_mcp_server_free(srv);

    (void)gmt_request(f.db, f.ids[0]); /* f rebuilt at t0 + idle */
    g_gmt_now_ms += CBM_GRAPH_MEM_IDLE_EVICT_MS;
    (void)gmt_request(g.db, g.ids[0]); /* acquiring g retires the expired f */
    int cached_after_walk = cbm_graph_mem_cached_count();

    cbm_graph_mem_set_clock_for_testing(NULL);
    cbm_mem_set_budget_for_tests(saved);
    gmt_fixture_free(&f);
    gmt_fixture_free(&g);

    ASSERT_NOT_NULL(srv);
    ASSERT_NOT_NULL(reply);
    ASSERT_EQ(cached_after_mcp, 0);
    ASSERT_EQ(cached_after_walk, 1); /* only g */
    PASS();
}

SUITE(graph_mem) {
    RUN_TEST(graph_mem_bfs_matches_sql_on_every_shape);
    RUN_TEST(graph_mem_degree_search_matches_sql);
    RUN_TEST(graph_mem_generation_change_invalidates);
    RUN_TEST(graph_mem_in_place_write_never_serves_stale);
    RUN_TEST(graph_mem_over_budget_keeps_sql_path);
    RUN_TEST(graph_mem_legacy_generation_keeps_sql_path);
    RUN_TEST(graph_mem_concurrent_requests_build_once);
    RUN_TEST(graph_mem_idle_layer_evicted_after_idle_time);
    RUN_TEST(graph_mem_held_layer_is_not_idle);
    RUN_TEST(graph_mem_next_request_evicts_expired_layers);
}
