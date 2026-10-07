/*
 * test_store_impact.c — the fixpoint impact walk (src/store/store_impact.c).
 *
 * The walk answers "what depends on these nodes" for test selection, so its
 * result must be a pure function of the graph and the request: complete (no
 * depth bound, cycles included), and identical whatever order rows, node ids
 * or seeds happen to arrive in.
 */
#include "test_framework.h"
#include <store/store.h>
#include <store/store_impact.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int64_t add_node(cbm_store_t *s, const char *label, const char *name) {
    char qn[64];
    snprintf(qn, sizeof(qn), "test.%s", name);
    cbm_node_t node = {.project = "test", .label = label, .name = name, .qualified_name = qn};
    return cbm_store_upsert_node(s, &node);
}

/* `from` depends on `to`: the edge the walk follows backwards. */
static void add_edge(cbm_store_t *s, int64_t from, int64_t to, const char *type) {
    cbm_edge_t e = {.project = "test", .source_id = from, .target_id = to, .type = type};
    (void)cbm_store_insert_edge(s, &e);
}

static cbm_store_t *new_store(void) {
    cbm_store_t *s = cbm_store_open_memory();
    if (s) {
        cbm_store_upsert_project(s, "test", "/tmp/test");
    }
    return s;
}

static const char *const CALLS_ONLY[] = {"CALLS"};
static const char *const CALLS_USAGE[] = {"CALLS", "USAGE"};

static cbm_impact_walk_t *open_walk(cbm_store_t *s, const char *const *types, int count,
                                    bool routes) {
    cbm_impact_policy_t policy = {
        .project = "test", .edge_types = types, .edge_type_count = count, .follow_routes = routes};
    cbm_impact_walk_t *w = NULL;
    return cbm_impact_walk_open(s, &policy, &w) == CBM_STORE_OK ? w : NULL;
}

static const cbm_impact_hit_t *hit_of(const cbm_impact_walk_t *w, int64_t id) {
    const cbm_impact_hit_t *hits = cbm_impact_walk_hits(w);
    for (int i = 0; i < cbm_impact_walk_count(w); i++) {
        if (hits[i].id == id) {
            return &hits[i];
        }
    }
    return NULL;
}

TEST(impact_walk_follows_a_chain_to_its_end) {
    /* n30 calls n29 calls ... calls n0. No depth bound: a change to n0 reaches
     * all thirty, each at its own hop. */
    enum { LEN = 31 };
    cbm_store_t *s = new_store();
    ASSERT_NOT_NULL(s);
    int64_t ids[LEN];
    for (int i = 0; i < LEN; i++) {
        char name[16];
        snprintf(name, sizeof(name), "n%02d", i);
        ids[i] = add_node(s, "Function", name);
    }
    for (int i = 1; i < LEN; i++) {
        add_edge(s, ids[i], ids[i - 1], "CALLS");
    }
    cbm_impact_walk_t *w = open_walk(s, CALLS_ONLY, 1, false);
    ASSERT_NOT_NULL(w);
    ASSERT_EQ(cbm_impact_walk_run(w, &ids[0], 1), CBM_STORE_OK);
    ASSERT_EQ(cbm_impact_walk_count(w), LEN);
    for (int i = 0; i < LEN; i++) {
        const cbm_impact_hit_t *h = hit_of(w, ids[i]);
        ASSERT_NOT_NULL(h);
        ASSERT_EQ(h->hop, i);
        ASSERT_EQ(h->via_id, i == 0 ? 0 : ids[i - 1]);
    }
    ASSERT_NULL(hit_of(w, ids[0])->via_edge);
    ASSERT_STR_EQ(hit_of(w, ids[1])->via_edge, "CALLS");
    ASSERT_TRUE(cbm_impact_walk_hit(w, ids[7]) == hit_of(w, ids[7]));
    cbm_impact_walk_close(w);

    /* A hop limit keeps the same answer, cut at that distance. */
    cbm_impact_policy_t bounded = {
        .project = "test", .edge_types = CALLS_ONLY, .edge_type_count = 1, .max_hops = 3};
    ASSERT_EQ(cbm_impact_walk_open(s, &bounded, &w), CBM_STORE_OK);
    ASSERT_EQ(cbm_impact_walk_run(w, &ids[0], 1), CBM_STORE_OK);
    ASSERT_EQ(cbm_impact_walk_count(w), 4);
    ASSERT_EQ(cbm_impact_walk_hit(w, ids[3])->hop, 3);
    ASSERT_NULL(cbm_impact_walk_hit(w, ids[4]));
    cbm_impact_walk_close(w);
    cbm_store_close(s);
    PASS();
}

TEST(impact_walk_terminates_on_a_cycle) {
    /* a -> b -> c -> a, and a self-loop on b. Every node is reached once. */
    cbm_store_t *s = new_store();
    int64_t a = add_node(s, "Function", "a");
    int64_t b = add_node(s, "Function", "b");
    int64_t c = add_node(s, "Function", "c");
    add_edge(s, a, b, "CALLS");
    add_edge(s, b, c, "CALLS");
    add_edge(s, c, a, "CALLS");
    add_edge(s, b, b, "CALLS");
    cbm_impact_walk_t *w = open_walk(s, CALLS_ONLY, 1, false);
    ASSERT_NOT_NULL(w);
    ASSERT_EQ(cbm_impact_walk_run(w, &a, 1), CBM_STORE_OK);
    ASSERT_EQ(cbm_impact_walk_count(w), 3);
    ASSERT_EQ(hit_of(w, a)->hop, 0);
    ASSERT_EQ(hit_of(w, c)->hop, 1); /* c calls a */
    ASSERT_EQ(hit_of(w, b)->hop, 2); /* b calls c */
    cbm_impact_walk_close(w);
    cbm_store_close(s);
    PASS();
}

/* seed <- {parent_a, parent_b} <- x. Which parent x is reached THROUGH must not
 * depend on the order nodes or edges were inserted in. */
static int64_t build_diamond(cbm_store_t *s, bool reversed, int64_t *seed, int64_t *parent_a,
                             int64_t *parent_b) {
    int64_t x;
    if (reversed) {
        x = add_node(s, "Function", "x");
        *parent_b = add_node(s, "Function", "parent_b");
        *parent_a = add_node(s, "Function", "parent_a");
        *seed = add_node(s, "Function", "seed");
        add_edge(s, x, *parent_b, "CALLS");
        add_edge(s, x, *parent_a, "CALLS");
        add_edge(s, *parent_b, *seed, "CALLS");
        add_edge(s, *parent_a, *seed, "CALLS");
    } else {
        *seed = add_node(s, "Function", "seed");
        *parent_a = add_node(s, "Function", "parent_a");
        *parent_b = add_node(s, "Function", "parent_b");
        x = add_node(s, "Function", "x");
        add_edge(s, *parent_a, *seed, "CALLS");
        add_edge(s, *parent_b, *seed, "CALLS");
        add_edge(s, x, *parent_a, "CALLS");
        add_edge(s, x, *parent_b, "CALLS");
    }
    return x;
}

TEST(impact_walk_via_parent_is_the_smallest_qualified_name) {
    for (int reversed = 0; reversed < 2; reversed++) {
        cbm_store_t *s = new_store();
        int64_t seed;
        int64_t parent_a;
        int64_t parent_b;
        int64_t x = build_diamond(s, reversed != 0, &seed, &parent_a, &parent_b);
        cbm_impact_walk_t *w = open_walk(s, CALLS_ONLY, 1, false);
        ASSERT_NOT_NULL(w);
        ASSERT_EQ(cbm_impact_walk_run(w, &seed, 1), CBM_STORE_OK);
        ASSERT_EQ(cbm_impact_walk_count(w), 4);
        ASSERT_EQ(hit_of(w, x)->hop, 2);
        ASSERT_EQ(hit_of(w, x)->via_id, parent_a);
        /* Canonical order: by hop, then by qualified name. */
        const cbm_impact_hit_t *hits = cbm_impact_walk_hits(w);
        ASSERT_EQ(hits[0].id, seed);
        ASSERT_EQ(hits[1].id, parent_a);
        ASSERT_EQ(hits[2].id, parent_b);
        ASSERT_EQ(hits[3].id, x);
        ASSERT_STR_EQ(hits[1].qualified_name, "test.parent_a");
        cbm_impact_walk_close(w);
        cbm_store_close(s);
    }
    PASS();
}

TEST(impact_walk_via_edge_rank_beats_the_name) {
    /* x reaches the change through `zeta` by CALLS and through `alpha` by
     * USAGE. The request ranks CALLS first, so the path shown is the call,
     * although `alpha` sorts first. */
    cbm_store_t *s = new_store();
    int64_t seed = add_node(s, "Function", "seed");
    int64_t alpha = add_node(s, "Function", "alpha");
    int64_t zeta = add_node(s, "Function", "zeta");
    int64_t x = add_node(s, "Function", "x");
    add_edge(s, alpha, seed, "CALLS");
    add_edge(s, zeta, seed, "CALLS");
    add_edge(s, x, alpha, "USAGE");
    add_edge(s, x, zeta, "CALLS");
    cbm_impact_walk_t *w = open_walk(s, CALLS_USAGE, 2, false);
    ASSERT_NOT_NULL(w);
    ASSERT_EQ(cbm_impact_walk_run(w, &seed, 1), CBM_STORE_OK);
    ASSERT_EQ(hit_of(w, x)->via_id, zeta);
    ASSERT_STR_EQ(hit_of(w, x)->via_edge, "CALLS");
    cbm_impact_walk_close(w);

    /* Ranked the other way round, the same graph shows the usage. */
    static const char *const usage_first[] = {"USAGE", "CALLS"};
    w = open_walk(s, usage_first, 2, false);
    ASSERT_NOT_NULL(w);
    ASSERT_EQ(cbm_impact_walk_run(w, &seed, 1), CBM_STORE_OK);
    ASSERT_EQ(hit_of(w, x)->via_id, alpha);
    ASSERT_STR_EQ(hit_of(w, x)->via_edge, "USAGE");
    cbm_impact_walk_close(w);
    cbm_store_close(s);
    PASS();
}

TEST(impact_walk_follows_only_the_requested_edge_types) {
    cbm_store_t *s = new_store();
    int64_t seed = add_node(s, "Function", "seed");
    int64_t caller = add_node(s, "Function", "caller");
    int64_t reader = add_node(s, "Function", "reader");
    int64_t importer = add_node(s, "Module", "importer");
    add_edge(s, caller, seed, "CALLS");
    add_edge(s, reader, seed, "USAGE");
    add_edge(s, importer, seed, "IMPORTS");
    cbm_impact_walk_t *w = open_walk(s, CALLS_ONLY, 1, false);
    ASSERT_EQ(cbm_impact_walk_run(w, &seed, 1), CBM_STORE_OK);
    ASSERT_EQ(cbm_impact_walk_count(w), 2);
    ASSERT_NOT_NULL(hit_of(w, caller));
    ASSERT_NULL(hit_of(w, reader));
    cbm_impact_walk_close(w);

    w = open_walk(s, CALLS_USAGE, 2, false);
    ASSERT_EQ(cbm_impact_walk_run(w, &seed, 1), CBM_STORE_OK);
    ASSERT_EQ(cbm_impact_walk_count(w), 3);
    ASSERT_NOT_NULL(hit_of(w, reader));
    ASSERT_NULL(hit_of(w, importer));
    ASSERT_TRUE(cbm_impact_walk_reached(w, reader));
    ASSERT_FALSE(cbm_impact_walk_reached(w, importer));
    cbm_impact_walk_close(w);
    cbm_store_close(s);
    PASS();
}

TEST(impact_walk_reaches_a_sink_and_stops_there) {
    /* test_case calls seed, runner calls test_case. A test case is a sink: it
     * is selected, and what calls it is not "impacted by the change". */
    cbm_store_t *s = new_store();
    int64_t seed = add_node(s, "Function", "seed");
    int64_t test_case = add_node(s, "Function", "test_case");
    int64_t runner = add_node(s, "Function", "runner");
    int64_t helper = add_node(s, "Function", "helper");
    int64_t other = add_node(s, "Function", "other");
    add_edge(s, test_case, seed, "CALLS");
    add_edge(s, runner, test_case, "CALLS");
    add_edge(s, helper, seed, "CALLS");
    add_edge(s, other, helper, "CALLS");
    cbm_impact_walk_t *w = open_walk(s, CALLS_ONLY, 1, false);
    ASSERT_EQ(cbm_impact_walk_add_sinks(w, &test_case, 1), CBM_STORE_OK);
    ASSERT_EQ(cbm_impact_walk_run(w, &seed, 1), CBM_STORE_OK);
    ASSERT_NOT_NULL(hit_of(w, test_case));
    ASSERT_NULL(hit_of(w, runner));
    ASSERT_NOT_NULL(hit_of(w, other));
    ASSERT_EQ(cbm_impact_walk_count(w), 4);
    cbm_impact_walk_close(w);

    /* A sink that is itself the change is not expanded either. */
    w = open_walk(s, CALLS_ONLY, 1, false);
    ASSERT_EQ(cbm_impact_walk_add_sinks(w, &test_case, 1), CBM_STORE_OK);
    ASSERT_EQ(cbm_impact_walk_run(w, &test_case, 1), CBM_STORE_OK);
    ASSERT_EQ(cbm_impact_walk_count(w), 1);
    cbm_impact_walk_close(w);
    cbm_store_close(s);
    PASS();
}

TEST(impact_walk_second_run_continues_from_the_first) {
    /* A lift adds seeds after the first fixpoint: they enter one hop beyond
     * everything reached so far, and what was reached keeps its place. */
    cbm_store_t *s = new_store();
    int64_t seed = add_node(s, "Function", "seed");
    int64_t caller = add_node(s, "Function", "caller");
    int64_t spawner = add_node(s, "Function", "spawner");
    int64_t spawn_user = add_node(s, "Function", "spawn_user");
    add_edge(s, caller, seed, "CALLS");
    add_edge(s, spawn_user, spawner, "CALLS");
    add_edge(s, spawn_user, caller, "CALLS");
    cbm_impact_walk_t *w = open_walk(s, CALLS_ONLY, 1, false);
    ASSERT_EQ(cbm_impact_walk_run(w, &seed, 1), CBM_STORE_OK);
    ASSERT_EQ(cbm_impact_walk_count(w), 3);
    ASSERT_EQ(hit_of(w, spawn_user)->hop, 2);
    int64_t lift[] = {spawner, caller};
    ASSERT_EQ(cbm_impact_walk_run(w, lift, 2), CBM_STORE_OK);
    ASSERT_EQ(cbm_impact_walk_count(w), 4);
    ASSERT_EQ(hit_of(w, spawner)->hop, 3);
    ASSERT_EQ(hit_of(w, spawner)->via_id, 0);
    ASSERT_EQ(hit_of(w, caller)->hop, 1);
    ASSERT_EQ(hit_of(w, spawn_user)->hop, 2);
    cbm_impact_walk_close(w);
    cbm_store_close(s);
    PASS();
}

TEST(impact_walk_crosses_a_route_only_when_asked) {
    /* handler --HANDLES--> route <--HTTP_CALLS-- client. A change to the
     * handler concerns the client that calls its route. */
    cbm_store_t *s = new_store();
    int64_t handler = add_node(s, "Function", "handler");
    int64_t route = add_node(s, "Route", "GET_orders");
    int64_t client = add_node(s, "Function", "client");
    int64_t client_caller = add_node(s, "Function", "client_caller");
    add_edge(s, handler, route, "HANDLES");
    add_edge(s, client, route, "HTTP_CALLS");
    add_edge(s, client_caller, client, "CALLS");
    cbm_impact_walk_t *w = open_walk(s, CALLS_ONLY, 1, false);
    ASSERT_EQ(cbm_impact_walk_run(w, &handler, 1), CBM_STORE_OK);
    ASSERT_EQ(cbm_impact_walk_count(w), 1);
    cbm_impact_walk_close(w);

    w = open_walk(s, CALLS_ONLY, 1, true);
    ASSERT_EQ(cbm_impact_walk_run(w, &handler, 1), CBM_STORE_OK);
    ASSERT_EQ(cbm_impact_walk_count(w), 4);
    ASSERT_EQ(hit_of(w, route)->hop, 1);
    ASSERT_STR_EQ(hit_of(w, route)->via_edge, "HANDLES");
    ASSERT_EQ(hit_of(w, client)->hop, 2);
    ASSERT_STR_EQ(hit_of(w, client)->via_edge, "HTTP_CALLS");
    ASSERT_EQ(hit_of(w, client_caller)->hop, 3);
    cbm_impact_walk_close(w);
    cbm_store_close(s);
    PASS();
}

TEST(impact_walk_rejects_what_it_cannot_answer) {
    cbm_store_t *s = new_store();
    int64_t seed = add_node(s, "Function", "seed");
    cbm_impact_walk_t *w = NULL;
    cbm_impact_policy_t no_types = {.project = "test", .edge_types = NULL, .edge_type_count = 0};
    ASSERT_EQ(cbm_impact_walk_open(s, &no_types, &w), CBM_STORE_ERR);
    ASSERT_NULL(w);
    ASSERT_EQ(cbm_impact_walk_open(NULL, &no_types, &w), CBM_STORE_ERR);

    /* A seed the store does not hold is an error, never an empty answer: an
     * empty answer would read as "nothing depends on this". */
    w = open_walk(s, CALLS_ONLY, 1, false);
    ASSERT_NOT_NULL(w);
    int64_t unknown = seed + 1000;
    ASSERT_EQ(cbm_impact_walk_run(w, &unknown, 1), CBM_STORE_ERR);
    ASSERT_EQ(cbm_impact_walk_add_sinks(w, &unknown, 1), CBM_STORE_ERR);
    cbm_impact_walk_close(w);

    /* Duplicate seeds are one seed. */
    w = open_walk(s, CALLS_ONLY, 1, false);
    int64_t twice[] = {seed, seed};
    ASSERT_EQ(cbm_impact_walk_run(w, twice, 2), CBM_STORE_OK);
    ASSERT_EQ(cbm_impact_walk_count(w), 1);
    cbm_impact_walk_close(w);
    cbm_impact_walk_close(NULL);
    cbm_store_close(s);
    PASS();
}

SUITE(store_impact) {
    RUN_TEST(impact_walk_follows_a_chain_to_its_end);
    RUN_TEST(impact_walk_terminates_on_a_cycle);
    RUN_TEST(impact_walk_via_parent_is_the_smallest_qualified_name);
    RUN_TEST(impact_walk_via_edge_rank_beats_the_name);
    RUN_TEST(impact_walk_follows_only_the_requested_edge_types);
    RUN_TEST(impact_walk_reaches_a_sink_and_stops_there);
    RUN_TEST(impact_walk_second_run_continues_from_the_first);
    RUN_TEST(impact_walk_crosses_a_route_only_when_asked);
    RUN_TEST(impact_walk_rejects_what_it_cannot_answer);
}
