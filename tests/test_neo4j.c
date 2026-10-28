/*
 * test_neo4j.c — Neo4j graph backend: URL/config/marker handling, the
 * store's remote routing seam, the native-query write guard, and (when
 * CBM_TEST_NEO4J_URI is set) a live sync + read-back against a real server.
 *
 * Live run:
 *   docker run -d -p7474:7474 -e NEO4J_AUTH=neo4j/testpass123 neo4j:5
 *   CBM_TEST_NEO4J_URI=http://localhost:7474 CBM_TEST_NEO4J_PASSWORD=testpass123 \
 *     build/c/test-runner neo4j
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "neo4j/neo4j_client.h"
#include "neo4j/neo4j_http.h"
#include "neo4j/neo4j_store.h"
#include "neo4j/neo4j_sync.h"
#include "mcp/mcp.h"
#include "store/store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── URL parsing ─────────────────────────────────────────────────── */

TEST(neo4j_url_defaults) {
    cbm_neo4j_url_t u;
    char err[256];
    ASSERT_EQ(cbm_neo4j_url_parse("http://db.local", &u, err, sizeof(err)), 0);
    ASSERT_STR_EQ(u.host, "db.local");
    ASSERT_EQ(u.port, 7474);
    ASSERT_FALSE(u.tls);
    ASSERT_STR_EQ(u.base_path, "");
    ASSERT_EQ(cbm_neo4j_url_parse("HTTPS://db.local/", &u, err, sizeof(err)), 0);
    ASSERT_TRUE(u.tls);
    ASSERT_EQ(u.port, 7473);
    PASS();
}

TEST(neo4j_url_port_path_ipv6) {
    cbm_neo4j_url_t u;
    char err[256];
    ASSERT_EQ(cbm_neo4j_url_parse("http://10.0.0.5:17474/neo4j/", &u, err, sizeof(err)), 0);
    ASSERT_STR_EQ(u.host, "10.0.0.5");
    ASSERT_EQ(u.port, 17474);
    ASSERT_STR_EQ(u.base_path, "/neo4j");
    ASSERT_EQ(cbm_neo4j_url_parse("http://[::1]:7475", &u, err, sizeof(err)), 0);
    ASSERT_STR_EQ(u.host, "::1");
    ASSERT_EQ(u.port, 7475);
    PASS();
}

TEST(neo4j_url_rejects_bolt_and_garbage) {
    cbm_neo4j_url_t u;
    char err[256];
    ASSERT_EQ(cbm_neo4j_url_parse("bolt://h:7687", &u, err, sizeof(err)), -1);
    ASSERT_NOT_NULL(strstr(err, "7474"));
    ASSERT_EQ(cbm_neo4j_url_parse("neo4j+s://h", &u, err, sizeof(err)), -1);
    ASSERT_EQ(cbm_neo4j_url_parse("ftp://h", &u, err, sizeof(err)), -1);
    ASSERT_EQ(cbm_neo4j_url_parse("http://h:0", &u, err, sizeof(err)), -1);
    ASSERT_EQ(cbm_neo4j_url_parse("http://h:99999", &u, err, sizeof(err)), -1);
    ASSERT_EQ(cbm_neo4j_url_parse("http://", &u, err, sizeof(err)), -1);
    ASSERT_EQ(cbm_neo4j_url_parse("", &u, err, sizeof(err)), -1);
    PASS();
}

TEST(neo4j_base64_vectors) {
    static const char *const in[] = {"", "f", "fo", "foo", "foob", "fooba", "foobar"};
    static const char *const out[] = {"",         "Zg==",     "Zm8=",    "Zm9v",
                                      "Zm9vYg==", "Zm9vYmE=", "Zm9vYmFy"};
    for (int i = 0; i < 7; i++) {
        char *b = cbm_neo4j_base64(in[i], strlen(in[i]));
        ASSERT_NOT_NULL(b);
        ASSERT_STR_EQ(b, out[i]);
        free(b);
    }
    PASS();
}

/* ── Config ──────────────────────────────────────────────────────── */

static void clear_neo4j_env(void) {
    cbm_unsetenv("NEO4J_URI");
    cbm_unsetenv("NEO4J_USER");
    cbm_unsetenv("NEO4J_USERNAME");
    cbm_unsetenv("NEO4J_PASSWORD");
    cbm_unsetenv("NEO4J_DATABASE");
    cbm_unsetenv(CBM_NEO4J_PASSWORD_ENV);
}

TEST(neo4j_config_explicit_beats_env) {
    clear_neo4j_env();
    cbm_setenv("NEO4J_USER", "envuser", 1);
    cbm_setenv("NEO4J_PASSWORD", "envpw", 1);
    cbm_setenv("NEO4J_DATABASE", "envdb", 1);
    cbm_neo4j_config_t c;
    char err[256];
    ASSERT_EQ(cbm_neo4j_config_resolve("http://h:7474", "u", "p", "d", &c, err, sizeof(err)), 0);
    ASSERT_STR_EQ(c.user, "u");
    ASSERT_STR_EQ(c.password, "p");
    ASSERT_STR_EQ(c.database, "d");
    ASSERT_EQ(cbm_neo4j_config_resolve("http://h:7474", NULL, NULL, NULL, &c, err, sizeof(err)), 0);
    ASSERT_STR_EQ(c.user, "envuser");
    ASSERT_STR_EQ(c.password, "envpw");
    ASSERT_STR_EQ(c.database, "envdb");
    /* The supervisor hand-off outranks the ambient NEO4J_PASSWORD. */
    cbm_setenv(CBM_NEO4J_PASSWORD_ENV, "handoff", 1);
    ASSERT_EQ(cbm_neo4j_config_resolve("http://h:7474", NULL, NULL, NULL, &c, err, sizeof(err)), 0);
    ASSERT_STR_EQ(c.password, "handoff");
    clear_neo4j_env();
    ASSERT_EQ(cbm_neo4j_config_resolve("http://h:7474", NULL, NULL, NULL, &c, err, sizeof(err)), 0);
    ASSERT_STR_EQ(c.user, "neo4j");
    ASSERT_STR_EQ(c.database, "neo4j");
    PASS();
}

TEST(neo4j_config_rejects_bad_database_and_missing_uri) {
    clear_neo4j_env();
    cbm_neo4j_config_t c;
    char err[256];
    ASSERT_EQ(cbm_neo4j_config_resolve("http://h", "u", "p", "bad/db", &c, err, sizeof(err)), -1);
    ASSERT_EQ(cbm_neo4j_config_resolve(NULL, "u", "p", NULL, &c, err, sizeof(err)), -1);
    PASS();
}

TEST(neo4j_config_meta_has_no_password) {
    clear_neo4j_env();
    cbm_neo4j_config_t c;
    char err[256];
    ASSERT_EQ(cbm_neo4j_config_resolve("http://meta-host:7474", "alice", "s3cret", "graphs", &c,
                                       err, sizeof(err)),
              0);
    char *meta = cbm_neo4j_config_to_meta(&c);
    ASSERT_NOT_NULL(meta);
    ASSERT_NULL(strstr(meta, "s3cret"));
    cbm_neo4j_config_t back;
    /* No env and no cache entry yet: the password stays empty. */
    ASSERT_EQ(cbm_neo4j_config_from_meta(meta, &back, err, sizeof(err)), 0);
    ASSERT_STR_EQ(back.uri, "http://meta-host:7474");
    ASSERT_STR_EQ(back.user, "alice");
    ASSERT_STR_EQ(back.database, "graphs");
    ASSERT_STR_EQ(back.password, "");
    /* After an index call remembered it, a reader resolves it. */
    cbm_neo4j_config_remember(&c);
    ASSERT_EQ(cbm_neo4j_config_from_meta(meta, &back, err, sizeof(err)), 0);
    ASSERT_STR_EQ(back.password, "s3cret");
    free(meta);
    ASSERT_EQ(cbm_neo4j_config_from_meta("{\"kind\":\"other\"}", &back, err, sizeof(err)), -1);
    ASSERT_EQ(cbm_neo4j_config_from_meta("not json", &back, err, sizeof(err)), -1);
    PASS();
}

TEST(neo4j_ident_ok) {
    ASSERT_TRUE(cbm_neo4j_ident_ok("CALLS"));
    ASSERT_TRUE(cbm_neo4j_ident_ok("_x1"));
    ASSERT_FALSE(cbm_neo4j_ident_ok("1abc"));
    ASSERT_FALSE(cbm_neo4j_ident_ok("A`B"));
    ASSERT_FALSE(cbm_neo4j_ident_ok("a b"));
    ASSERT_FALSE(cbm_neo4j_ident_ok(""));
    ASSERT_FALSE(cbm_neo4j_ident_ok(NULL));
    PASS();
}

/* ── Marker file ─────────────────────────────────────────────────── */

TEST(neo4j_sidecar_roundtrip) {
    clear_neo4j_env();
    char *dir = th_mktempdir("cbm_n4_sidecar");
    ASSERT_NOT_NULL(dir);
    char db[512];
    snprintf(db, sizeof(db), "%s/proj.db", dir);
    cbm_neo4j_config_t c;
    char err[256];
    char gen[64];
    ASSERT_EQ(cbm_neo4j_sidecar_read(db, &c, gen, sizeof(gen), err, sizeof(err)), 1);
    ASSERT_EQ(cbm_neo4j_config_resolve("http://side:7474", "bob", "pw", NULL, &c, err, sizeof(err)),
              0);
    ASSERT_EQ(cbm_neo4j_sidecar_write(db, &c, "u1g2"), 0);
    cbm_neo4j_config_t back;
    ASSERT_EQ(cbm_neo4j_sidecar_read(db, &back, gen, sizeof(gen), err, sizeof(err)), 0);
    ASSERT_STR_EQ(back.uri, "http://side:7474");
    ASSERT_STR_EQ(back.user, "bob");
    ASSERT_STR_EQ(gen, "u1g2");
    /* The marker never stores the password. */
    char path[600];
    ASSERT_EQ(cbm_neo4j_sidecar_path(db, path, sizeof(path)), 0);
    FILE *f = fopen(path, "rb");
    ASSERT_NOT_NULL(f);
    char buf[1024];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    ASSERT_NULL(strstr(buf, "\"pw\""));
    ASSERT_EQ(cbm_neo4j_sidecar_remove(db), 0);
    ASSERT_EQ(cbm_neo4j_sidecar_read(db, &back, gen, sizeof(gen), err, sizeof(err)), 1);
    th_rmtree(dir);
    PASS();
}

/* ── Store routing seam ──────────────────────────────────────────── */

typedef struct {
    int closed;
    int by_id_calls;
    int bfs_calls;
} fake_remote_t;

static fake_remote_t g_fake;

static void fake_close(void *ctx) {
    ((fake_remote_t *)ctx)->closed++;
}
static const char *fake_error(void *ctx) {
    (void)ctx;
    return "fake failure";
}
static char *fake_dup(const char *s) {
    char *d = malloc(strlen(s) + 1);
    strcpy(d, s);
    return d;
}
static int fake_find_node_by_id(void *ctx, int64_t id, cbm_node_t *out) {
    ((fake_remote_t *)ctx)->by_id_calls++;
    if (id != 42) {
        return CBM_STORE_NOT_FOUND;
    }
    memset(out, 0, sizeof(*out));
    out->id = 42;
    out->project = fake_dup("p");
    out->label = fake_dup("Function");
    out->name = fake_dup("remote_fn");
    out->qualified_name = fake_dup("p.remote_fn");
    out->file_path = fake_dup("a.c");
    out->properties_json = fake_dup("{}");
    return CBM_STORE_OK;
}
static int fake_find_node_by_qn(void *ctx, const char *project, const char *qn, cbm_node_t *out) {
    return CBM_STORE_NOT_FOUND;
}
static int fake_find_nodes(void *ctx, cbm_remote_node_query_t kind, const char *project,
                           const char *arg, cbm_node_t **out, int *count) {
    *out = NULL;
    *count = 0;
    return CBM_STORE_OK;
}
static int fake_find_edges(void *ctx, int64_t node_id, bool by_target, const char *type,
                           cbm_edge_t **out, int *count) {
    *out = NULL;
    *count = 0;
    return CBM_STORE_ERR;
}
static int fake_find_edges_by_type(void *ctx, const char *project, const char *type,
                                   cbm_edge_t **out, int *count) {
    *out = NULL;
    *count = 0;
    return CBM_STORE_OK;
}
static int fake_count(void *ctx, cbm_remote_count_t kind, const char *project, const char *arg) {
    return kind == CBM_REMOTE_COUNT_NODES ? 1234 : 7;
}
static int fake_degrees(void *ctx, const int64_t *ids, int n, const char *type, int *in, int *out) {
    for (int i = 0; i < n; i++) {
        in[i] = 3;
        out[i] = 5;
    }
    return CBM_STORE_OK;
}
static int fake_bfs(void *ctx, const int64_t *seeds, int seed_count, bool multi,
                    const char *direction, const char **types, int ntypes, int max_depth,
                    int max_results, bool trail, int max_edges, cbm_traverse_result_t *out,
                    bool *truncated) {
    ((fake_remote_t *)ctx)->bfs_calls++;
    memset(out, 0, sizeof(*out));
    return CBM_STORE_OK;
}

static const cbm_store_remote_ops_t k_fake_ops = {
    .close = fake_close,
    .error = fake_error,
    .find_node_by_id = fake_find_node_by_id,
    .find_node_by_qn = fake_find_node_by_qn,
    .find_nodes = fake_find_nodes,
    .find_edges = fake_find_edges,
    .find_edges_by_type = fake_find_edges_by_type,
    .count = fake_count,
    .degrees = fake_degrees,
    .bfs = fake_bfs,
};

TEST(neo4j_store_remote_routing) {
    memset(&g_fake, 0, sizeof(g_fake));
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "p", "/tmp/p");
    ASSERT_FALSE(cbm_store_is_remote(s));
    cbm_store_attach_remote(s, &k_fake_ops, &g_fake);
    ASSERT_TRUE(cbm_store_is_remote(s));

    cbm_node_t n = {0};
    ASSERT_EQ(cbm_store_find_node_by_id(s, 42, &n), CBM_STORE_OK);
    ASSERT_STR_EQ(n.name, "remote_fn");
    cbm_node_free_fields(&n);
    ASSERT_EQ(cbm_store_find_node_by_id(s, 7, &n), CBM_STORE_NOT_FOUND);
    ASSERT_EQ(g_fake.by_id_calls, 2);

    ASSERT_EQ(cbm_store_count_nodes(s, "p"), 1234);
    ASSERT_EQ(cbm_store_count_edges(s, "p"), 7);
    int in = 0;
    int out = 0;
    cbm_store_node_degree(s, 42, &in, &out);
    ASSERT_EQ(in, 3);
    ASSERT_EQ(out, 5);

    cbm_edge_t *edges = NULL;
    int ec = -1;
    ASSERT_EQ(cbm_store_find_edges_by_source(s, 42, &edges, &ec), CBM_STORE_ERR);
    ASSERT_NOT_NULL(strstr(cbm_store_error(s), "fake failure"));

    cbm_traverse_result_t tr;
    ASSERT_EQ(cbm_store_bfs(s, 42, "outbound", NULL, 0, 3, 10, &tr), CBM_STORE_OK);
    cbm_store_traverse_free(&tr);
    int64_t seeds[] = {1, 2};
    bool trunc = true;
    ASSERT_EQ(cbm_store_bfs_multi(s, seeds, 2, "inbound", NULL, 0, 2, 10, &tr, &trunc),
              CBM_STORE_OK);
    cbm_store_traverse_free(&tr);
    ASSERT_EQ(g_fake.bfs_calls, 2);

    cbm_store_close(s);
    ASSERT_EQ(g_fake.closed, 1);
    PASS();
}

/* ── Routed lookups keep the local predicates exactly ────────────── */

typedef struct {
    int search_calls;
    int last_limit;
    int last_offset;
    char last_name_hint[64];
    uint64_t queries;
} fake2_t;

static fake2_t g_fake2;

static void fake2_node(cbm_node_t *n, int64_t id, const char *name, const char *qn,
                       const char *file) {
    memset(n, 0, sizeof(*n));
    n->id = id;
    n->project = fake_dup("p");
    n->label = fake_dup("Function");
    n->name = fake_dup(name);
    n->qualified_name = fake_dup(qn);
    n->file_path = fake_dup(file);
    n->properties_json = fake_dup("{}");
}

static int fake2_find_nodes(void *ctx, cbm_remote_node_query_t kind, const char *project,
                            const char *arg, cbm_node_t **out, int *count) {
    g_fake2.queries++;
    /* Deliberately broad candidate sets: store.c must apply the exact
     * SQLite predicate to them. */
    cbm_node_t *n = calloc(4, sizeof(cbm_node_t));
    if (kind == CBM_REMOTE_NODES_QN_ENDS_CI) {
        fake2_node(&n[0], 1, "Foo", "p.a.Foo", "a.c");      /* ".foo", any case: kept */
        fake2_node(&n[1], 2, "xfoo", "p.a.xfoo", "a.c");    /* no '.' before: dropped */
        fake2_node(&n[2], 3, "foo", "foo", "a.c");          /* exact QN: kept */
        fake2_node(&n[3], 4, "bar", "p.b.foo.bar", "a.c"); /* not a suffix: dropped */
        *count = 4;
    } else if (kind == CBM_REMOTE_NODES_QN_CONTAINS_CI) {
        fake2_node(&n[0], 1, "run", "p.a.run(int)", "a.c");       /* base p.a.run: kept */
        fake2_node(&n[1], 2, "run", "p.a.run", "a.c");            /* unsuffixed: dropped */
        fake2_node(&n[2], 3, "runner", "p.a.runner(int)", "a.c"); /* other base: dropped */
        *count = 3;
    } else {
        *count = 0;
    }
    *out = n;
    return CBM_STORE_OK;
}

static int fake2_search(void *ctx, const cbm_remote_search_t *q, cbm_search_result_t **out,
                        int *count, int *total) {
    g_fake2.queries++;
    g_fake2.search_calls++;
    g_fake2.last_limit = q->limit;
    g_fake2.last_offset = q->offset;
    snprintf(g_fake2.last_name_hint, sizeof(g_fake2.last_name_hint), "%s",
             q->name_contains_ci && q->name_contains_ci[0] ? q->name_contains_ci[0] : "");
    /* Candidates in (name, id) order, as the backend returns them. */
    static const char *const names[] = {"Alpha", "alphabet", "Beta", "gamma_alpha"};
    static const char *const files[] = {"src/x.c", "lib/y.c", "src/z.c", "src/w.c"};
    cbm_search_result_t *rows = calloc(4, sizeof(cbm_search_result_t));
    for (int i = 0; i < 4; i++) {
        char qn[64];
        snprintf(qn, sizeof(qn), "p.%s", names[i]);
        fake2_node(&rows[i].node, i + 1, names[i], qn, files[i]);
        rows[i].in_degree = i;
        rows[i].out_degree = 1;
    }
    *out = rows;
    *count = 4;
    *total = 99; /* what the backend counted when it paged itself */
    return CBM_STORE_OK;
}

static uint64_t fake2_queries(void *ctx) {
    return g_fake2.queries;
}

static const cbm_store_remote_ops_t k_fake2_ops = {
    .close = fake_close,
    .error = fake_error,
    .find_nodes = fake2_find_nodes,
    .search = fake2_search,
    .queries = fake2_queries,
};

static int search_names(cbm_store_t *s, const char *name_pattern, const char *file_pattern,
                        int limit, int offset, char *buf, size_t sz, int *total) {
    cbm_search_params_t p = {.project = "p",
                             .name_pattern = name_pattern,
                             .file_pattern = file_pattern,
                             .min_degree = -1,
                             .max_degree = -1,
                             .limit = limit,
                             .offset = offset};
    cbm_search_output_t out = {0};
    int rc = cbm_store_search(s, &p, &out);
    buf[0] = '\0';
    size_t off = 0;
    for (int i = 0; rc == CBM_STORE_OK && i < out.count && off < sz; i++) {
        off += (size_t)snprintf(buf + off, sz - off, "%s,", out.results[i].node.name);
    }
    *total = out.total;
    cbm_store_search_free(&out);
    return rc;
}

TEST(neo4j_routed_lookups_apply_local_predicates) {
    memset(&g_fake, 0, sizeof(g_fake));
    memset(&g_fake2, 0, sizeof(g_fake2));
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_attach_remote(s, &k_fake2_ops, &g_fake);

    /* QN suffix: SQLite's  qn LIKE '%.foo' (ASCII any case) OR qn = 'foo'. */
    cbm_node_t *nodes = NULL;
    int n = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_qn_suffix(s, "p", "foo", &nodes, &n), CBM_STORE_OK);
    ASSERT_EQ(n, 2);
    ASSERT_STR_EQ(nodes[0].qualified_name, "p.a.Foo");
    ASSERT_STR_EQ(nodes[1].qualified_name, "foo");
    cbm_store_free_nodes(nodes, n);

    /* QN base: only callable-suffixed QNs whose base is exactly p.a.run. */
    ASSERT_EQ(cbm_store_find_nodes_by_qn_base(s, "p", "p.a.run", false, &nodes, &n), CBM_STORE_OK);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(nodes[0].qualified_name, "p.a.run(int)");
    cbm_store_free_nodes(nodes, n);

    /* search: the regex (cbm, POSIX ERE, unanchored, any case) and the file
     * glob run locally over the candidates; total and paging follow them. */
    char names[128];
    int total = 0;
    ASSERT_EQ(search_names(s, "^alpha", "src/*", 10, 0, names, sizeof(names), &total),
              CBM_STORE_OK);
    ASSERT_STR_EQ(names, "Alpha,");
    ASSERT_EQ(total, 1);
    ASSERT_EQ(g_fake2.last_limit, -1); /* local filters: every candidate, unpaged */
    ASSERT_STR_EQ(g_fake2.last_name_hint, "alpha");
    ASSERT_EQ(search_names(s, "alpha", NULL, 1, 1, names, sizeof(names), &total), CBM_STORE_OK);
    ASSERT_STR_EQ(names, "alphabet,");
    ASSERT_EQ(total, 3);
    ASSERT_EQ(search_names(s, "(", NULL, 10, 0, names, sizeof(names), &total), CBM_STORE_ERR);
    ASSERT_NOT_NULL(strstr(cbm_store_error(s), "invalid regex"));

    /* No pattern: the backend pages and counts itself. */
    ASSERT_EQ(search_names(s, NULL, NULL, 2, 5, names, sizeof(names), &total), CBM_STORE_OK);
    ASSERT_EQ(g_fake2.last_limit, 2);
    ASSERT_EQ(g_fake2.last_offset, 5);
    ASSERT_EQ(total, 99);

    /* Backend usage: remote queries since the reset, plus local graph reads. */
    cbm_store_backend_usage_t usage;
    cbm_store_backend_usage(s, &usage);
    ASSERT_EQ((int)usage.remote_queries, (int)g_fake2.queries);
    ASSERT_EQ(usage.local_reads, 0);
    cbm_store_backend_usage_reset(s);
    cbm_store_note_local_graph_read(s, "architecture");
    cbm_store_note_local_graph_read(s, "text search");
    cbm_store_note_local_graph_read(s, "architecture");
    cbm_store_backend_usage(s, &usage);
    ASSERT_EQ((int)usage.remote_queries, 0);
    ASSERT_EQ(usage.local_reads, 3);
    ASSERT_STR_EQ(usage.local_what, "architecture, text search");
    cbm_store_close(s);

    /* Without a remote the marks are no-ops. */
    s = cbm_store_open_memory();
    cbm_store_note_local_graph_read(s, "architecture");
    cbm_store_backend_usage(s, &usage);
    ASSERT_EQ(usage.local_reads, 0);
    cbm_store_close(s);
    PASS();
}

TEST(neo4j_marker_found_for_renamed_database) {
    clear_neo4j_env();
    char *dir = th_mktempdir("cbm_n4_rename");
    ASSERT_NOT_NULL(dir);
    char canonical[512];
    char renamed[512];
    snprintf(canonical, sizeof(canonical), "%s/proj.db", dir);
    snprintf(renamed, sizeof(renamed), "%s/dummy.db", dir);
    char found[600];
    ASSERT_EQ(cbm_neo4j_sidecar_locate(renamed, "proj", found, sizeof(found)), 1);
    cbm_neo4j_config_t c;
    char err[256];
    ASSERT_EQ(cbm_neo4j_config_resolve("http://side:7474", "u", "p", NULL, &c, err, sizeof(err)), 0);
    ASSERT_EQ(cbm_neo4j_sidecar_write(canonical, &c, "u1g1"), 0);
    /* The database was found under another name; its marker by project name. */
    ASSERT_EQ(cbm_neo4j_sidecar_locate(renamed, "proj", found, sizeof(found)), 0);
    ASSERT_STR_EQ(found, canonical);
    ASSERT_EQ(cbm_neo4j_sidecar_locate(canonical, "proj", found, sizeof(found)), 0);
    ASSERT_STR_EQ(found, canonical);
    ASSERT_EQ(cbm_neo4j_sidecar_locate(renamed, "other", found, sizeof(found)), 1);
    th_rmtree(dir);
    PASS();
}

TEST(neo4j_served_by_is_named_in_replies) {
    ASSERT_STR_EQ(cbm_mcp_graph_served_by(3, 0), "neo4j");
    ASSERT_STR_EQ(cbm_mcp_graph_served_by(3, 1), "neo4j+local");
    ASSERT_STR_EQ(cbm_mcp_graph_served_by(0, 2), "local");
    ASSERT_NULL(cbm_mcp_graph_served_by(0, 0));

    char *reply = cbm_mcp_result_add_graph_served(cbm_mcp_text_result("{\"rows\":1}", false), 4, 1,
                                                  "text search");
    yyjson_doc *d = yyjson_read(reply, strlen(reply), 0);
    yyjson_val *root = yyjson_doc_get_root(d);
    const char *notice =
        yyjson_get_str(yyjson_obj_get(yyjson_arr_get(yyjson_obj_get(root, "content"), 1), "text"));
    ASSERT_NOT_NULL(notice);
    ASSERT_NOT_NULL(strstr(notice, "neo4j+local"));
    ASSERT_NOT_NULL(strstr(notice, "text search"));
    yyjson_val *gb = yyjson_obj_get(yyjson_obj_get(root, "structuredContent"), "graph_backend");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(gb, "served_by")), "neo4j+local");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(gb, "status")), "serving");
    ASSERT_EQ((int)yyjson_get_int(yyjson_obj_get(gb, "remote_queries")), 4);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(gb, "local")), "text search");
    yyjson_doc_free(d);
    free(reply);

    /* A tool that read no graph data is left untouched. */
    char *plain = cbm_mcp_text_result("ok", false);
    char *same = cbm_mcp_result_add_graph_served(plain, 0, 0, NULL);
    ASSERT_TRUE(same == plain);
    free(same);
    PASS();
}

TEST(neo4j_native_query_is_read_only) {
    static const char *const writes[] = {
        "MATCH (n) DETACH DELETE n",  "create (n:X)",
        "MATCH (n) SET n.x = 1",      "MERGE (a:A)",
        "CALL db.clearQueryCaches()", "LOAD CSV FROM 'x' AS r RETURN r",
        "MATCH (n) REMOVE n.x",       "FOREACH (x IN [1] | CREATE (:A))"};
    for (size_t i = 0; i < sizeof(writes) / sizeof(writes[0]); i++) {
        cbm_cypher_result_t r;
        ASSERT_EQ(cbm_neo4j_native_query("/nonexistent/x.db", "p", writes[i], 0, &r), -1);
        ASSERT_NOT_NULL(r.error);
        ASSERT_NOT_NULL(strstr(r.error, "read-only"));
        cbm_cypher_result_free(&r);
    }
    /* Keywords inside strings, backticks and comments are data, not writes;
     * such a query passes the guard and fails only on the missing project. */
    cbm_cypher_result_t r;
    ASSERT_EQ(cbm_neo4j_native_query("/nonexistent/x.db", "p",
                                     "MATCH (n {name:'CREATE'}) // DELETE\nRETURN n.`set`", 0, &r),
              -1);
    ASSERT_NOT_NULL(r.error);
    ASSERT_NULL(strstr(r.error, "read-only"));
    cbm_cypher_result_free(&r);
    PASS();
}

/* ── Response validation and no-fallback behavior ─────────────────── */

static const char *shape_of(const char *json, int statements) {
    static char verdict[128];
    yyjson_doc *d = yyjson_read(json, strlen(json), 0);
    const char *e =
        cbm_neo4j_result_shape_error(yyjson_obj_get(yyjson_doc_get_root(d), "results"), statements);
    (void)snprintf(verdict, sizeof(verdict), "%s", e ? e : "ok");
    yyjson_doc_free(d);
    return verdict;
}

TEST(neo4j_rejects_malformed_2xx_bodies) {
    /* A complete reply is accepted, including a statement with no rows. */
    ASSERT_STR_EQ(shape_of("{\"results\":[{\"columns\":[\"a\",\"b\"],\"data\":[{\"row\":[1,2]}]},"
                           "{\"columns\":[\"n\"],\"data\":[]}],\"errors\":[]}",
                           2),
                  "ok");
    /* Everything else must be an error, never "no rows": a 200 from a proxy
     * or another service, a truncated reply, or rows that do not fit. */
    ASSERT_STR_NEQ(shape_of("{\"errors\":[]}", 1), "ok");
    ASSERT_STR_NEQ(shape_of("{\"results\":{},\"errors\":[]}", 1), "ok");
    ASSERT_STR_NEQ(shape_of("{\"results\":[],\"errors\":[]}", 1), "ok");
    ASSERT_STR_NEQ(shape_of("{\"results\":[{\"columns\":[\"a\"],\"data\":[]}]}", 2), "ok");
    ASSERT_STR_NEQ(shape_of("{\"results\":[{\"data\":[]}]}", 1), "ok");
    ASSERT_STR_NEQ(
        shape_of("{\"results\":[{\"columns\":[\"a\",\"b\"],\"data\":[{\"row\":[1]}]}]}", 1), "ok");
    ASSERT_STR_NEQ(shape_of("{\"results\":[{\"columns\":[\"a\"],\"data\":[{\"meta\":[]}]}]}", 1),
                   "ok");
    PASS();
}

TEST(neo4j_request_with_invalid_utf8_is_still_sent) {
    /* A docstring from a Latin-1 source file ("Copyright \xa9 2004") is not
     * UTF-8. It used to fail the whole sync with "failed to serialize Neo4j
     * request"; it must now reach the transport (here a closed port). */
    clear_neo4j_env();
    cbm_neo4j_config_t cfg;
    char err[256];
    ASSERT_EQ(cbm_neo4j_config_resolve("http://127.0.0.1:1", "neo4j", "pw", NULL, &cfg, err,
                                       sizeof(err)),
              0);
    cbm_neo4j_t *c = cbm_neo4j_connect(&cfg, err, sizeof(err));
    ASSERT_NOT_NULL(c);
    cbm_neo4j_batch_t b;
    ASSERT_EQ(cbm_neo4j_batch_init(&b), 0);
    yyjson_mut_val *params = cbm_neo4j_batch_add(&b, "RETURN $doc");
    ASSERT_NOT_NULL(params);
    yyjson_mut_obj_add_strcpy(b.doc, params, "doc", "Copyright \xa9 2004 valida\xe7\xe3o");
    ASSERT_EQ(cbm_neo4j_batch_run(c, &b, NULL), -1);
    ASSERT_NOT_NULL(cbm_neo4j_error(c));
    ASSERT_NULL(strstr(cbm_neo4j_error(c), "serialize"));
    cbm_neo4j_batch_free(&b);
    cbm_neo4j_close(c);
    PASS();
}

TEST(neo4j_sidecar_records_last_sync_error) {
    clear_neo4j_env();
    char *dir = th_mktempdir("cbm_n4_status");
    ASSERT_NOT_NULL(dir);
    char db[512];
    snprintf(db, sizeof(db), "%s/proj.db", dir);
    cbm_neo4j_config_t c;
    char err[256];
    ASSERT_EQ(cbm_neo4j_config_resolve("http://side:7474", "bob", "pw", NULL, &c, err, sizeof(err)),
              0);
    char field[256];
    ASSERT_EQ(cbm_neo4j_sidecar_field(db, "last_sync_error", field, sizeof(field)), -1);
    /* A failed background sync keeps the last good generation and says why. */
    ASSERT_EQ(cbm_neo4j_sidecar_write_status(db, &c, "u1g2", "cannot connect to Neo4j"), 0);
    ASSERT_EQ(cbm_neo4j_sidecar_field(db, "last_sync_error", field, sizeof(field)), 0);
    ASSERT_STR_EQ(field, "cannot connect to Neo4j");
    ASSERT_EQ(cbm_neo4j_sidecar_field(db, "synced_generation", field, sizeof(field)), 0);
    ASSERT_STR_EQ(field, "u1g2");
    /* The next successful sync clears it. */
    ASSERT_EQ(cbm_neo4j_sidecar_write(db, &c, "u1g3"), 0);
    ASSERT_EQ(cbm_neo4j_sidecar_field(db, "last_sync_error", field, sizeof(field)), 0);
    ASSERT_STR_EQ(field, "");
    th_rmtree(dir);
    PASS();
}

TEST(neo4j_unavailable_graph_read_is_an_error) {
    /* A graph read Neo4j cannot serve is refused, never answered locally. */
    char *reply = cbm_mcp_graph_unavailable_result("trace_path", "Neo4j unreachable");
    ASSERT_NOT_NULL(reply);
    yyjson_doc *d = yyjson_read(reply, strlen(reply), 0);
    ASSERT_NOT_NULL(d);
    yyjson_val *root = yyjson_doc_get_root(d);
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(root, "isError")));
    yyjson_val *content = yyjson_obj_get(root, "content");
    ASSERT_EQ((int)yyjson_arr_size(content), 1);
    const char *text = yyjson_get_str(yyjson_obj_get(yyjson_arr_get(content, 0), "text"));
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(strstr(text, "trace_path"));
    ASSERT_NOT_NULL(strstr(text, "Neo4j unreachable"));
    ASSERT_NOT_NULL(strstr(text, "never fall back"));
    yyjson_val *gb = yyjson_obj_get(yyjson_obj_get(root, "structuredContent"), "graph_backend");
    if (gb) {
        ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(gb, "status")), "unavailable");
        ASSERT_NULL(yyjson_obj_get(gb, "served_by"));
    }
    yyjson_doc_free(d);
    free(reply);
    PASS();
}

TEST(neo4j_inherited_sync_failure_fails_the_run) {
    clear_neo4j_env();
    char *dir = th_mktempdir("cbm_n4_inherit");
    ASSERT_NOT_NULL(dir);
    char db[512];
    snprintf(db, sizeof(db), "%s/proj.db", dir);
    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "proj", dir);
    ASSERT_EQ(cbm_store_generation_advance(s), CBM_STORE_OK);
    cbm_store_close(s);

    char err[512];
    /* A plain SQLite project has nothing to sync. */
    ASSERT_EQ(cbm_neo4j_after_publish(db, "proj", NULL, false, err, sizeof(err)), 0);

    /* Neo4j-backed (marker present) but Neo4j unreachable: a re-index that
     * inherits the backend fails instead of publishing a local-only graph. */
    cbm_neo4j_config_t c;
    ASSERT_EQ(cbm_neo4j_config_resolve("http://127.0.0.1:1", "neo4j", "pw", NULL, &c, err,
                                       sizeof(err)),
              0);
    ASSERT_EQ(cbm_neo4j_sidecar_write(db, &c, ""), 0);
    cbm_neo4j_config_wipe(&c);
    cbm_setenv("NEO4J_PASSWORD", "pw", 1);
    ASSERT_EQ(cbm_neo4j_after_publish(db, "proj", NULL, false, err, sizeof(err)), -1);
    ASSERT_TRUE(err[0] != '\0');
    char field[256];
    ASSERT_EQ(cbm_neo4j_sidecar_field(db, "last_sync_error", field, sizeof(field)), 0);
    ASSERT_TRUE(field[0] != '\0');

    /* A malformed marker fails too, rather than silently going local. */
    char marker[600];
    ASSERT_EQ(cbm_neo4j_sidecar_path(db, marker, sizeof(marker)), 0);
    FILE *f = fopen(marker, "wb");
    ASSERT_NOT_NULL(f);
    fputs("{not json", f);
    fclose(f);
    ASSERT_EQ(cbm_neo4j_after_publish(db, "proj", NULL, false, err, sizeof(err)), -1);

    clear_neo4j_env();
    th_rmtree(dir);
    PASS();
}

/* ── Live server (optional) ──────────────────────────────────────── */

static void live_node(cbm_store_t *s, const char *name, int64_t *id) {
    char qn[128];
    snprintf(qn, sizeof(qn), "live.%s", name);
    cbm_node_t n = {.project = "live",
                    .label = "Function",
                    .name = name,
                    .qualified_name = qn,
                    .file_path = "m.c",
                    .start_line = 1,
                    .end_line = 2,
                    .properties_json = "{\"complexity\":3,\"tags\":[\"a\",\"b\"]}"};
    *id = cbm_store_upsert_node(s, &n);
}

static void live_edge(cbm_store_t *s, int64_t a, int64_t b, const char *type) {
    cbm_edge_t e = {
        .project = "live", .source_id = a, .target_id = b, .type = type, .properties_json = "{}"};
    cbm_store_insert_edge(s, &e);
}

/* Compare a BFS answered by SQLite with the same BFS answered by Neo4j. */
static int bfs_signature(cbm_store_t *s, int64_t start, const char *dir, bool multi, char *buf,
                         size_t sz) {
    cbm_traverse_result_t tr;
    bool trunc = false;
    int rc = multi ? cbm_store_bfs_multi(s, &start, 1, dir, NULL, 0, 5, 100, &tr, &trunc)
                   : cbm_store_bfs(s, start, dir, NULL, 0, 5, 100, &tr);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    size_t off = 0;
    for (int i = 0; i < tr.visited_count && off < sz; i++) {
        off += (size_t)snprintf(buf + off, sz - off, "%s@%d,", tr.visited[i].node.name,
                                tr.visited[i].hop);
    }
    for (int i = 0; i < tr.edge_count && off < sz; i++) {
        off += (size_t)snprintf(buf + off, sz - off, "%s>%s,", tr.edges[i].from_name,
                                tr.edges[i].to_name);
    }
    cbm_store_traverse_free(&tr);
    return CBM_STORE_OK;
}

/* One string describing what each newly routed read returns, so the local
 * and the Neo4j answer for the same database can be compared verbatim. */
static void routed_signature(cbm_store_t *s, int64_t b, char *buf, size_t sz) {
    size_t off = 0;
#define SIG(...)                                                                                   \
    do {                                                                                           \
        if (off < sz) {                                                                            \
            off += (size_t)snprintf(buf + off, sz - off, __VA_ARGS__);                             \
        }                                                                                          \
    } while (0)
    cbm_search_params_t p = {.project = "live",
                             .name_pattern = "^[ab]",
                             .min_degree = -1,
                             .max_degree = -1,
                             .limit = 10};
    cbm_search_output_t so = {0};
    SIG("search rc=%d:", cbm_store_search(s, &p, &so));
    for (int i = 0; i < so.count; i++) {
        SIG("%s/%d/%d,", so.results[i].node.name, so.results[i].in_degree,
            so.results[i].out_degree);
    }
    SIG(" total=%d", so.total);
    cbm_store_search_free(&so);
    cbm_search_params_t p2 = {
        .project = "live", .min_degree = 1, .max_degree = -1, .limit = 2, .offset = 1};
    SIG(" | page rc=%d:", cbm_store_search(s, &p2, &so));
    for (int i = 0; i < so.count; i++) {
        SIG("%s,", so.results[i].node.name);
    }
    SIG(" total=%d", so.total);
    cbm_store_search_free(&so);

    cbm_schema_info_t sch = {0};
    SIG(" | schema rc=%d:", cbm_store_get_schema(s, "live", &sch));
    for (int i = 0; i < sch.node_label_count; i++) {
        SIG("%s=%d[", sch.node_labels[i].label, sch.node_labels[i].count);
        for (int k = 0; k < sch.node_labels[i].property_count; k++) {
            SIG("%s ", sch.node_labels[i].properties[k]);
        }
        SIG("]");
    }
    for (int i = 0; i < sch.edge_type_count; i++) {
        SIG("%s=%d[", sch.edge_types[i].type, sch.edge_types[i].count);
        for (int k = 0; k < sch.edge_types[i].property_count; k++) {
            SIG("%s ", sch.edge_types[i].properties[k]);
        }
        SIG("]");
    }
    cbm_store_schema_free(&sch);

    cbm_node_t *nodes = NULL;
    int n = 0;
    SIG(" | qn_suffix rc=%d:", cbm_store_find_nodes_by_qn_suffix(s, "live", "B", &nodes, &n));
    for (int i = 0; i < n; i++) {
        SIG("%s,", nodes[i].qualified_name);
    }
    cbm_store_free_nodes(nodes, n);

    char **files = NULL;
    int nf = 0;
    SIG(" | files rc=%d:", cbm_store_list_files(s, "live", &files, &nf));
    for (int i = 0; i < nf; i++) {
        SIG("%s,", files[i]);
        free(files[i]);
    }
    free(files);

    char **callers = NULL;
    char **callees = NULL;
    int nc = 0;
    int ne = 0;
    SIG(" | neighbors rc=%d:",
        cbm_store_node_neighbor_names(s, b, 10, &callers, &nc, &callees, &ne));
    for (int i = 0; i < nc; i++) {
        SIG("<%s", callers[i]);
        free(callers[i]);
    }
    for (int i = 0; i < ne; i++) {
        SIG(">%s", callees[i]);
        free(callees[i]);
    }
    free(callers);
    free(callees);
#undef SIG
}

/* The live test needs a real Neo4j server, so the suite registers it only
 * when one is configured (see SUITE below) -- never a silent skip. */
static bool live_neo4j_configured(void) {
    const char *uri = getenv("CBM_TEST_NEO4J_URI");
    return uri && uri[0];
}

TEST(neo4j_live_sync_and_read) {
    const char *uri = getenv("CBM_TEST_NEO4J_URI");
    const char *pw = getenv("CBM_TEST_NEO4J_PASSWORD");
    ASSERT_TRUE(uri && uri[0]);
    clear_neo4j_env();
    char *dir = th_mktempdir("cbm_n4_live");
    ASSERT_NOT_NULL(dir);
    char db[512];
    snprintf(db, sizeof(db), "%s/live.db", dir);
    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "live", dir);
    int64_t a;
    int64_t b;
    int64_t c;
    int64_t d;
    live_node(s, "a", &a);
    live_node(s, "b", &b);
    live_node(s, "c", &c);
    live_node(s, "d", &d);
    live_edge(s, a, b, "CALLS");
    live_edge(s, b, c, "CALLS");
    live_edge(s, c, a, "CALLS"); /* cycle back to the root */
    live_edge(s, a, d, "USAGE");
    ASSERT_EQ(cbm_store_generation_advance(s), CBM_STORE_OK);

    char sql_out[2][1024];
    ASSERT_EQ(bfs_signature(s, a, "outbound", false, sql_out[0], sizeof(sql_out[0])), CBM_STORE_OK);
    ASSERT_EQ(bfs_signature(s, c, "inbound", true, sql_out[1], sizeof(sql_out[1])), CBM_STORE_OK);
    cbm_store_close(s);

    cbm_neo4j_config_t cfg;
    char err[512];
    ASSERT_EQ(cbm_neo4j_config_resolve(uri, NULL, pw, NULL, &cfg, err, sizeof(err)), 0);
    ASSERT_EQ(cbm_neo4j_after_publish(db, "live", &cfg, false, err, sizeof(err)), 0);

    /* The attach serves the same answers from Neo4j. */
    s = cbm_store_open_path_query(db);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_neo4j_store_attach(s, db, "live", err, sizeof(err)), CBM_NEO4J_ATTACHED);
    ASSERT_EQ(cbm_store_count_nodes(s, "live"), 4);
    ASSERT_EQ(cbm_store_count_edges_by_type(s, "live", "CALLS"), 3);
    char n4_out[2][1024];
    ASSERT_EQ(bfs_signature(s, a, "outbound", false, n4_out[0], sizeof(n4_out[0])), CBM_STORE_OK);
    ASSERT_EQ(bfs_signature(s, c, "inbound", true, n4_out[1], sizeof(n4_out[1])), CBM_STORE_OK);
    ASSERT_STR_EQ(n4_out[0], sql_out[0]);
    ASSERT_STR_EQ(n4_out[1], sql_out[1]);
    cbm_node_t n = {0};
    ASSERT_EQ(cbm_store_find_node_by_qn(s, "live", "live.b", &n), CBM_STORE_OK);
    ASSERT_EQ(n.id, b);
    ASSERT_NOT_NULL(strstr(n.properties_json, "complexity"));
    cbm_node_free_fields(&n);

    /* Every newly routed read: identical answers from Neo4j and from the
     * local index of the same generation. */
    cbm_store_t *local = cbm_store_open_path_query(db);
    ASSERT_NOT_NULL(local);
    static char local_sig[4096];
    static char remote_sig[4096];
    routed_signature(local, b, local_sig, sizeof(local_sig));
    cbm_store_backend_usage_reset(s);
    routed_signature(s, b, remote_sig, sizeof(remote_sig));
    ASSERT_STR_EQ(remote_sig, local_sig);
    cbm_store_backend_usage_t usage;
    cbm_store_backend_usage(s, &usage);
    ASSERT_TRUE(usage.remote_queries >= 7); /* every read went to Neo4j */
    ASSERT_EQ(usage.local_reads, 0);
    cbm_store_close(local);
    cbm_store_close(s);

    /* A rejected login is not retried right away (Neo4j counts each one
     * toward its lockout); the right password is unaffected. */
    cbm_neo4j_config_t bad = cfg;
    snprintf(bad.password, sizeof(bad.password), "definitely-wrong");
    cbm_neo4j_t *badc = cbm_neo4j_connect(&bad, err, sizeof(err));
    ASSERT_NOT_NULL(badc);
    ASSERT_EQ(cbm_neo4j_ping(badc), -1);
    ASSERT_NULL(strstr(cbm_neo4j_error(badc), "not retrying"));
    ASSERT_EQ(cbm_neo4j_ping(badc), -1);
    ASSERT_NOT_NULL(strstr(cbm_neo4j_error(badc), "not retrying"));
    cbm_neo4j_close(badc);
    cbm_neo4j_t *goodc = cbm_neo4j_connect(&cfg, err, sizeof(err));
    ASSERT_NOT_NULL(goodc);
    ASSERT_EQ(cbm_neo4j_ping(goodc), 0);
    cbm_neo4j_close(goodc);
    cbm_neo4j_test_reset_auth_backoff();

    /* Native Cypher sees flattened scalar properties and binds $project. */
    cbm_cypher_result_t r;
    ASSERT_EQ(cbm_neo4j_native_query(db, "live",
                                     "MATCH (n:CbmNode {db:$db, gen:$gen, project:$project}) "
                                     "RETURN sum(n.complexity) AS total",
                                     0, &r),
              0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "12");
    cbm_cypher_result_free(&r);

    /* The graph vanishes from Neo4j (database cleared by hand): an unchanged
     * re-run must re-upload it, not trust its own marker. */
    cbm_neo4j_t *conn = cbm_neo4j_connect(&cfg, err, sizeof(err));
    ASSERT_NOT_NULL(conn);
    ASSERT_EQ(cbm_neo4j_drop_project(conn, "live", err, sizeof(err)), 0);
    ASSERT_EQ(cbm_neo4j_after_publish(db, "live", NULL, false, err, sizeof(err)), 0);
    s = cbm_store_open_path_query(db);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_neo4j_store_attach(s, db, "live", err, sizeof(err)), CBM_NEO4J_ATTACHED);
    ASSERT_EQ(cbm_store_count_nodes(s, "live"), 4);
    cbm_store_close(s);

    ASSERT_EQ(cbm_neo4j_drop_project(conn, "live", err, sizeof(err)), 0);
    cbm_neo4j_close(conn);
    cbm_neo4j_config_wipe(&cfg);
    th_rmtree(dir);
    PASS();
}

SUITE(neo4j) {
    RUN_TEST(neo4j_url_defaults);
    RUN_TEST(neo4j_url_port_path_ipv6);
    RUN_TEST(neo4j_url_rejects_bolt_and_garbage);
    RUN_TEST(neo4j_base64_vectors);
    RUN_TEST(neo4j_config_explicit_beats_env);
    RUN_TEST(neo4j_config_rejects_bad_database_and_missing_uri);
    RUN_TEST(neo4j_config_meta_has_no_password);
    RUN_TEST(neo4j_ident_ok);
    RUN_TEST(neo4j_sidecar_roundtrip);
    RUN_TEST(neo4j_store_remote_routing);
    RUN_TEST(neo4j_native_query_is_read_only);
    RUN_TEST(neo4j_rejects_malformed_2xx_bodies);
    RUN_TEST(neo4j_request_with_invalid_utf8_is_still_sent);
    RUN_TEST(neo4j_sidecar_records_last_sync_error);
    RUN_TEST(neo4j_unavailable_graph_read_is_an_error);
    RUN_TEST(neo4j_inherited_sync_failure_fails_the_run);
    RUN_TEST(neo4j_routed_lookups_apply_local_predicates);
    RUN_TEST(neo4j_marker_found_for_renamed_database);
    RUN_TEST(neo4j_served_by_is_named_in_replies);
    if (live_neo4j_configured()) {
        RUN_TEST(neo4j_live_sync_and_read);
    } else {
        printf("  (neo4j live test not registered: set CBM_TEST_NEO4J_URI and "
               "CBM_TEST_NEO4J_PASSWORD to run it against a server)\n");
    }
}
