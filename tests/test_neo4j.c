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
#include "foundation/limits.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "neo4j/neo4j_client.h"
#include "neo4j/neo4j_http.h"
#include "neo4j/neo4j_store.h"
#include "neo4j/neo4j_sync.h"
#include "mcp/mcp.h"
#include "store/store.h"

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

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
        "MATCH (n) REMOVE n.x",       "FOREACH (x IN [1] | CREATE (:A))",
        /* a quote inside a block comment must not open a string (C1) */
        "MATCH (n) /* ' */ DETACH DELETE n /* ' */",
        /* a backslash inside a backtick identifier is not an escape */
        "MATCH (n) WITH n AS `a\\` DETACH DELETE n RETURN `x`",
        "INSERT (:A)", "USE other MATCH (n) RETURN n"};
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

/* A one-statement Neo4j response of n rows: ["dir/f<i>.c", {"i": i}] with
 * columns path and props, or just the path when two_cols is false. */
static char *big_body(int n, bool two_cols) {
    size_t cap = (size_t)n * 64 + 256;
    char *s = malloc(cap);
    if (!s) {
        return NULL;
    }
    size_t len = (size_t)snprintf(s, cap, "{\"results\":[{\"columns\":[%s],\"data\":[",
                                  two_cols ? "\"path\",\"props\"" : "\"path\"");
    for (int i = 0; i < n; i++) {
        if (two_cols) {
            len += (size_t)snprintf(s + len, cap - len,
                                    "%s{\"row\":[\"dir/f%07d.c\",{\"i\":%d}],\"meta\":[null,null]}",
                                    i ? "," : "", i, i);
        } else {
            len += (size_t)snprintf(s + len, cap - len,
                                    "%s{\"row\":[\"dir/f%07d.c\"],\"meta\":[null]}", i ? "," : "",
                                    i);
        }
    }
    (void)snprintf(s + len, cap - len, "]}],\"errors\":[]}");
    return s;
}

TEST(neo4j_large_results_are_read_in_linear_time) {
    /* Rows are objects, so reading row i by index steps through every row
     * before it. search_code lists all of a project's files (272k on the
     * production graph) and spent 86 s there before any grep ran. In a
     * benchmark the old way took 12 s for 100k rows and 156 s for 272k; the
     * iterator takes milliseconds. The bound is loose enough for a slow CI
     * machine and far below the quadratic cost of 200k rows. */
    enum { N = 200000, BOUND_MS = 3000 };
    char *body = big_body(N, false);
    ASSERT_NOT_NULL(body);
    char **files = NULL;
    int nf = 0;
    uint64_t t0 = cbm_now_ms();
    ASSERT_EQ(cbm_neo4j_test_column_strings(body, &files, &nf), CBM_STORE_OK);
    uint64_t list_ms = cbm_now_ms() - t0;
    free(body);
    ASSERT_EQ(nf, N);
    char want[32];
    for (int i = 0; i < nf; i++) {
        snprintf(want, sizeof(want), "dir/f%07d.c", i);
        ASSERT_STR_EQ(files[i], want); /* every row, in order */
        free(files[i]);
    }
    free(files);
    ASSERT_LT(list_ms, BOUND_MS);

    /* query_graph's native rows: same walk, two columns, a map value, and the
     * 100000-row cap when max_rows is 0. */
    body = big_body(N, true);
    ASSERT_NOT_NULL(body);
    cbm_cypher_result_t r;
    t0 = cbm_now_ms();
    ASSERT_EQ(cbm_neo4j_test_native_rows(body, 0, &r), 0);
    uint64_t native_ms = cbm_now_ms() - t0;
    ASSERT_EQ(r.col_count, 2);
    ASSERT_STR_EQ(r.columns[0], "path");
    ASSERT_STR_EQ(r.columns[1], "props");
    ASSERT_EQ(r.row_count, 100000);
    ASSERT_TRUE(r.truncated);
    for (int i = 0; i < r.row_count; i += 9999) {
        snprintf(want, sizeof(want), "dir/f%07d.c", i);
        ASSERT_STR_EQ(r.rows[i][0], want);
    }
    ASSERT_STR_EQ(r.rows[99999][0], "dir/f0099999.c");
    ASSERT_STR_EQ(r.rows[99999][1], "{\"i\":99999}");
    cbm_cypher_result_free(&r);
    ASSERT_LT(native_ms, BOUND_MS);

    /* A small max_rows keeps the first rows only. */
    ASSERT_EQ(cbm_neo4j_test_native_rows(body, 3, &r), 0);
    free(body);
    ASSERT_EQ(r.row_count, 3);
    ASSERT_TRUE(r.truncated);
    ASSERT_STR_EQ(r.rows[2][0], "dir/f0000002.c");
    cbm_cypher_result_free(&r);
    PASS();
}

TEST(neo4j_chunked_body_decodes_across_any_split) {
    /* Two chunks, a zero chunk with a trailer, then the next response's
     * first bytes (keep-alive surplus that must stay intact). */
    static const char raw[] = "5\r\nhello\r\n"
                              "7;ext=1\r\n, world\r\n"
                              "0\r\nX-T: 1\r\n\r\n"
                              "HTTP/1.1";
    const size_t raw_len = sizeof(raw) - 1;
    const size_t body_len = raw_len - strlen("HTTP/1.1");
    for (size_t step = 1; step <= raw_len; step++) {
        char *out = NULL;
        size_t out_len = 0;
        size_t consumed = 0;
        size_t buffered = 0;
        ASSERT_EQ(
            cbm_neo4j_test_dechunk(raw, raw_len, step, &out, &out_len, &consumed, &buffered), 0);
        ASSERT_EQ(out_len, 12);
        ASSERT_EQ(memcmp(out, "hello, world", 12), 0);
        ASSERT_EQ(consumed, body_len);
        /* Whatever of the next response already arrived is untouched. */
        ASSERT_TRUE(buffered >= consumed && buffered <= raw_len);
        ASSERT_EQ(memcmp(out + consumed, raw + consumed, buffered - consumed), 0);
        free(out);
    }
    /* Cut anywhere before the final empty line: truncated, never a body. */
    for (size_t cut = 0; cut < body_len; cut++) {
        char *out = NULL;
        size_t out_len = 0;
        size_t consumed = 0;
        size_t buffered = 0;
        ASSERT_EQ(cbm_neo4j_test_dechunk(raw, cut, 3, &out, &out_len, &consumed, &buffered), 1);
    }
    char *out = NULL;
    size_t out_len = 0;
    size_t consumed = 0;
    size_t buffered = 0;
    ASSERT_EQ(cbm_neo4j_test_dechunk("zz\r\nab\r\n0\r\n\r\n", 14, 4, &out, &out_len, &consumed,
                                     &buffered),
              -1);
    /* A chunk size larger than anything that could follow is "need more",
     * never an overflowed bounds check. */
    ASSERT_EQ(cbm_neo4j_test_dechunk("ffffffffffffffff\r\nab", 20, 5, &out, &out_len, &consumed,
                                     &buffered),
              1);

    /* A 48 MB body in 8 KB chunks read 16 KB at a time, through the same
     * read loop the socket uses. Copying and decoding the whole body again
     * after every read would move ~150 GB here; now each byte moves once. */
    enum { CHUNK = 8192, CHUNKS = 6000, STEP = 16384, BOUND_MS = 3000 };
    size_t big_len = (size_t)CHUNKS * (CHUNK + 10) + 16;
    char *big = malloc(big_len);
    ASSERT_NOT_NULL(big);
    size_t n = 0;
    for (int c = 0; c < CHUNKS; c++) {
        n += (size_t)snprintf(big + n, big_len - n, "%x\r\n", CHUNK);
        memset(big + n, 'a' + c % 26, CHUNK);
        n += CHUNK;
        big[n++] = '\r';
        big[n++] = '\n';
    }
    memcpy(big + n, "0\r\n\r\n", 5);
    n += 5;
    uint64_t t0 = cbm_now_ms();
    ASSERT_EQ(cbm_neo4j_test_dechunk(big, n, STEP, &out, &out_len, &consumed, &buffered), 0);
    uint64_t ms = cbm_now_ms() - t0;
    ASSERT_EQ(out_len, (size_t)CHUNKS * CHUNK);
    ASSERT_EQ(consumed, n);
    for (int c = 0; c < CHUNKS; c += 97) {
        ASSERT_EQ(out[(size_t)c * CHUNK], 'a' + c % 26);
        ASSERT_EQ(out[(size_t)c * CHUNK + CHUNK - 1], 'a' + c % 26);
    }
    free(out);
    free(big);
    ASSERT_LT(ms, BOUND_MS);
    PASS();
}

TEST(neo4j_row_conversion_edge_cases) {
    char **files = NULL;
    int nf = -1;
    /* No rows: success, nothing allocated. */
    ASSERT_EQ(cbm_neo4j_test_column_strings("{\"results\":[{\"columns\":[\"p\"],\"data\":[]}],"
                                            "\"errors\":[]}",
                                            &files, &nf),
              CBM_STORE_OK);
    ASSERT_EQ(nf, 0);
    ASSERT_NULL(files);
    /* null and non-string cells: "" and the JSON text, as before. */
    ASSERT_EQ(cbm_neo4j_test_column_strings("{\"results\":[{\"columns\":[\"p\"],\"data\":["
                                            "{\"row\":[null]},{\"row\":[7]},{\"row\":[\"a\"]}]}],"
                                            "\"errors\":[]}",
                                            &files, &nf),
              CBM_STORE_OK);
    ASSERT_EQ(nf, 3);
    ASSERT_STR_EQ(files[0], "");
    ASSERT_STR_EQ(files[1], "7");
    ASSERT_STR_EQ(files[2], "a");
    for (int i = 0; i < nf; i++) {
        free(files[i]);
    }
    free(files);
    /* Native rows print null as "null". */
    cbm_cypher_result_t r;
    ASSERT_EQ(cbm_neo4j_test_native_rows("{\"results\":[{\"columns\":[\"a\",\"b\"],\"data\":["
                                         "{\"row\":[null,[1,2]]}]}],\"errors\":[]}",
                                         0, &r),
              0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_FALSE(r.truncated);
    ASSERT_STR_EQ(r.rows[0][0], "null");
    ASSERT_STR_EQ(r.rows[0][1], "[1,2]");
    cbm_cypher_result_free(&r);
    /* A body the client would refuse is refused here too. */
    ASSERT_EQ(cbm_neo4j_test_column_strings("{\"results\":[{\"data\":[]}]}", &files, &nf),
              CBM_STORE_ERR);
    ASSERT_EQ(cbm_neo4j_test_native_rows("not json", 0, &r), -1);
    ASSERT_NOT_NULL(r.error);
    cbm_cypher_result_free(&r);
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

/* search_code scans the files cbm_store_list_files names, so Neo4j must name
 * exactly the files SQLite names -- and, for an ordinary index (one with File
 * nodes), find them with the indexed File query alone instead of reading
 * every node of the project. */
typedef struct {
    const char *label;
    const char *name;
    const char *path;
} lf_node_t;

typedef struct {
    const char *project;
    lf_node_t nodes[6];
    const char *expected;
    uint64_t queries;
} lf_case_t;

TEST(neo4j_live_list_files_matches_local) {
    static const lf_case_t cases[] = {
        /* An ordinary index: only File paths, even with code in a file that
         * has no File node, a Folder, and a node without a path. */
        {"lf-files",
         {{"File", "a.c", "src/a.c"},
          {"File", "b.c", "src/b.c"},
          {"Function", "fa", "src/a.c"},
          {"Function", "fgen", "src/gen.c"},
          {"Folder", "src", "src"},
          {"Function", "nopath", ""}},
         "src/a.c,src/b.c,",
         1},
        /* No File nodes (an older graph): every non-Folder node path. */
        {"lf-nofiles",
         {{"Function", "one", "x/one.c"},
          {"Function", "one2", "x/one.c"},
          {"Function", "two", "x/two.c"},
          {"Folder", "x", "x"}},
         "x/one.c,x/two.c,",
         2},
        /* File nodes exist but name no path: nothing, as in SQLite. */
        {"lf-emptyfile",
         {{"File", "f", ""}, {"Function", "y", "y.c"}},
         "",
         2},
    };
    const char *uri = getenv("CBM_TEST_NEO4J_URI");
    const char *pw = getenv("CBM_TEST_NEO4J_PASSWORD");
    ASSERT_TRUE(uri && uri[0]);
    clear_neo4j_env();
    char *dir = th_mktempdir("cbm_n4_files");
    ASSERT_NOT_NULL(dir);
    cbm_neo4j_config_t cfg;
    char err[512];
    ASSERT_EQ(cbm_neo4j_config_resolve(uri, NULL, pw, NULL, &cfg, err, sizeof(err)), 0);
    cbm_neo4j_t *conn = cbm_neo4j_connect(&cfg, err, sizeof(err));
    ASSERT_NOT_NULL(conn);

    for (size_t c = 0; c < sizeof(cases) / sizeof(cases[0]); c++) {
        const lf_case_t *k = &cases[c];
        /* A failed earlier run returns before its cleanup below. */
        (void)cbm_neo4j_drop_project(conn, k->project, err, sizeof(err));
        char db[512];
        snprintf(db, sizeof(db), "%s/%s.db", dir, k->project);
        cbm_store_t *s = cbm_store_open_path(db);
        ASSERT_NOT_NULL(s);
        cbm_store_upsert_project(s, k->project, dir);
        for (size_t i = 0; i < sizeof(k->nodes) / sizeof(k->nodes[0]) && k->nodes[i].label; i++) {
            char qn[128];
            snprintf(qn, sizeof(qn), "%s.%s", k->project, k->nodes[i].name);
            cbm_node_t n = {.project = k->project,
                            .label = k->nodes[i].label,
                            .name = k->nodes[i].name,
                            .qualified_name = qn,
                            .file_path = k->nodes[i].path,
                            .start_line = 1,
                            .end_line = 2,
                            .properties_json = "{}"};
            ASSERT_TRUE(cbm_store_upsert_node(s, &n) > 0);
        }
        ASSERT_EQ(cbm_store_generation_advance(s), CBM_STORE_OK);
        cbm_store_close(s);
        ASSERT_EQ(cbm_neo4j_after_publish(db, k->project, &cfg, false, err, sizeof(err)), 0);

        char sig[2][512];
        for (int remote = 0; remote < 2; remote++) {
            s = cbm_store_open_path_query(db);
            ASSERT_NOT_NULL(s);
            if (remote) {
                ASSERT_EQ(cbm_neo4j_store_attach(s, db, k->project, err, sizeof(err)),
                          CBM_NEO4J_ATTACHED);
                cbm_store_backend_usage_reset(s);
            }
            char **files = NULL;
            int nf = 0;
            ASSERT_EQ(cbm_store_list_files(s, k->project, &files, &nf), CBM_STORE_OK);
            size_t off = 0;
            sig[remote][0] = '\0';
            for (int i = 0; i < nf; i++) {
                off += (size_t)snprintf(sig[remote] + off, sizeof(sig[remote]) - off, "%s,",
                                        files[i]);
                free(files[i]);
            }
            free(files);
            if (remote) {
                cbm_store_backend_usage_t usage;
                cbm_store_backend_usage(s, &usage);
                ASSERT_EQ(usage.remote_queries, k->queries);
                ASSERT_EQ(usage.local_reads, 0);
            }
            cbm_store_close(s);
        }
        ASSERT_STR_EQ(sig[0], k->expected); /* SQLite */
        ASSERT_STR_EQ(sig[1], sig[0]);      /* Neo4j answers the same */
        if (c == 0) {
            /* The File query must read File nodes through the label index.
             * With the path test inside the MATCH, Neo4j chose the file_path
             * index instead, which holds every node of the project. */
            char explain[1024];
            snprintf(explain, sizeof(explain), "EXPLAIN %s",
                     cbm_neo4j_test_list_file_nodes_cypher());
            cbm_neo4j_batch_t b;
            ASSERT_EQ(cbm_neo4j_batch_init(&b), 0);
            yyjson_mut_val *p = cbm_neo4j_batch_add(&b, explain);
            yyjson_mut_obj_add_strcpy(b.doc, p, "db", k->project);
            yyjson_mut_obj_add_int(b.doc, p, "gen", 1);
            yyjson_mut_obj_add_strcpy(b.doc, p, "project", k->project);
            cbm_neo4j_result_t r;
            ASSERT_EQ(cbm_neo4j_batch_run(conn, &b, &r), 0);
            yyjson_val *root =
                yyjson_obj_get(yyjson_obj_get(yyjson_arr_get(r.results, 0), "plan"), "root");
            const char *plan = yyjson_get_str(yyjson_obj_get(root, "string-representation"));
            ASSERT_NOT_NULL(plan);
            ASSERT_NOT_NULL(strstr(plan, "CbmNode(db, gen, label)"));
            cbm_neo4j_result_free(&r);
            cbm_neo4j_batch_free(&b);
        }
        ASSERT_EQ(cbm_neo4j_drop_project(conn, k->project, err, sizeof(err)), 0);
    }

    cbm_neo4j_close(conn);
    cbm_neo4j_config_wipe(&cfg);
    th_rmtree(dir);
    PASS();
}

TEST(neo4j_live_large_file_list_matches_local) {
    /* The whole path search_code's scope step takes -- store open, attach,
     * cbm_store_list_files over HTTP -- on 100k files, plus native query_graph
     * over as many rows. Neo4j must return exactly SQLite's list, and quickly:
     * reading the rows used to take 12 s for 100k in a benchmark. */
    enum { N = 100000, BOUND_MS = 10000 };
    static const char project[] = "lf-large";
    const char *uri = getenv("CBM_TEST_NEO4J_URI");
    const char *pw = getenv("CBM_TEST_NEO4J_PASSWORD");
    ASSERT_TRUE(uri && uri[0]);
    clear_neo4j_env();
    char *dir = th_mktempdir("cbm_n4_large");
    ASSERT_NOT_NULL(dir);
    cbm_neo4j_config_t cfg;
    char err[512];
    ASSERT_EQ(cbm_neo4j_config_resolve(uri, NULL, pw, NULL, &cfg, err, sizeof(err)), 0);
    cbm_neo4j_t *conn = cbm_neo4j_connect(&cfg, err, sizeof(err));
    ASSERT_NOT_NULL(conn);
    (void)cbm_neo4j_drop_project(conn, project, err, sizeof(err));

    char db[512];
    snprintf(db, sizeof(db), "%s/%s.db", dir, project);
    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, project, dir);
    ASSERT_EQ(cbm_store_begin(s), CBM_STORE_OK);
    for (int i = 0; i < N; i++) {
        char name[32];
        char qn[64];
        char path[64];
        /* Descending, so the sorted list is not simply insertion order. */
        snprintf(name, sizeof(name), "f%07d.c", N - 1 - i);
        snprintf(qn, sizeof(qn), "%s.%s", project, name);
        snprintf(path, sizeof(path), "d%02d/%s", i % 37, name);
        cbm_node_t n = {.project = project,
                        .label = "File",
                        .name = name,
                        .qualified_name = qn,
                        .file_path = path,
                        .start_line = 1,
                        .end_line = 1,
                        .properties_json = "{}"};
        ASSERT_TRUE(cbm_store_upsert_node(s, &n) > 0);
    }
    ASSERT_EQ(cbm_store_commit(s), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_generation_advance(s), CBM_STORE_OK);
    cbm_store_close(s);
    ASSERT_EQ(cbm_neo4j_after_publish(db, project, &cfg, false, err, sizeof(err)), 0);

    char **local = NULL;
    int nlocal = 0;
    s = cbm_store_open_path_query(db);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_list_files(s, project, &local, &nlocal), CBM_STORE_OK);
    cbm_store_close(s);
    ASSERT_EQ(nlocal, N);

    uint64_t t0 = cbm_now_ms();
    s = cbm_store_open_path_query(db);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_neo4j_store_attach(s, db, project, err, sizeof(err)), CBM_NEO4J_ATTACHED);
    char **remote = NULL;
    int nremote = 0;
    ASSERT_EQ(cbm_store_list_files(s, project, &remote, &nremote), CBM_STORE_OK);
    uint64_t list_ms = cbm_now_ms() - t0;
    cbm_store_close(s);
    ASSERT_EQ(nremote, nlocal);
    for (int i = 0; i < nlocal; i++) {
        ASSERT_STR_EQ(remote[i], local[i]);
        free(remote[i]);
    }
    free(remote);
    printf("  [lf-large] %d files listed from Neo4j in %llu ms\n", N, (unsigned long long)list_ms);
    ASSERT_LT(list_ms, BOUND_MS);

    cbm_cypher_result_t r;
    t0 = cbm_now_ms();
    ASSERT_EQ(cbm_neo4j_native_query(db, project,
                                     "MATCH (n:CbmNode {db: $db, gen: $gen, label: 'File'}) "
                                     "RETURN n.file_path AS path ORDER BY path",
                                     0, &r),
              0);
    uint64_t native_ms = cbm_now_ms() - t0;
    ASSERT_EQ(r.row_count, N);
    for (int i = 0; i < N; i++) {
        ASSERT_STR_EQ(r.rows[i][0], local[i]);
        free(local[i]);
    }
    free(local);
    cbm_cypher_result_free(&r);
    printf("  [lf-large] %d native rows in %llu ms\n", N, (unsigned long long)native_ms);
    ASSERT_LT(native_ms, BOUND_MS);

    ASSERT_EQ(cbm_neo4j_drop_project(conn, project, err, sizeof(err)), 0);
    cbm_neo4j_close(conn);
    cbm_neo4j_config_wipe(&cfg);
    th_rmtree(dir);
    PASS();
}

/* ── query_graph's engine dialect scoped for Neo4j ─────────────────── */

/* 0 when the query scopes to want (NULL: is refused with a reason). */
static int scope_expect(const char *in, const char *want) {
    char why[128];
    char *got = cbm_neo4j_scope_query(in, why, sizeof(why));
    if (!want) {
        if (got) {
            printf("  scope(%s) = %s, want refusal\n", in, got);
            free(got);
            return 1;
        }
        ASSERT_TRUE(why[0] != '\0');
        return 0;
    }
    if (!got || strcmp(got, want) != 0) {
        printf("  scope(%s)\n    got  %s\n    want %s\n", in, got ? got : "(null)", want);
        free(got);
        return 1;
    }
    free(got);
    return 0;
}
#define SCOPE_EXPECT(in, want) ASSERT_EQ(scope_expect(in, want), 0)

TEST(neo4j_scope_query_scopes_every_node_pattern) {
    SCOPE_EXPECT("MATCH (n) RETURN count(n)",
                 "MATCH (n:CbmNode {db: $db, gen: $gen, project: $project}) RETURN count(n)");
    /* regex dialects differ (POSIX ERE vs Java) */
    SCOPE_EXPECT("MATCH (n) WHERE n.name =~ 'v[[:digit:]]+' RETURN count(n)", NULL);
    SCOPE_EXPECT("MATCH (n {project: 'other'}) RETURN n", NULL);
    /* hop ranges read as the engine reads them: *N is 1..N, all capped */
    {
        int cap = cbm_cypher_max_depth();
        char want[512];
        char q[256];
#define SC ":CbmNode {db: $db, gen: $gen, project: $project}"
        snprintf(want, sizeof(want), "MATCH (a" SC ")-[:CALLS*1..3]->(b" SC ") RETURN count(b)");
        SCOPE_EXPECT("MATCH (a)-[:CALLS*3]->(b) RETURN count(b)", want);
        snprintf(want, sizeof(want), "MATCH (a" SC ")-[*1..%d]->(b" SC ") RETURN b", cap);
        SCOPE_EXPECT("MATCH (a)-[*]->(b) RETURN b", want);
        snprintf(want, sizeof(want), "MATCH (a" SC ")-[r:CALLS*2..%d]->(b" SC ") RETURN b", cap);
        SCOPE_EXPECT("MATCH (a)-[r:CALLS*2..]->(b) RETURN b", want);
        snprintf(want, sizeof(want), "MATCH (a" SC ")<-[*..%d]-(b" SC ") RETURN b", cap);
        SCOPE_EXPECT("MATCH (a)<-[*..]-(b) RETURN b", want);
        if (cap >= 4) {
            SCOPE_EXPECT("MATCH (a)-[*..4]-(b) RETURN b",
                         "MATCH (a" SC ")-[*..4]-(b" SC ") RETURN b");
        }
        snprintf(q, sizeof(q), "MATCH (a)-[*1..%d]->(b) RETURN b", cap + 50);
        snprintf(want, sizeof(want), "MATCH (a" SC ")-[*1..%d]->(b" SC ") RETURN b", cap);
        SCOPE_EXPECT(q, want);
        snprintf(q, sizeof(q), "MATCH (a)-[*%d]->(b) RETURN b", cap + 50);
        SCOPE_EXPECT(q, want);
        snprintf(q, sizeof(q), "MATCH (a)-[*%d..]->(b) RETURN b", cap + 1);
        SCOPE_EXPECT(q, NULL); /* starts past the engine's limit */
        /* a '*' outside a relationship is untouched */
        SCOPE_EXPECT("MATCH (n) RETURN count(*)", "MATCH (n" SC ") RETURN count(*)");
        SCOPE_EXPECT("MATCH (n) RETURN n.start_line * 2", "MATCH (n" SC ") RETURN n.start_line * 2");
#undef SC
    }
    /* the engine compares text, Neo4j typed values: '6' would not match 6 */
    SCOPE_EXPECT("MATCH (n) WHERE n.start_line = '6' RETURN n", NULL);
    SCOPE_EXPECT("MATCH (n {start_line: '-1.5'}) RETURN n", NULL);
    SCOPE_EXPECT("MATCH (n {name: '2fa'}) RETURN n",
                 "MATCH (n :CbmNode {db: $db, gen: $gen, project: $project, name: '2fa'}) RETURN n");
    SCOPE_EXPECT("MATCH ()-[r:CALLS]->() RETURN count(r)",
                 "MATCH (:CbmNode {db: $db, gen: $gen, project: $project})-[r:CALLS]->(:CbmNode {db: $db, gen: $gen, project: $project}) "
                 "RETURN count(r)");
    SCOPE_EXPECT("MATCH (a:Function {name: 'x'})<-[:CALLS]-(b) RETURN b.name",
                 "MATCH (a:CbmNode:Function {db: $db, gen: $gen, project: $project, name: 'x'})<-[:CALLS]-"
                 "(b:CbmNode {db: $db, gen: $gen, project: $project}) RETURN b.name");
    SCOPE_EXPECT("MATCH (a), (b:File|Folder) RETURN a",
                 "MATCH (a:CbmNode {db: $db, gen: $gen, project: $project}), "
                 "(b:CbmNode&(File|Folder) {db: $db, gen: $gen, project: $project}) RETURN a");
    SCOPE_EXPECT("MATCH (n {}) RETURN n", "MATCH (n :CbmNode {db: $db, gen: $gen, project: $project}) RETURN n");
    SCOPE_EXPECT("MATCH (a) OPTIONAL MATCH (a)-[:USAGE]->(u) RETURN a.name, u.name",
                 "MATCH (a:CbmNode {db: $db, gen: $gen, project: $project}) OPTIONAL MATCH "
                 "(a:CbmNode {db: $db, gen: $gen, project: $project})-[:USAGE]->(u:CbmNode {db: $db, gen: $gen, project: $project}) "
                 "RETURN a.name, u.name");
    /* a pattern predicate in WHERE is a pattern too; a call is not */
    SCOPE_EXPECT("MATCH (a) WHERE NOT (a)<-[:CALLS]-() RETURN count(a)",
                 "MATCH (a:CbmNode {db: $db, gen: $gen, project: $project}) WHERE NOT "
                 "(a:CbmNode {db: $db, gen: $gen, project: $project})<-[:CALLS]-(:CbmNode {db: $db, gen: $gen, project: $project}) "
                 "RETURN count(a)");
    /* parentheses inside string literals are text */
    SCOPE_EXPECT("MATCH (n) WHERE n.name = '(x)' RETURN n",
                 "MATCH (n:CbmNode {db: $db, gen: $gen, project: $project}) WHERE n.name = '(x)' RETURN n");
    SCOPE_EXPECT("MATCH (n:A:B|C) RETURN n", NULL);
    /* the engine drops `--` to the end of the line; Neo4j reads a pattern */
    SCOPE_EXPECT("MATCH (a)-[:CALLS]->(b) RETURN count(b) -- (x) RETURN 1", NULL);
    SCOPE_EXPECT("MATCH (a)--(b) RETURN count(b)", NULL);
    SCOPE_EXPECT("MATCH (n) /* x */ RETURN n", NULL);
    SCOPE_EXPECT("MATCH (n) // x\nRETURN n", NULL);
    SCOPE_EXPECT("MATCH (`n`) RETURN `n`", NULL);
    /* the scope keys cannot be supplied (or overridden) by the query */
    SCOPE_EXPECT("MATCH (n {db: 'other'}) RETURN n", NULL);
    SCOPE_EXPECT("MATCH (n {name: 'x', gen: 1}) RETURN n", NULL);
    /* '--' inside a string is text */
    SCOPE_EXPECT("MATCH (n) WHERE n.name = 'a--b' RETURN n",
                 "MATCH (n:CbmNode {db: $db, gen: $gen, project: $project}) WHERE n.name = 'a--b' RETURN n");
    /* a call's parentheses are not a node pattern */
    SCOPE_EXPECT("MATCH (n) RETURN count(n) - 1",
                 "MATCH (n:CbmNode {db: $db, gen: $gen, project: $project}) RETURN count(n) - 1");
    /* engine-only virtual properties have no Neo4j value */
    SCOPE_EXPECT("MATCH (n:Function) WHERE n.in_degree = '0' RETURN n.name", NULL);
    SCOPE_EXPECT("RETURN 1", NULL);
    PASS();
}

/* Rows of a query, one line each, for comparing two stores' answers. */
static void cypher_signature(cbm_store_t *s, const char *q, char *buf, size_t sz) {
    cbm_cypher_result_t r;
    int rc = cbm_cypher_execute(s, q, "lv2", 0, &r);
    size_t off = (size_t)snprintf(buf, sz, "rc=%d err=%s rows=%d:", rc, r.error ? r.error : "",
                                  r.row_count);
    for (int i = 0; i < r.row_count && off < sz; i++) {
        for (int j = 0; j < r.col_count && off < sz; j++) {
            off += (size_t)snprintf(buf + off, sz - off, "%s|", r.rows[i][j]);
        }
        if (off < sz) {
            off += (size_t)snprintf(buf + off, sz - off, ";");
        }
    }
    cbm_cypher_result_free(&r);
}

/* A hub with many callers, answered by the engine over Neo4j (prefetched
 * edges, paged scans, batched reads) exactly as over the local index, in a
 * handful of round trips instead of one per edge and neighbour. */
TEST(neo4j_live_engine_reads_match_local) {
    const char *uri = getenv("CBM_TEST_NEO4J_URI");
    const char *pw = getenv("CBM_TEST_NEO4J_PASSWORD");
    ASSERT_TRUE(uri && uri[0]);
    clear_neo4j_env();
    char *dir = th_mktempdir("cbm_n4_lv2");
    ASSERT_NOT_NULL(dir);
    char db[512];
    snprintf(db, sizeof(db), "%s/lv2.db", dir);
    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "lv2", dir);
    enum { CALLERS = 1200 };
    static int64_t ids[CALLERS + 2];
    cbm_node_t hub = {.project = "lv2", .label = "Function", .name = "h",
                      .qualified_name = "lv2.h", .file_path = "h.c", .start_line = 1,
                      .end_line = 2, .properties_json = "{}"};
    int64_t hub_id = cbm_store_upsert_node(s, &hub);
    cbm_node_t var = {.project = "lv2", .label = "Variable", .name = "g",
                      .qualified_name = "lv2.g", .file_path = "h.c", .start_line = 3,
                      .end_line = 3, .properties_json = "{}"};
    int64_t var_id = cbm_store_upsert_node(s, &var);
    for (int i = 0; i < CALLERS; i++) {
        char name[32];
        char qn[48];
        char file[32];
        snprintf(name, sizeof(name), "c%04d", i);
        snprintf(qn, sizeof(qn), "lv2.%s", name);
        snprintf(file, sizeof(file), "f%03d.c", i / 10);
        cbm_node_t n = {.project = "lv2", .label = "Function", .name = name, .qualified_name = qn,
                        .file_path = file, .start_line = 1 + i % 10, .end_line = 1 + i % 10,
                        .properties_json = "{}"};
        ids[i] = cbm_store_upsert_node(s, &n);
        cbm_edge_t e = {.project = "lv2", .source_id = ids[i], .target_id = hub_id,
                        .type = "CALLS", .properties_json = "{}"};
        cbm_store_insert_edge(s, &e);
        if (i % 100 == 0) {
            cbm_edge_t u = {.project = "lv2", .source_id = ids[i], .target_id = var_id,
                            .type = "USAGE", .properties_json = "{}"};
            cbm_store_insert_edge(s, &u);
        }
    }
    ASSERT_EQ(cbm_store_generation_advance(s), CBM_STORE_OK);
    cbm_store_close(s);

    cbm_neo4j_config_t cfg;
    char err[512];
    ASSERT_EQ(cbm_neo4j_config_resolve(uri, NULL, pw, NULL, &cfg, err, sizeof(err)), 0);
    ASSERT_EQ(cbm_neo4j_after_publish(db, "lv2", &cfg, false, err, sizeof(err)), 0);
    cbm_store_t *local = cbm_store_open_path_query(db);
    cbm_store_t *remote = cbm_store_open_path_query(db);
    ASSERT_NOT_NULL(local);
    ASSERT_NOT_NULL(remote);
    ASSERT_EQ(cbm_neo4j_store_attach(remote, db, "lv2", err, sizeof(err)), CBM_NEO4J_ATTACHED);
    cbm_store_test_set_remote_scan_page(97); /* many short pages */

    static const char *const queries[] = {
        "MATCH (n) RETURN count(n)",
        "MATCH (n) RETURN n.file_path, n.name",
        "MATCH (n:Function) RETURN n.name ORDER BY n.name DESC LIMIT 7",
        "MATCH (a)-[:CALLS]->(b {name: 'h'}) RETURN count(a)",
        "MATCH (a:Function)-[:CALLS]->(b:Function) RETURN b.name, count(a) AS c ORDER BY c DESC",
        "MATCH (h {name: 'h'})<-[:CALLS]-(c) RETURN c.name, c.file_path ORDER BY c.name LIMIT 9",
        "MATCH (a)-[r]-(b {name: 'h'}) RETURN count(r)",
        "MATCH (a:Function) OPTIONAL MATCH (a)-[:USAGE]->(u) RETURN a.name, u.name "
        "ORDER BY a.name LIMIT 30",
        "MATCH (a)-[:USAGE]->(v:Variable) RETURN a.name ORDER BY a.name",
        NULL};
    static char want[1 << 16];
    static char got[1 << 16];
    for (int i = 0; queries[i]; i++) {
        cypher_signature(local, queries[i], want, sizeof(want));
        cypher_signature(remote, queries[i], got, sizeof(got));
        if (strcmp(want, got) != 0) {
            printf("  query: %s\n   local:  %.300s\n   neo4j:  %.300s\n", queries[i], want, got);
        }
        ASSERT_STR_EQ(got, want);
    }

    /* Hubs and paging, with tiny limits: the hub (1,200 callers) is left out
     * of every prefetch and read in pages of 70; 512 callers (~517 edges)
     * overflow a 300-edge prefetch and are packed into smaller requests.
     * Answers stay those of the local index. */
    cbm_neo4j_test_set_edge_limits(300, 50, 70);
    for (int i = 0; queries[i]; i++) {
        cypher_signature(local, queries[i], want, sizeof(want));
        cypher_signature(remote, queries[i], got, sizeof(got));
        if (strcmp(want, got) != 0) {
            printf("  [paged] query: %s\n   local:  %.300s\n   neo4j:  %.300s\n", queries[i], want,
                   got);
        }
        ASSERT_STR_EQ(got, want);
    }
    /* A paged read stops between pages at the read deadline. */
    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    cbm_store_set_read_deadline(remote, 1); /* long past */
    ASSERT_EQ(cbm_store_find_edges_by_target(remote, hub_id, &edges, &edge_count),
              CBM_STORE_SCAN_LIMIT);
    ASSERT_NULL(edges);
    cbm_store_set_read_deadline(remote, 0);
    ASSERT_EQ(cbm_store_find_edges_by_target(remote, hub_id, &edges, &edge_count), CBM_STORE_OK);
    ASSERT_EQ(edge_count, CALLERS);
    for (int i = 1; i < edge_count; i++) {
        ASSERT_TRUE(edges[i - 1].id < edges[i].id); /* r.id order across pages */
    }
    cbm_store_free_edges(edges, edge_count);
    cbm_neo4j_test_set_edge_limits(0, 0, 0);

    /* The hub's callers: a few round trips, not one per caller. */
    cbm_store_backend_usage_reset(remote);
    cypher_signature(remote, "MATCH (a)-[:CALLS]->(b {name: 'h'}) RETURN count(a)", got,
                     sizeof(got));
    cbm_store_backend_usage_t usage;
    cbm_store_backend_usage(remote, &usage);
    printf("  [lv2] hub callers counted in %llu Neo4j queries\n",
           (unsigned long long)usage.remote_queries);
    ASSERT_TRUE(usage.remote_queries < 60);

    /* Batched reads agree with the local index. */
    cbm_node_t *a = NULL;
    cbm_node_t *b = NULL;
    int na = 0;
    int nb = 0;
    int64_t pick[4] = {ids[5], hub_id, -12345, ids[CALLERS - 1]};
    ASSERT_EQ(cbm_store_find_nodes_by_ids(local, pick, 4, &a, &na), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_find_nodes_by_ids(remote, pick, 4, &b, &nb), CBM_STORE_OK);
    ASSERT_EQ(na, 3);
    ASSERT_EQ(nb, na);
    for (int i = 0; i < na; i++) {
        ASSERT_EQ(b[i].id, a[i].id);
        ASSERT_STR_EQ(b[i].qualified_name, a[i].qualified_name);
    }
    cbm_store_free_nodes(a, na);
    cbm_store_free_nodes(b, nb);
    const char *files[3] = {"f007.c", "nope.c", "h.c"};
    ASSERT_EQ(cbm_store_find_nodes_by_files(local, "lv2", files, 3, &a, &na), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_find_nodes_by_files(remote, "lv2", files, 3, &b, &nb), CBM_STORE_OK);
    ASSERT_EQ(na, 12);
    ASSERT_EQ(nb, na);
    for (int i = 0; i < na; i++) {
        ASSERT_EQ(b[i].id, a[i].id);
        ASSERT_STR_EQ(b[i].file_path, a[i].file_path);
    }
    cbm_store_free_nodes(a, na);
    cbm_store_free_nodes(b, nb);

    /* The scoped native form of an engine query answers the same count. */
    char *scoped = cbm_neo4j_scope_query("MATCH (a)-[:CALLS]->(b {name: 'h'}) RETURN count(a)",
                                         err, sizeof(err));
    ASSERT_NOT_NULL(scoped);
    cbm_cypher_result_t r;
    ASSERT_EQ(cbm_neo4j_native_query(db, "lv2", scoped, 0, &r), 0);
    free(scoped);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "1200");
    cbm_cypher_result_free(&r);

    cbm_store_test_set_remote_scan_page(0);
    cbm_store_close(local);
    cbm_store_close(remote);
    cbm_neo4j_t *conn = cbm_neo4j_connect(&cfg, err, sizeof(err));
    ASSERT_NOT_NULL(conn);
    ASSERT_EQ(cbm_neo4j_drop_project(conn, "lv2", err, sizeof(err)), 0);
    cbm_neo4j_close(conn);
    cbm_neo4j_config_wipe(&cfg);
    th_rmtree(dir);
    PASS();
}

/* Native and store connections run in Neo4j's READ access mode with a
 * server-side time limit: the server itself refuses a write the keyword
 * guard did not see, and stops a runaway query. */
TEST(neo4j_live_read_only_connection_refuses_writes) {
    const char *uri = getenv("CBM_TEST_NEO4J_URI");
    const char *pw = getenv("CBM_TEST_NEO4J_PASSWORD");
    ASSERT_TRUE(uri && uri[0]);
    clear_neo4j_env();
    cbm_neo4j_config_t cfg;
    char err[512];
    ASSERT_EQ(cbm_neo4j_config_resolve(uri, NULL, pw, NULL, &cfg, err, sizeof(err)), 0);
    cbm_neo4j_t *c = cbm_neo4j_connect(&cfg, err, sizeof(err));
    ASSERT_NOT_NULL(c);
    ASSERT_EQ(cbm_neo4j_set_read_only(c, 1500), 0);
    cbm_neo4j_batch_t b;
    cbm_neo4j_result_t r;
    ASSERT_EQ(cbm_neo4j_batch_init(&b), 0);
    (void)cbm_neo4j_batch_add(&b, "CREATE (:CbmReadOnlyProbe) RETURN 1");
    ASSERT_EQ(cbm_neo4j_batch_run(c, &b, &r), -1);
    cbm_neo4j_batch_free(&b);
    ASSERT_NOT_NULL(strstr(cbm_neo4j_error(c), "AccessMode"));
    uint64_t t0 = cbm_now_ms();
    ASSERT_EQ(cbm_neo4j_batch_init(&b), 0);
    (void)cbm_neo4j_batch_add(&b, "UNWIND range(1, 2000000000) AS x RETURN count(x)");
    ASSERT_EQ(cbm_neo4j_batch_run(c, &b, &r), -1);
    cbm_neo4j_batch_free(&b);
    uint64_t took = cbm_now_ms() - t0;
    printf("  [ro] runaway query stopped by the server after %llu ms: %.80s\n",
           (unsigned long long)took, cbm_neo4j_error(c));
    ASSERT_LT(took, 30000);
    cbm_neo4j_close(c);

    /* the write really did not happen */
    c = cbm_neo4j_connect(&cfg, err, sizeof(err));
    ASSERT_NOT_NULL(c);
    ASSERT_EQ(cbm_neo4j_batch_init(&b), 0);
    (void)cbm_neo4j_batch_add(&b, "MATCH (p:CbmReadOnlyProbe) RETURN count(p)");
    ASSERT_EQ(cbm_neo4j_batch_run(c, &b, &r), 0);
    cbm_neo4j_batch_free(&b);
    ASSERT_EQ(cbm_neo4j_val_int(cbm_neo4j_row_col(yyjson_arr_get_first(cbm_neo4j_result_data(&r, 0)), 0), -1), 0);
    cbm_neo4j_result_free(&r);
    cbm_neo4j_close(c);
    cbm_neo4j_config_wipe(&cfg);
    PASS();
}

/* A server that accepts connections but never answers (hung, or a network
 * black hole in front of it) fails a request after one silent stall tick
 * plus the liveness check, not after the 120 s I/O timeout -- and the error
 * says the server stopped responding. */
TEST(neo4j_http_dead_server_fails_fast) {
#ifdef _WIN32
    SKIP_PLATFORM("uses a POSIX loopback listener");
#else
    int ls = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_TRUE(ls >= 0);
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = 0;
    ASSERT_EQ(bind(ls, (struct sockaddr *)&a, sizeof(a)), 0);
    ASSERT_EQ(listen(ls, 16), 0); /* the kernel completes connects; nothing ever answers */
    socklen_t al = sizeof(a);
    ASSERT_EQ(getsockname(ls, (struct sockaddr *)&a, &al), 0);
    char url[64];
    (void)snprintf(url, sizeof(url), "http://127.0.0.1:%d", (int)ntohs(a.sin_port));
    cbm_neo4j_url_t u;
    char err[512] = "";
    ASSERT_EQ(cbm_neo4j_url_parse(url, &u, err, sizeof(err)), 0);
    cbm_neo4j_http_t *h = cbm_neo4j_http_open(&u, "neo4j", "x", err, sizeof(err));
    ASSERT_NOT_NULL(h);
    int status = 0;
    char *resp = NULL;
    uint64_t t0 = cbm_now_ms();
    int rc = cbm_neo4j_http_post(h, "/db/neo4j/tx/commit", "{}", 2, &status, &resp, NULL, err,
                                 sizeof(err));
    uint64_t took = cbm_now_ms() - t0;
    printf("  [stall] dead server reported after %llu ms: %.120s\n", (unsigned long long)took,
           err);
    free(resp);
    cbm_neo4j_http_close(h);
    close(ls);
    ASSERT_EQ(rc, -1);
    ASSERT_NOT_NULL(strstr(err, "stopped responding"));
    ASSERT_TRUE(took >= 9000); /* one silent tick first: a slow reply is not a dead server */
    ASSERT_LT(took, 25000);    /* not the 120 s I/O timeout, and no second try */
    PASS();
#endif
}

/* ── Delta sync ─────────────────────────────────────────────────── */

TEST(neo4j_sync_stats_summaries) {
    cbm_neo4j_sync_stats_t st;
    memset(&st, 0, sizeof(st));
    char out[512];
    st.mode = CBM_NEO4J_SYNC_DELTA;
    st.nodes_added = 1;
    st.nodes_changed = 4;
    st.nodes_removed = 1;
    st.edges_added = 2;
    st.edges_changed = 2;
    st.edges_removed = 2;
    st.nodes_total = 10;
    st.edges_total = 20;
    ASSERT_EQ(cbm_neo4j_sync_stats_format(&st, out, sizeof(out)), 0);
    ASSERT_STR_EQ(out, "delta: nodes +1 ~4 -1, edges +2 ~2 -2 (graph: 10 nodes, 20 edges)");
    memset(&st, 0, sizeof(st));
    st.mode = CBM_NEO4J_SYNC_FULL;
    st.nodes_total = 3;
    st.edges_total = 5;
    snprintf(st.detail, sizeof(st.detail), "first sync");
    ASSERT_EQ(cbm_neo4j_sync_stats_format(&st, out, sizeof(out)), 0);
    ASSERT_STR_EQ(out, "full: 3 nodes, 5 edges (first sync)");
    st.mode = CBM_NEO4J_SYNC_SKIPPED;
    ASSERT_EQ(cbm_neo4j_sync_stats_format(&st, out, sizeof(out)), 0);
    ASSERT_STR_EQ(out, "unchanged: Neo4j already holds this generation");

    /* Removing the marker also drops the delta record. */
    char *dir = th_mktempdir("cbm_n4_state");
    ASSERT_NOT_NULL(dir);
    char db[512];
    char state[600];
    snprintf(db, sizeof(db), "%s/p.db", dir);
    ASSERT_EQ(cbm_neo4j_state_path(db, state, sizeof(state)), 0);
    ASSERT_NOT_NULL(strstr(state, "p.db.neo4j-state.db"));
    ASSERT_EQ(th_write_file(state, "x"), 0);
    ASSERT_EQ(cbm_neo4j_sidecar_remove(db), 0);
    FILE *f = fopen(state, "rb");
    ASSERT_NULL(f);
    th_rmtree(dir);
    PASS();
}

static int64_t dnode(cbm_store_t *s, const char *project, const char *label, const char *name,
                     const char *props) {
    char qn[128];
    snprintf(qn, sizeof(qn), "%s.%s", project, name);
    cbm_node_t n = {.project = project,
                    .label = label,
                    .name = name,
                    .qualified_name = qn,
                    .file_path = "d.c",
                    .start_line = 1,
                    .end_line = 9,
                    .properties_json = props};
    return cbm_store_upsert_node(s, &n);
}

/* Helpers return 0 on success, 1 on failure (like a TEST body). */
static int dedge(cbm_store_t *s, const char *project, int64_t a, int64_t b, const char *type,
                 const char *props) {
    cbm_edge_t e = {
        .project = project, .source_id = a, .target_id = b, .type = type, .properties_json = props};
    ASSERT_TRUE(cbm_store_insert_edge(s, &e) > 0);
    return 0;
}

static int dsql(cbm_store_t *s, const char *sql) {
    char *msg = NULL;
    int rc = sqlite3_exec(cbm_store_get_db(s), sql, NULL, NULL, &msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "dsql(%s): %s\n", sql, msg ? msg : "?");
    }
    sqlite3_free(msg);
    ASSERT_EQ(rc, SQLITE_OK);
    return 0;
}

/* One Neo4j statement's rows serialized as JSON (malloc'd), or NULL. */
static char *dquery(cbm_neo4j_t *c, const char *cypher, const char *db) {
    cbm_neo4j_batch_t b;
    if (cbm_neo4j_batch_init(&b) != 0) {
        return NULL;
    }
    yyjson_mut_val *p = cbm_neo4j_batch_add(&b, cypher);
    yyjson_mut_obj_add_strcpy(b.doc, p, "db", db);
    cbm_neo4j_result_t r;
    int rc = cbm_neo4j_batch_run(c, &b, &r);
    cbm_neo4j_batch_free(&b);
    if (rc != 0) {
        fprintf(stderr, "dquery: %s\n", cbm_neo4j_error(c));
        return NULL;
    }
    size_t len = 0;
    char *out = yyjson_val_write(cbm_neo4j_result_data(&r, 0), 0, &len);
    cbm_neo4j_result_free(&r);
    return out;
}

/* First column of the first row as an integer (-1 when absent). */
static int64_t dint(cbm_neo4j_t *c, const char *cypher, const char *db) {
    cbm_neo4j_batch_t b;
    if (cbm_neo4j_batch_init(&b) != 0) {
        return -1;
    }
    yyjson_mut_val *p = cbm_neo4j_batch_add(&b, cypher);
    yyjson_mut_obj_add_strcpy(b.doc, p, "db", db);
    cbm_neo4j_result_t r;
    int rc = cbm_neo4j_batch_run(c, &b, &r);
    cbm_neo4j_batch_free(&b);
    if (rc != 0) {
        return -1;
    }
    int64_t v = cbm_neo4j_val_int(
        cbm_neo4j_row_col(yyjson_arr_get_first(cbm_neo4j_result_data(&r, 0)), 0), -1);
    cbm_neo4j_result_free(&r);
    return v;
}

/* The published graph of db, canonical: every node (sorted labels, sorted
 * properties) and edge (type, endpoints, sorted properties) by id. `gen` is
 * left out: a full upload moves the graph to a new number, a delta does not. */
static char *ddump(cbm_neo4j_t *c, const char *db) {
    char *nodes = dquery(c,
                         "MATCH (p:CbmProject {name: $db}) "
                         "MATCH (n:CbmNode {db: $db, gen: p.gen}) "
                         "CALL { WITH n UNWIND labels(n) AS l WITH l ORDER BY l "
                         "RETURN collect(l) AS ls } "
                         "CALL { WITH n UNWIND [k IN keys(n) WHERE k <> 'gen'] AS k "
                         "WITH n, k ORDER BY k RETURN collect([k, n[k]]) AS kv } "
                         "RETURN n.id, ls, kv ORDER BY n.id",
                         db);
    char *edges = dquery(c,
                         "MATCH (p:CbmProject {name: $db}) "
                         "MATCH (a:CbmNode {db: $db, gen: p.gen})-[e]->"
                         "(b:CbmNode {db: $db, gen: p.gen}) "
                         "CALL { WITH e UNWIND keys(e) AS k WITH e, k ORDER BY k "
                         "RETURN collect([k, e[k]]) AS kv } "
                         "RETURN e.id, type(e), a.id, b.id, kv ORDER BY e.id",
                         db);
    char *proj = dquery(c,
                        "MATCH (p:CbmProject {name: $db}) "
                        "RETURN p.generation, p.node_count, p.edge_count, p.sync_format",
                        db);
    char *out = NULL;
    if (nodes && edges && proj) {
        size_t n = strlen(nodes) + strlen(edges) + strlen(proj) + 16;
        out = malloc(n);
        if (out) {
            snprintf(out, n, "%s\n%s\n%s", nodes, edges, proj);
        }
    }
    free(nodes);
    free(edges);
    free(proj);
    return out;
}

/* elementId of the node with SQLite id `id` in db's published graph. */
static char *delement(cbm_neo4j_t *c, const char *db, int64_t id) {
    char cypher[256];
    snprintf(cypher, sizeof(cypher),
             "MATCH (p:CbmProject {name: $db}) MATCH (n:CbmNode {db: $db, gen: p.gen, id: %lld}) "
             "RETURN elementId(n)",
             (long long)id);
    return dquery(c, cypher, db);
}

static int64_t dcount_local(const char *db, const char *project) {
    cbm_store_t *s = cbm_store_open_path_query(db);
    int64_t n = s ? cbm_store_count_nodes(s, project) : -1;
    cbm_store_close(s);
    return n;
}

/* Node count served from Neo4j through a normal attach (-1 when refused). */
static int64_t dcount_remote(const char *db, const char *project) {
    char err[512];
    cbm_store_t *s = cbm_store_open_path_query(db);
    if (!s) {
        return -1;
    }
    int64_t n = -1;
    if (cbm_neo4j_store_attach(s, db, project, err, sizeof(err)) == CBM_NEO4J_ATTACHED) {
        n = cbm_store_count_nodes(s, project);
    } else {
        fprintf(stderr, "attach %s: %s\n", project, err);
    }
    cbm_store_close(s);
    return n;
}

/* A re-index sends exactly the changed rows, in place: the graph it leaves is
 * identical to a fresh full upload of the same SQLite generation, and nodes
 * it did not touch keep their Neo4j identity (they were not re-written). */
TEST(neo4j_live_delta_matches_full) {
    const char *uri = getenv("CBM_TEST_NEO4J_URI");
    const char *pw = getenv("CBM_TEST_NEO4J_PASSWORD");
    ASSERT_TRUE(uri && uri[0]);
    clear_neo4j_env();
    cbm_unsetenv("CBM_NEO4J_DELTA_MAX_ROWS");
    const char *P = "dlt";
    char *dir = th_mktempdir("cbm_n4_delta");
    ASSERT_NOT_NULL(dir);
    char db[512];
    snprintf(db, sizeof(db), "%s/dlt.db", dir);
    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, P, dir);
    int64_t f1 = dnode(s, P, "Function", "f1", "{\"complexity\":1,\"tags\":[\"x\"]}");
    int64_t f2 = dnode(s, P, "Function", "f2", "{\"complexity\":2}");
    int64_t f3 = dnode(s, P, "Function", "f3", "{}");
    int64_t c1 = dnode(s, P, "Class", "c1", "{\"importance\":0.5}");
    int64_t w1 = dnode(s, P, "Weird Label", "w1", "{}");
    int64_t m1 = dnode(s, P, "Method", "m1", "{\"nested\":{\"a\":1}}");
    ASSERT_EQ(dedge(s, P, f1, f2, "CALLS", "{}"), 0);
    ASSERT_EQ(dedge(s, P, f2, f3, "CALLS", "{\"line\":3}"), 0);
    ASSERT_EQ(dedge(s, P, f3, c1, "USAGE", "{}"), 0);
    ASSERT_EQ(dedge(s, P, c1, w1, "HTTP-CALLS", "{\"url_path\":\"/a\"}"), 0);
    ASSERT_EQ(dedge(s, P, w1, m1, "CALLS", "{}"), 0);
    ASSERT_EQ(dedge(s, P, m1, f1, "USAGE", "{}"), 0);
    ASSERT_EQ(cbm_store_generation_advance(s), CBM_STORE_OK);

    cbm_neo4j_config_t cfg;
    char err[512];
    ASSERT_EQ(cbm_neo4j_config_resolve(uri, NULL, pw, NULL, &cfg, err, sizeof(err)), 0);
    cbm_neo4j_t *conn = cbm_neo4j_connect(&cfg, err, sizeof(err));
    ASSERT_NOT_NULL(conn);
    (void)cbm_neo4j_drop_project(conn, P, err, sizeof(err));

    /* First sync: nothing recorded yet, so everything is uploaded. */
    ASSERT_EQ(cbm_neo4j_after_publish(db, P, &cfg, false, err, sizeof(err)), 0);
    cbm_neo4j_sync_stats_t st;
    cbm_neo4j_last_sync_stats(&st);
    ASSERT_EQ(st.mode, CBM_NEO4J_SYNC_FULL);
    ASSERT_NOT_NULL(strstr(st.detail, "first sync"));
    ASSERT_EQ(st.nodes_total, 6);
    ASSERT_EQ(st.edges_total, 6);
    char *eid_f1 = delement(conn, P, f1);
    char *eid_f2 = delement(conn, P, f2);
    ASSERT_NOT_NULL(eid_f1);
    ASSERT_NOT_NULL(eid_f2);

    /* Every kind of change a re-index makes. */
    int64_t n1 = dnode(s, P, "Function", "n1", "{}"); /* node added */
    ASSERT_EQ(dedge(s, P, n1, f1, "CALLS", "{}"), 0); /* edge added */
    /* node removed, and its 2 edges with it */
    ASSERT_EQ(dsql(s, "DELETE FROM nodes WHERE name = 'f3'"), 0);
    ASSERT_EQ(dsql(s, "UPDATE nodes SET properties = '{\"complexity\":7}' WHERE name = 'f2'"), 0);
    ASSERT_EQ(dsql(s, "UPDATE nodes SET label = 'Interface' WHERE name = 'c1'"), 0);
    ASSERT_EQ(dsql(s, "UPDATE nodes SET label = 'Function' WHERE name = 'w1'"), 0);
    ASSERT_EQ(dsql(s, "UPDATE nodes SET label = 'Odd Label' WHERE name = 'm1'"), 0);
    char sql[256];
    snprintf(sql, sizeof(sql),
             "UPDATE edges SET properties = '{\"weight\":2}' WHERE source_id = %lld",
             (long long)w1);
    ASSERT_EQ(dsql(s, sql), 0); /* edge properties changed */
    snprintf(sql, sizeof(sql), "UPDATE edges SET target_id = %lld WHERE source_id = %lld",
             (long long)f2, (long long)m1);
    ASSERT_EQ(dsql(s, sql), 0);                            /* edge moved to another node */
    ASSERT_EQ(dedge(s, P, f1, w1, "HTTP-CALLS", "{}"), 0); /* odd type added */
    ASSERT_EQ(cbm_store_generation_advance(s), CBM_STORE_OK);

    ASSERT_EQ(cbm_neo4j_after_publish(db, P, NULL, false, err, sizeof(err)), 0);
    cbm_neo4j_last_sync_stats(&st);
    ASSERT_EQ(st.mode, CBM_NEO4J_SYNC_DELTA);
    ASSERT_EQ(st.nodes_added, 1);
    ASSERT_EQ(st.nodes_changed, 4);
    ASSERT_EQ(st.nodes_removed, 1);
    ASSERT_EQ(st.edges_added, 2);
    ASSERT_EQ(st.edges_changed, 2);
    ASSERT_EQ(st.edges_removed, 2);
    ASSERT_EQ(st.nodes_total, 6);
    ASSERT_EQ(st.edges_total, 6);
    char last[512];
    ASSERT_EQ(cbm_neo4j_sidecar_field(db, "last_sync", last, sizeof(last)), 0);
    ASSERT_NOT_NULL(strstr(last, "delta: nodes +1 ~4 -1, edges +2 ~2 -2"));

    /* Untouched and changed-in-place nodes kept their Neo4j identity. */
    char *eid_f1_after = delement(conn, P, f1);
    char *eid_f2_after = delement(conn, P, f2);
    ASSERT_NOT_NULL(eid_f1_after);
    ASSERT_NOT_NULL(eid_f2_after);
    ASSERT_STR_EQ(eid_f1_after, eid_f1);
    ASSERT_STR_EQ(eid_f2_after, eid_f2);
    free(eid_f1_after);
    free(eid_f2_after);
    ASSERT_EQ(dcount_remote(db, P), dcount_local(db, P));

    /* The same generation uploaded in full gives the identical graph. */
    char *after_delta = ddump(conn, P);
    ASSERT_NOT_NULL(after_delta);
    cbm_setenv("CBM_NEO4J_DELTA_MAX_ROWS", "0", 1);
    char done[128];
    ASSERT_EQ(cbm_neo4j_sync_db(conn, db, P, done, sizeof(done), err, sizeof(err)), 0);
    cbm_neo4j_last_sync_stats(&st);
    ASSERT_EQ(st.mode, CBM_NEO4J_SYNC_FULL);
    ASSERT_NOT_NULL(strstr(st.detail, "disabled"));
    cbm_unsetenv("CBM_NEO4J_DELTA_MAX_ROWS");
    char *after_full = ddump(conn, P);
    ASSERT_NOT_NULL(after_full);
    ASSERT_STR_EQ(after_delta, after_full);
    free(after_delta);
    free(after_full);

    /* The full upload rewrote the record: the next re-index is a delta again,
     * and an unchanged one sends nothing. */
    ASSERT_EQ(dsql(s, "UPDATE nodes SET properties = '{\"complexity\":8}' WHERE name = 'f2'"), 0);
    ASSERT_EQ(cbm_store_generation_advance(s), CBM_STORE_OK);
    ASSERT_EQ(cbm_neo4j_after_publish(db, P, NULL, false, err, sizeof(err)), 0);
    cbm_neo4j_last_sync_stats(&st);
    ASSERT_EQ(st.mode, CBM_NEO4J_SYNC_DELTA);
    ASSERT_EQ(st.nodes_changed, 1);
    ASSERT_EQ(st.nodes_added + st.nodes_removed + st.edges_added + st.edges_changed +
                  st.edges_removed,
              0);
    ASSERT_EQ(cbm_neo4j_after_publish(db, P, NULL, false, err, sizeof(err)), 0);
    cbm_neo4j_last_sync_stats(&st);
    ASSERT_EQ(st.mode, CBM_NEO4J_SYNC_SKIPPED);
    ASSERT_EQ(dcount_remote(db, P), dcount_local(db, P));

    cbm_store_close(s);
    free(eid_f1);
    free(eid_f2);
    ASSERT_EQ(cbm_neo4j_drop_project(conn, P, err, sizeof(err)), 0);
    cbm_neo4j_close(conn);
    cbm_neo4j_config_wipe(&cfg);
    th_rmtree(dir);
    PASS();
}

static int dbump(cbm_store_t *s, const char *what) {
    char sql[256];
    snprintf(sql, sizeof(sql), "UPDATE nodes SET properties = '{\"v\":\"%s\"}' WHERE name = 'a'",
             what);
    ASSERT_EQ(dsql(s, sql), 0);
    ASSERT_EQ(cbm_store_generation_advance(s), CBM_STORE_OK);
    return 0;
}

static int dexpect_full(const char *db, const char *P, const char *why) {
    char err[512];
    ASSERT_EQ(cbm_neo4j_after_publish(db, P, NULL, false, err, sizeof(err)), 0);
    cbm_neo4j_sync_stats_t st;
    cbm_neo4j_last_sync_stats(&st);
    if (st.mode != CBM_NEO4J_SYNC_FULL || !strstr(st.detail, why)) {
        fprintf(stderr, "expected a full sync because '%s', got mode %d (%s)\n", why, st.mode,
                st.detail);
    }
    ASSERT_EQ(st.mode, CBM_NEO4J_SYNC_FULL);
    ASSERT_NOT_NULL(strstr(st.detail, why));
    ASSERT_EQ(dcount_remote(db, P), dcount_local(db, P));
    return 0;
}

static int dexpect_delta(const char *db, const char *P) {
    char err[512];
    ASSERT_EQ(cbm_neo4j_after_publish(db, P, NULL, false, err, sizeof(err)), 0);
    cbm_neo4j_sync_stats_t st;
    cbm_neo4j_last_sync_stats(&st);
    if (st.mode != CBM_NEO4J_SYNC_DELTA) {
        fprintf(stderr, "expected a delta sync, got mode %d (%s)\n", st.mode, st.detail);
    }
    ASSERT_EQ(st.mode, CBM_NEO4J_SYNC_DELTA);
    ASSERT_EQ(dcount_remote(db, P), dcount_local(db, P));
    return 0;
}

static int dbuild(const char *db, const char *P, const char *dir) {
    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, P, dir);
    int64_t a = dnode(s, P, "Function", "a", "{}");
    int64_t b = dnode(s, P, "Function", "b", "{}");
    int64_t c = dnode(s, P, "Class", "c", "{}");
    ASSERT_EQ(dedge(s, P, a, b, "CALLS", "{}"), 0);
    ASSERT_EQ(dedge(s, P, b, c, "USAGE", "{}"), 0);
    ASSERT_EQ(cbm_store_generation_advance(s), CBM_STORE_OK);
    cbm_store_close(s);
    return 0;
}

/* The same graph as dbuild plus node d, written in another order, so every id
 * differs from dbuild's. */
static int dbuild_renumbered(const char *db, const char *P, const char *dir) {
    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, P, dir);
    int64_t d = dnode(s, P, "Method", "d", "{}");
    int64_t c = dnode(s, P, "Class", "c", "{}");
    int64_t b = dnode(s, P, "Function", "b", "{}");
    int64_t a = dnode(s, P, "Function", "a", "{\"v\":\"r\"}");
    ASSERT_EQ(dedge(s, P, b, c, "USAGE", "{}"), 0);
    ASSERT_EQ(dedge(s, P, d, a, "CALLS", "{}"), 0);
    ASSERT_EQ(dedge(s, P, a, b, "CALLS", "{}"), 0);
    ASSERT_EQ(cbm_store_generation_advance(s), CBM_STORE_OK);
    cbm_store_close(s);
    return 0;
}

/* Delete a project database file and its WAL sidecars (the delta record and
 * marker stay, as when the indexer rewrites the database). */
static int dremove_db(const char *db) {
    char side[600];
    snprintf(side, sizeof(side), "%s-wal", db);
    (void)th_unlink_force(side);
    snprintf(side, sizeof(side), "%s-shm", db);
    (void)th_unlink_force(side);
    ASSERT_EQ(th_unlink_force(db), 0);
    return 0;
}

typedef struct {
    cbm_neo4j_t *conn;
    const char *project;
    int calls;
} dhook_ctx_t;

/* A concurrent writer: moves the published counts under the delta's feet. */
static void dhook_move_graph(void *ctx) {
    dhook_ctx_t *h = ctx;
    h->calls++;
    char *r = dquery(h->conn, "MATCH (p:CbmProject {name: $db}) SET p.edge_count = -5 RETURN 1",
                     h->project);
    free(r);
}

/* Every reason the record cannot be trusted ends in a correct full upload,
 * never in a delta applied to the wrong graph. */
TEST(neo4j_live_delta_falls_back_safely) {
    const char *uri = getenv("CBM_TEST_NEO4J_URI");
    const char *pw = getenv("CBM_TEST_NEO4J_PASSWORD");
    ASSERT_TRUE(uri && uri[0]);
    clear_neo4j_env();
    cbm_unsetenv("CBM_NEO4J_DELTA_MAX_ROWS");
    const char *P = "dfb";
    char *dir = th_mktempdir("cbm_n4_dfb");
    ASSERT_NOT_NULL(dir);
    char db[512];
    char state[600];
    snprintf(db, sizeof(db), "%s/dfb.db", dir);
    ASSERT_EQ(cbm_neo4j_state_path(db, state, sizeof(state)), 0);
    ASSERT_EQ(dbuild(db, P, dir), 0);
    cbm_neo4j_config_t cfg;
    char err[512];
    ASSERT_EQ(cbm_neo4j_config_resolve(uri, NULL, pw, NULL, &cfg, err, sizeof(err)), 0);
    cbm_neo4j_t *conn = cbm_neo4j_connect(&cfg, err, sizeof(err));
    ASSERT_NOT_NULL(conn);
    (void)cbm_neo4j_drop_project(conn, P, err, sizeof(err));
    ASSERT_EQ(cbm_neo4j_after_publish(db, P, &cfg, false, err, sizeof(err)), 0);
    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);

    ASSERT_EQ(dbump(s, "1"), 0);
    ASSERT_EQ(dexpect_delta(db, P), 0);

    /* Someone changed the graph in Neo4j behind the record's back. */
    free(dquery(conn, "MATCH (p:CbmProject {name: $db}) SET p.node_count = 999 RETURN 1", P));
    ASSERT_EQ(dbump(s, "2"), 0);
    ASSERT_EQ(dexpect_full(db, P, "differs"), 0);
    ASSERT_EQ(dbump(s, "3"), 0);
    ASSERT_EQ(dexpect_delta(db, P), 0);

    /* The graph was deleted from Neo4j. */
    ASSERT_EQ(cbm_neo4j_drop_project(conn, P, err, sizeof(err)), 0);
    ASSERT_EQ(dbump(s, "4"), 0);
    ASSERT_EQ(dexpect_full(db, P, "differs"), 0);

    /* The change is larger than one transaction may carry. */
    cbm_setenv("CBM_NEO4J_DELTA_MAX_ROWS", "1", 1);
    ASSERT_EQ(dsql(s, "UPDATE nodes SET properties = '{\"v\":\"5\"}'"), 0);
    ASSERT_EQ(cbm_store_generation_advance(s), CBM_STORE_OK);
    ASSERT_EQ(dexpect_full(db, P, "too large"), 0);
    cbm_unsetenv("CBM_NEO4J_DELTA_MAX_ROWS");

    /* The record was deleted, or is not a record at all. */
    ASSERT_EQ(th_unlink_force(state), 0);
    ASSERT_EQ(dbump(s, "6"), 0);
    ASSERT_EQ(dexpect_full(db, P, "first sync"), 0);
    ASSERT_EQ(th_write_file(state, "not a database"), 0);
    ASSERT_EQ(dbump(s, "7"), 0);
    ASSERT_EQ(dexpect_full(db, P, "unreadable"), 0);
    ASSERT_EQ(dbump(s, "8"), 0);
    ASSERT_EQ(dexpect_delta(db, P), 0);

    /* A concurrent writer moves the graph between the delta's check and its
     * transaction: the in-transaction guard refuses it, nothing of it is
     * applied, and a full upload repairs the graph. */
    dhook_ctx_t h = {.conn = conn, .project = P, .calls = 0};
    cbm_neo4j_test_set_delta_hook(dhook_move_graph, &h);
    ASSERT_EQ(dbump(s, "9"), 0);
    ASSERT_EQ(dexpect_full(db, P, "rejected"), 0);
    cbm_neo4j_test_set_delta_hook(NULL, NULL);
    ASSERT_EQ(h.calls, 1);
    ASSERT_EQ(dint(conn, "MATCH (p:CbmProject {name: $db}) RETURN p.edge_count", P), 2);

    /* A re-index writes a brand-new database file (new db_uid) in which the
     * unchanged rows keep their ids: still a delta, sending only the rows that
     * differ (a, b and c lose the "v" property the steps above gave them; the
     * edges are unchanged). */
    cbm_store_close(s);
    ASSERT_EQ(dremove_db(db), 0);
    ASSERT_EQ(dbuild(db, P, dir), 0);
    ASSERT_EQ(dexpect_delta(db, P), 0);
    cbm_neo4j_sync_stats_t st;
    cbm_neo4j_last_sync_stats(&st);
    ASSERT_EQ(st.nodes_changed, 3);
    ASSERT_EQ(st.nodes_added + st.nodes_removed + st.edges_added + st.edges_changed +
                  st.edges_removed,
              0);
    s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(dbump(s, "10"), 0);
    ASSERT_EQ(dexpect_delta(db, P), 0);

    /* A rebuild that renumbers every id (rows written in another order, one
     * more node): the delta still ends exactly where a full upload does. */
    cbm_store_close(s);
    ASSERT_EQ(dremove_db(db), 0);
    ASSERT_EQ(dbuild_renumbered(db, P, dir), 0);
    ASSERT_EQ(dexpect_delta(db, P), 0);
    char *after_delta = ddump(conn, P);
    ASSERT_NOT_NULL(after_delta);
    cbm_setenv("CBM_NEO4J_DELTA_MAX_ROWS", "0", 1);
    char done[128];
    ASSERT_EQ(cbm_neo4j_sync_db(conn, db, P, done, sizeof(done), err, sizeof(err)), 0);
    cbm_unsetenv("CBM_NEO4J_DELTA_MAX_ROWS");
    char *after_full = ddump(conn, P);
    ASSERT_NOT_NULL(after_full);
    ASSERT_STR_EQ(after_delta, after_full);
    free(after_delta);
    free(after_full);
    s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);

    cbm_store_close(s);
    ASSERT_EQ(cbm_neo4j_drop_project(conn, P, err, sizeof(err)), 0);
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
    RUN_TEST(neo4j_large_results_are_read_in_linear_time);
    RUN_TEST(neo4j_row_conversion_edge_cases);
    RUN_TEST(neo4j_chunked_body_decodes_across_any_split);
    RUN_TEST(neo4j_request_with_invalid_utf8_is_still_sent);
    RUN_TEST(neo4j_sidecar_records_last_sync_error);
    RUN_TEST(neo4j_unavailable_graph_read_is_an_error);
    RUN_TEST(neo4j_inherited_sync_failure_fails_the_run);
    RUN_TEST(neo4j_routed_lookups_apply_local_predicates);
    RUN_TEST(neo4j_marker_found_for_renamed_database);
    RUN_TEST(neo4j_served_by_is_named_in_replies);
    RUN_TEST(neo4j_scope_query_scopes_every_node_pattern);
    RUN_TEST(neo4j_http_dead_server_fails_fast);
    RUN_TEST(neo4j_sync_stats_summaries);
    if (live_neo4j_configured()) {
        RUN_TEST(neo4j_live_sync_and_read);
        RUN_TEST(neo4j_live_delta_matches_full);
        RUN_TEST(neo4j_live_delta_falls_back_safely);
        RUN_TEST(neo4j_live_list_files_matches_local);
        RUN_TEST(neo4j_live_large_file_list_matches_local);
        RUN_TEST(neo4j_live_engine_reads_match_local);
        RUN_TEST(neo4j_live_read_only_connection_refuses_writes);
    } else {
        printf("  (neo4j live test not registered: set CBM_TEST_NEO4J_URI and "
               "CBM_TEST_NEO4J_PASSWORD to run it against a server)\n");
    }
}
