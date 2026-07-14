/*
 * test_graph_buffer.c — Tests for in-memory graph buffer.
 *
 * RED phase: Tests define expected behavior for node/edge insertion,
 * lookup, dedup, delete, and dump to SQLite.
 */
#include "test_framework.h"
#include "graph_buffer/graph_buffer.h"
#include "store/store.h"
#include "foundation/compat.h" /* cbm_mkstemp */
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/platform.h"
#include "sqlite3.h"           /* vendored/sqlite3/ via -Ivendored/sqlite3 */
#include <stdatomic.h>
#include <string.h>
#include <unistd.h>

static int gbuf_make_temp_db(char *path, size_t pathsz) {
    snprintf(path, pathsz, "/tmp/cbm_gbuf_dump_XXXXXX");
    int fd = cbm_mkstemp(path);
    if (fd < 0) {
        return -1;
    }
    close(fd);
    return 0;
}

static int gbuf_store_has_qn(const char *path, const char *project, const char *qn) {
    cbm_store_t *store = cbm_store_open_path_query(path);
    if (!store) {
        return 0;
    }
    cbm_node_t node = {0};
    int found = cbm_store_find_node_by_qn(store, project, qn, &node) == CBM_STORE_OK;
    cbm_node_free_fields(&node);
    cbm_store_close(store);
    return found;
}

/* ── Node operations ───────────────────────────────────────────── */

TEST(gbuf_create_free) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test-proj", "/tmp/repo");
    ASSERT_NOT_NULL(gb);
    ASSERT_EQ(cbm_gbuf_node_count(gb), 0);
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 0);
    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_free_null) {
    cbm_gbuf_free(NULL); /* should not crash */
    PASS();
}

TEST(gbuf_upsert_node) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t id = cbm_gbuf_upsert_node(gb, "Function", "main", "pkg.main", "main.go", 1, 10, "{}");
    ASSERT_GT(id, 0);
    ASSERT_EQ(cbm_gbuf_node_count(gb), 1);

    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(gb, "pkg.main");
    ASSERT_NOT_NULL(n);
    ASSERT_STR_EQ(n->label, "Function");
    ASSERT_STR_EQ(n->name, "main");
    ASSERT_STR_EQ(n->qualified_name, "pkg.main");
    ASSERT_STR_EQ(n->file_path, "main.go");
    ASSERT_EQ(n->start_line, 1);
    ASSERT_EQ(n->end_line, 10);
    ASSERT_EQ(n->id, id);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_upsert_updates) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t id1 = cbm_gbuf_upsert_node(gb, "Function", "main", "pkg.main", "main.go", 1, 10, "{}");
    /* Upsert same QN with different fields */
    int64_t id2 = cbm_gbuf_upsert_node(gb, "Method", "main", "pkg.main", "main.go", 5, 20,
                                       "{\"key\":\"val\"}");
    ASSERT_EQ(id1, id2);                   /* same temp ID */
    ASSERT_EQ(cbm_gbuf_node_count(gb), 1); /* still one node */

    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(gb, "pkg.main");
    ASSERT_NOT_NULL(n);
    ASSERT_STR_EQ(n->label, "Method"); /* updated */
    ASSERT_EQ(n->end_line, 20);        /* updated */

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_route_upsert_file_path_is_deterministic) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    const char *route_qn = "__route__GET__/items/{}";
    int64_t id1 = cbm_gbuf_upsert_node(gb, "Route", "/items/{item_id}", route_qn, "z/last.py", 0,
                                       0, "{}");
    int64_t id2 = cbm_gbuf_upsert_node(gb, "Route", "/items/{id}", route_qn, "a/first.py", 0, 0,
                                       "{\"method\":\"GET\",\"source\":\"decorator\"}");
    int64_t id3 = cbm_gbuf_upsert_node(gb, "Route", "", route_qn, "", 0, 0, "{}");
    ASSERT_EQ(id1, id2);
    ASSERT_EQ(id1, id3);

    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(gb, route_qn);
    ASSERT_NOT_NULL(n);
    ASSERT_STR_EQ(n->name, "/items/{id}");
    ASSERT_STR_EQ(n->file_path, "a/first.py");
    ASSERT_STR_EQ(n->properties_json, "{\"method\":\"GET\",\"source\":\"decorator\"}");

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_section_upsert_file_path_is_deterministic) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    const char *section_qn = "proj.docs.deployment.Implantacao";

    int64_t id1 = cbm_gbuf_upsert_node(gb, "Section", "Implantacao", section_qn,
                                       "docs/deployment/index.md", 1, 2, "{}");
    int64_t id2 = cbm_gbuf_upsert_node(gb, "Section", "Implantacao", section_qn,
                                       "docs/deployment.md", 1, 2, "{}");
    ASSERT_EQ(id1, id2);

    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(gb, section_qn);
    ASSERT_NOT_NULL(n);
    ASSERT_STR_EQ(n->file_path, "docs/deployment.md");

    cbm_gbuf_free(gb);
    PASS();
}

static int assert_full_definition_source(const cbm_gbuf_t *gb, const char *qn) {
    enum {
        FULL_DEF_START_LINE = 34,
        FULL_DEF_END_LINE = 43,
    };
    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(gb, qn);
    ASSERT_NOT_NULL(n);
    ASSERT_STR_EQ(n->file_path, "src/type.c");
    ASSERT_EQ(n->start_line, FULL_DEF_START_LINE);
    ASSERT_EQ(n->end_line, FULL_DEF_END_LINE);
    ASSERT_STR_EQ(n->properties_json, "{\"source\":\"definition\"}");
    return 0;
}

static int assert_install_sh_module_source(const cbm_gbuf_t *gb, const char *qn) {
    enum {
        INSTALL_SH_START_LINE = 1,
        INSTALL_SH_END_LINE = 221,
    };
    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(gb, qn);
    ASSERT_NOT_NULL(n);
    ASSERT_STR_EQ(n->label, "Module");
    ASSERT_STR_EQ(n->file_path, "install.sh");
    ASSERT_EQ(n->start_line, INSTALL_SH_START_LINE);
    ASSERT_EQ(n->end_line, INSTALL_SH_END_LINE);
    ASSERT_STR_EQ(n->properties_json, "{\"source\":\"shell\"}");
    return 0;
}

TEST(gbuf_upsert_definition_source_prefers_richer_span) {
    enum {
        DECL_START_LINE = 7,
        DECL_END_LINE = 7,
        FULL_DEF_START_LINE = 34,
        FULL_DEF_END_LINE = 43,
    };
    const char *qn = "proj.TypeName";

    cbm_gbuf_t *decl_then_def = cbm_gbuf_new("test", "/tmp");
    int64_t id1 =
        cbm_gbuf_upsert_node(decl_then_def, "Class", "TypeName", qn, "include/type.h",
                             DECL_START_LINE, DECL_END_LINE, "{\"source\":\"declaration\"}");
    int64_t id2 =
        cbm_gbuf_upsert_node(decl_then_def, "Class", "TypeName", qn, "src/type.c",
                             FULL_DEF_START_LINE, FULL_DEF_END_LINE,
                             "{\"source\":\"definition\"}");
    ASSERT_EQ(id1, id2);
    ASSERT_EQ(assert_full_definition_source(decl_then_def, qn), 0);
    cbm_gbuf_free(decl_then_def);

    cbm_gbuf_t *def_then_decl = cbm_gbuf_new("test", "/tmp");
    id1 = cbm_gbuf_upsert_node(def_then_decl, "Class", "TypeName", qn, "src/type.c",
                               FULL_DEF_START_LINE, FULL_DEF_END_LINE,
                               "{\"source\":\"definition\"}");
    id2 = cbm_gbuf_upsert_node(def_then_decl, "Class", "TypeName", qn, "include/type.h",
                               DECL_START_LINE, DECL_END_LINE, "{\"source\":\"declaration\"}");
    ASSERT_EQ(id1, id2);
    ASSERT_EQ(assert_full_definition_source(def_then_decl, qn), 0);
    cbm_gbuf_free(def_then_decl);

    PASS();
}

TEST(gbuf_upsert_module_source_prefers_richer_span) {
    enum {
        INSTALL_START_LINE = 1,
        INSTALL_PS_END_LINE = 155,
        INSTALL_SH_END_LINE = 221,
    };
    const char *qn = "proj.install";

    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t id1 =
        cbm_gbuf_upsert_node(gb, "Module", "install.ps1", qn, "install.ps1",
                             INSTALL_START_LINE, INSTALL_PS_END_LINE, "{\"source\":\"powershell\"}");
    int64_t id2 =
        cbm_gbuf_upsert_node(gb, "Module", "install.sh", qn, "install.sh", INSTALL_START_LINE,
                             INSTALL_SH_END_LINE, "{\"source\":\"shell\"}");
    ASSERT_EQ(id1, id2);
    ASSERT_EQ(assert_install_sh_module_source(gb, qn), 0);
    cbm_gbuf_free(gb);

    gb = cbm_gbuf_new("test", "/tmp");
    id1 = cbm_gbuf_upsert_node(gb, "Module", "install.sh", qn, "install.sh",
                               INSTALL_START_LINE, INSTALL_SH_END_LINE,
                               "{\"source\":\"shell\"}");
    id2 = cbm_gbuf_upsert_node(gb, "Module", "install.ps1", qn, "install.ps1",
                               INSTALL_START_LINE, INSTALL_PS_END_LINE,
                               "{\"source\":\"powershell\"}");
    ASSERT_EQ(id1, id2);
    ASSERT_EQ(assert_install_sh_module_source(gb, qn), 0);
    cbm_gbuf_free(gb);

    PASS();
}

TEST(gbuf_find_by_id) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t id = cbm_gbuf_upsert_node(gb, "Function", "foo", "pkg.foo", "foo.go", 1, 5, "{}");

    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_id(gb, id);
    ASSERT_NOT_NULL(n);
    ASSERT_STR_EQ(n->name, "foo");

    /* Not found */
    ASSERT_NULL(cbm_gbuf_find_by_id(gb, 999));

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_find_by_label) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_upsert_node(gb, "Function", "foo", "pkg.foo", "f.go", 1, 5, "{}");
    cbm_gbuf_upsert_node(gb, "Function", "bar", "pkg.bar", "f.go", 6, 10, "{}");
    cbm_gbuf_upsert_node(gb, "Class", "Baz", "pkg.Baz", "f.go", 11, 20, "{}");

    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    int rc = cbm_gbuf_find_by_label(gb, "Function", &nodes, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 2);

    rc = cbm_gbuf_find_by_label(gb, "Class", &nodes, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1);

    rc = cbm_gbuf_find_by_label(gb, "Module", &nodes, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 0);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_upsert_reindexes_label_and_name) {
    enum {
        COMPAT_DECL_START = 41,
        COMPAT_DECL_END = 42,
        COMPAT_DEF_START = 22,
        COMPAT_DEF_END = 36,
    };
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t id1 = cbm_gbuf_upsert_node(gb, "Macro", "OLD_NAME", "pkg.compat.cbm_strndup",
                                       "compat.h", COMPAT_DECL_START, COMPAT_DECL_END, "{}");
    int64_t id2 = cbm_gbuf_upsert_node(gb, "Function", "cbm_strndup",
                                       "pkg.compat.cbm_strndup", "compat.c", COMPAT_DEF_START,
                                       COMPAT_DEF_END,
                                       "{\"loop_depth\":1,\"self_recursive\":false}");
    ASSERT_EQ(id1, id2);

    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    ASSERT_EQ(cbm_gbuf_find_by_label(gb, "Macro", &nodes, &count), 0);
    ASSERT_EQ(count, 0);
    ASSERT_EQ(cbm_gbuf_find_by_label(gb, "Function", &nodes, &count), 0);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(nodes[0]->id, id1);

    ASSERT_EQ(cbm_gbuf_find_by_name(gb, "OLD_NAME", &nodes, &count), 0);
    ASSERT_EQ(count, 0);
    ASSERT_EQ(cbm_gbuf_find_by_name(gb, "cbm_strndup", &nodes, &count), 0);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(nodes[0]->id, id1);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_find_by_name) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_upsert_node(gb, "Function", "main", "a.main", "a.go", 1, 5, "{}");
    cbm_gbuf_upsert_node(gb, "Function", "main", "b.main", "b.go", 1, 5, "{}");

    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    int rc = cbm_gbuf_find_by_name(gb, "main", &nodes, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 2);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_delete_by_label) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t f1 = cbm_gbuf_upsert_node(gb, "Function", "foo", "pkg.foo", "f.go", 1, 5, "{}");
    int64_t f2 = cbm_gbuf_upsert_node(gb, "Function", "bar", "pkg.bar", "f.go", 6, 10, "{}");
    cbm_gbuf_upsert_node(gb, "Class", "Baz", "pkg.Baz", "f.go", 11, 20, "{}");

    /* Add edge between functions */
    cbm_gbuf_insert_edge(gb, f1, f2, "CALLS", "{}");
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 1);

    /* Delete all functions — should cascade-delete the CALLS edge */
    ASSERT_EQ(cbm_gbuf_delete_by_label(gb, "Function"), 0);
    ASSERT_EQ(cbm_gbuf_node_count(gb), 1); /* only Class remains */
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 0); /* edge cascade-deleted */

    ASSERT_NULL(cbm_gbuf_find_by_qn(gb, "pkg.foo"));
    ASSERT_NULL(cbm_gbuf_find_by_qn(gb, "pkg.bar"));
    ASSERT_NOT_NULL(cbm_gbuf_find_by_qn(gb, "pkg.Baz"));

    cbm_gbuf_free(gb);
    PASS();
}

/* ── Edge operations ───────────────────────────────────────────── */

TEST(gbuf_insert_edge) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t n1 = cbm_gbuf_upsert_node(gb, "Function", "a", "pkg.a", "f.go", 1, 5, "{}");
    int64_t n2 = cbm_gbuf_upsert_node(gb, "Function", "b", "pkg.b", "f.go", 6, 10, "{}");

    int64_t eid = cbm_gbuf_insert_edge(gb, n1, n2, "CALLS", "{}");
    ASSERT_GT(eid, 0);
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 1);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_edge_dedup) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t n1 = cbm_gbuf_upsert_node(gb, "Function", "a", "pkg.a", "f.go", 1, 5, "{}");
    int64_t n2 = cbm_gbuf_upsert_node(gb, "Function", "b", "pkg.b", "f.go", 6, 10, "{}");

    int64_t eid1 = cbm_gbuf_insert_edge(gb, n1, n2, "CALLS", "{}");
    int64_t eid2 = cbm_gbuf_insert_edge(gb, n1, n2, "CALLS", "{\"weight\":5}");
    ASSERT_EQ(eid1, eid2); /* same edge, deduped */
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 1);

    /* Different type = different edge */
    int64_t eid3 = cbm_gbuf_insert_edge(gb, n1, n2, "IMPORTS", "{}");
    ASSERT_NEQ(eid1, eid3);
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 2);

    cbm_gbuf_free(gb);
    PASS();
}

/* #768: two named imports from the same specifier (same source, same target
 * file) must produce two distinct IMPORTS edges, keyed apart by local_name --
 * not collapse into one edge that silently drops whichever import lost the
 * dedup race. Re-inserting the SAME local_name (e.g. an idempotent re-index)
 * must still dedup to one edge. */
TEST(gbuf_imports_multi_symbol_dedup) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t consumer =
        cbm_gbuf_upsert_node(gb, "File", "consumer.ts", "pkg.consumer", "consumer.ts", 1, 1, "{}");
    int64_t lib = cbm_gbuf_upsert_node(gb, "File", "lib.ts", "pkg.lib", "lib.ts", 1, 1, "{}");

    int64_t eid_a = cbm_gbuf_insert_edge(gb, consumer, lib, "IMPORTS", "{\"local_name\":\"A\"}");
    int64_t eid_b = cbm_gbuf_insert_edge(gb, consumer, lib, "IMPORTS", "{\"local_name\":\"B\"}");
    ASSERT_GT(eid_a, 0);
    ASSERT_GT(eid_b, 0);
    ASSERT_NEQ(eid_a, eid_b); /* distinct symbols -> distinct edges */
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 2);

    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_source_type(gb, consumer, "IMPORTS", &edges, &count);
    ASSERT_EQ(count, 2);
    ASSERT_TRUE(strstr(edges[0]->properties_json, "\"local_name\":\"A\"") != NULL ||
                strstr(edges[1]->properties_json, "\"local_name\":\"A\"") != NULL);
    ASSERT_TRUE(strstr(edges[0]->properties_json, "\"local_name\":\"B\"") != NULL ||
                strstr(edges[1]->properties_json, "\"local_name\":\"B\"") != NULL);

    /* Re-inserting the same symbol (idempotent re-index) still dedups. */
    int64_t eid_a_again =
        cbm_gbuf_insert_edge(gb, consumer, lib, "IMPORTS", "{\"local_name\":\"A\"}");
    ASSERT_EQ(eid_a_again, eid_a);
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 2);

    cbm_gbuf_free(gb);
    PASS();
}

/* #768 hardening: the dedup key lives in a fixed-size stack buffer. Two long
 * local_names sharing a prefix must NOT silently collide when the verbatim
 * key would be truncated — the key builder re-keys oversized local_names with
 * a hash of the FULL name. Determinism must hold: re-inserting the same long
 * name still dedups to the same edge. */
TEST(gbuf_imports_long_local_name_no_collision) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t consumer =
        cbm_gbuf_upsert_node(gb, "File", "consumer.ts", "pkg.consumer", "consumer.ts", 1, 1, "{}");
    int64_t lib = cbm_gbuf_upsert_node(gb, "File", "lib.ts", "pkg.lib", "lib.ts", 1, 1, "{}");

    /* 300-char shared prefix, distinct 4-char tails — a truncated verbatim
     * key keeps only the shared prefix and would merge the two edges. */
    enum { LONG_PREFIX = 300 };
    char name_a[LONG_PREFIX + 8];
    char name_b[LONG_PREFIX + 8];
    memset(name_a, 'x', LONG_PREFIX);
    memset(name_b, 'x', LONG_PREFIX);
    memcpy(name_a + LONG_PREFIX, "AAAA", 5);
    memcpy(name_b + LONG_PREFIX, "BBBB", 5);

    char props_a[512];
    char props_b[512];
    snprintf(props_a, sizeof(props_a), "{\"local_name\":\"%s\"}", name_a);
    snprintf(props_b, sizeof(props_b), "{\"local_name\":\"%s\"}", name_b);

    int64_t eid_a = cbm_gbuf_insert_edge(gb, consumer, lib, "IMPORTS", props_a);
    int64_t eid_b = cbm_gbuf_insert_edge(gb, consumer, lib, "IMPORTS", props_b);
    ASSERT_GT(eid_a, 0);
    ASSERT_GT(eid_b, 0);
    ASSERT_NEQ(eid_a, eid_b); /* prefix-sharing long names stay distinct */
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 2);

    /* Hash re-keying is deterministic: same long name dedups. */
    int64_t eid_a_again = cbm_gbuf_insert_edge(gb, consumer, lib, "IMPORTS", props_a);
    ASSERT_EQ(eid_a_again, eid_a);
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 2);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_find_edges_by_source_type) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t a = cbm_gbuf_upsert_node(gb, "Function", "a", "pkg.a", "f.go", 1, 5, "{}");
    int64_t b = cbm_gbuf_upsert_node(gb, "Function", "b", "pkg.b", "f.go", 6, 10, "{}");
    int64_t c = cbm_gbuf_upsert_node(gb, "Function", "c", "pkg.c", "f.go", 11, 15, "{}");

    cbm_gbuf_insert_edge(gb, a, b, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, a, c, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, a, b, "IMPORTS", "{}");

    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_source_type(gb, a, "CALLS", &edges, &count);
    ASSERT_EQ(count, 2);

    cbm_gbuf_find_edges_by_source_type(gb, a, "IMPORTS", &edges, &count);
    ASSERT_EQ(count, 1);

    cbm_gbuf_find_edges_by_source_type(gb, b, "CALLS", &edges, &count);
    ASSERT_EQ(count, 0);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_find_edges_by_target_type) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t a = cbm_gbuf_upsert_node(gb, "Function", "a", "pkg.a", "f.go", 1, 5, "{}");
    int64_t b = cbm_gbuf_upsert_node(gb, "Function", "b", "pkg.b", "f.go", 6, 10, "{}");

    cbm_gbuf_insert_edge(gb, a, b, "CALLS", "{}");

    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_target_type(gb, b, "CALLS", &edges, &count);
    ASSERT_EQ(count, 1);

    cbm_gbuf_find_edges_by_target_type(gb, a, "CALLS", &edges, &count);
    ASSERT_EQ(count, 0);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_find_edges_by_type) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t a = cbm_gbuf_upsert_node(gb, "Function", "a", "pkg.a", "f.go", 1, 5, "{}");
    int64_t b = cbm_gbuf_upsert_node(gb, "Function", "b", "pkg.b", "f.go", 6, 10, "{}");

    cbm_gbuf_insert_edge(gb, a, b, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, b, a, "CALLS", "{}");

    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "CALLS", &edges, &count);
    ASSERT_EQ(count, 2);

    cbm_gbuf_find_edges_by_type(gb, "IMPORTS", &edges, &count);
    ASSERT_EQ(count, 0);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_delete_edges_by_type) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t a = cbm_gbuf_upsert_node(gb, "Function", "a", "pkg.a", "f.go", 1, 5, "{}");
    int64_t b = cbm_gbuf_upsert_node(gb, "Function", "b", "pkg.b", "f.go", 6, 10, "{}");

    cbm_gbuf_insert_edge(gb, a, b, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, a, b, "IMPORTS", "{}");
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 2);

    cbm_gbuf_delete_edges_by_type(gb, "CALLS");
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 1);
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(gb, "CALLS"), 0);
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(gb, "IMPORTS"), 1);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_edge_count_by_type) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t a = cbm_gbuf_upsert_node(gb, "Function", "a", "pkg.a", "f.go", 1, 5, "{}");
    int64_t b = cbm_gbuf_upsert_node(gb, "Function", "b", "pkg.b", "f.go", 6, 10, "{}");
    int64_t c = cbm_gbuf_upsert_node(gb, "Function", "c", "pkg.c", "f.go", 11, 15, "{}");

    cbm_gbuf_insert_edge(gb, a, b, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, a, c, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, b, c, "IMPORTS", "{}");

    ASSERT_EQ(cbm_gbuf_edge_count_by_type(gb, "CALLS"), 2);
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(gb, "IMPORTS"), 1);
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(gb, "HTTP_CALLS"), 0);

    cbm_gbuf_free(gb);
    PASS();
}

/* ── Dump to SQLite ────────────────────────────────────────────── */

TEST(gbuf_dump_empty) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");

    /* Dump empty buffer should succeed */
    int rc = cbm_gbuf_flush_to_store(gb, NULL);
    /* NULL store should be handled gracefully — we just skip */
    (void)rc;

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_flush_to_store) {
    /* Create a buffer with some data */
    cbm_gbuf_t *gb = cbm_gbuf_new("test-proj", "/tmp/repo");
    int64_t n1 = cbm_gbuf_upsert_node(gb, "Function", "main", "test-proj::main.go::main", "main.go",
                                      1, 10, "{}");
    int64_t n2 = cbm_gbuf_upsert_node(gb, "Function", "helper", "test-proj::helper.go::helper",
                                      "helper.go", 1, 5, "{}");
    cbm_gbuf_insert_edge(gb, n1, n2, "CALLS", "{}");

    /* Open an in-memory store and flush */
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    int rc = cbm_gbuf_flush_to_store(gb, store);
    ASSERT_EQ(rc, 0);

    /* Verify data landed in store */
    int node_count = cbm_store_count_nodes(store, "test-proj");
    ASSERT_EQ(node_count, 2);

    int edge_count = cbm_store_count_edges(store, "test-proj");
    ASSERT_EQ(edge_count, 1);

    cbm_store_close(store);
    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_many_nodes) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");

    /* Insert 1000 nodes */
    for (int i = 0; i < 1000; i++) {
        char name[32], qn[64];
        snprintf(name, sizeof(name), "func_%d", i);
        snprintf(qn, sizeof(qn), "pkg.func_%d", i);
        int64_t id = cbm_gbuf_upsert_node(gb, "Function", name, qn, "f.go", i, i + 5, "{}");
        ASSERT_GT(id, 0);
    }
    ASSERT_EQ(cbm_gbuf_node_count(gb), 1000);

    /* Verify lookup */
    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(gb, "pkg.func_500");
    ASSERT_NOT_NULL(n);
    ASSERT_STR_EQ(n->name, "func_500");

    cbm_gbuf_free(gb);
    PASS();
}

/* ── Node edge cases ───────────────────────────────────────────── */

TEST(gbuf_upsert_null_qn) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    /* NULL qualified_name → returns 0 (error), no node inserted */
    int64_t id = cbm_gbuf_upsert_node(gb, "Function", "foo", NULL, "f.go", 1, 5, "{}");
    ASSERT_EQ(id, 0);
    ASSERT_EQ(cbm_gbuf_node_count(gb), 0);
    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_upsert_empty_qn) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    /* Empty string QN is valid — it's a non-NULL key */
    int64_t id = cbm_gbuf_upsert_node(gb, "Function", "anon", "", "f.go", 1, 5, "{}");
    ASSERT_GT(id, 0);
    ASSERT_EQ(cbm_gbuf_node_count(gb), 1);

    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(gb, "");
    ASSERT_NOT_NULL(n);
    ASSERT_STR_EQ(n->qualified_name, "");

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_upsert_same_qn_updates_all_fields) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t id1 = cbm_gbuf_upsert_node(gb, "Function", "old_name", "pkg.fn", "old.go", 1, 10,
                                       "{\"k\":\"v1\"}");
    int64_t id2 = cbm_gbuf_upsert_node(gb, "Method", "new_name", "pkg.fn", "new.go", 20, 30,
                                       "{\"k\":\"v2\"}");
    ASSERT_EQ(id1, id2);
    ASSERT_EQ(cbm_gbuf_node_count(gb), 1);

    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(gb, "pkg.fn");
    ASSERT_NOT_NULL(n);
    ASSERT_STR_EQ(n->label, "Method");
    ASSERT_STR_EQ(n->name, "new_name");
    ASSERT_STR_EQ(n->file_path, "new.go");
    ASSERT_EQ(n->start_line, 20);
    ASSERT_EQ(n->end_line, 30);
    ASSERT_STR_EQ(n->properties_json, "{\"k\":\"v2\"}");

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_upsert_long_qn) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");

    /* Build a 1200-char QN */
    char long_qn[1201];
    memset(long_qn, 'a', 1200);
    long_qn[1200] = '\0';

    int64_t id = cbm_gbuf_upsert_node(gb, "Function", "long", long_qn, "f.go", 1, 5, "{}");
    ASSERT_GT(id, 0);

    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(gb, long_qn);
    ASSERT_NOT_NULL(n);
    ASSERT_EQ(strlen(n->qualified_name), 1200);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_find_by_qn_missing) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_upsert_node(gb, "Function", "foo", "pkg.foo", "f.go", 1, 5, "{}");

    ASSERT_NULL(cbm_gbuf_find_by_qn(gb, "does.not.exist"));
    ASSERT_NULL(cbm_gbuf_find_by_qn(gb, ""));
    ASSERT_NULL(cbm_gbuf_find_by_qn(gb, "pkg.FOO")); /* case sensitive */

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_find_by_id_missing) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_upsert_node(gb, "Function", "foo", "pkg.foo", "f.go", 1, 5, "{}");

    ASSERT_NULL(cbm_gbuf_find_by_id(gb, 0));
    ASSERT_NULL(cbm_gbuf_find_by_id(gb, -1));
    ASSERT_NULL(cbm_gbuf_find_by_id(gb, 999));
    ASSERT_NULL(cbm_gbuf_find_by_id(gb, INT64_MAX));

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_find_by_label_no_matches) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_upsert_node(gb, "Function", "foo", "pkg.foo", "f.go", 1, 5, "{}");

    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    int rc = cbm_gbuf_find_by_label(gb, "Route", &nodes, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 0);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_find_by_name_multiple) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_upsert_node(gb, "Function", "init", "a.init", "a.go", 1, 5, "{}");
    cbm_gbuf_upsert_node(gb, "Function", "init", "b.init", "b.go", 1, 5, "{}");
    cbm_gbuf_upsert_node(gb, "Method", "init", "c.S.init", "c.go", 1, 5, "{}");
    cbm_gbuf_upsert_node(gb, "Function", "other", "d.other", "d.go", 1, 5, "{}");

    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    int rc = cbm_gbuf_find_by_name(gb, "init", &nodes, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 3);

    rc = cbm_gbuf_find_by_name(gb, "other", &nodes, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1);

    rc = cbm_gbuf_find_by_name(gb, "missing", &nodes, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 0);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_delete_by_label_cascades_edges) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t f = cbm_gbuf_upsert_node(gb, "Function", "fn", "pkg.fn", "f.go", 1, 5, "{}");
    int64_t c = cbm_gbuf_upsert_node(gb, "Class", "Cls", "pkg.Cls", "f.go", 10, 20, "{}");
    int64_t m = cbm_gbuf_upsert_node(gb, "Method", "meth", "pkg.Cls.meth", "f.go", 12, 18, "{}");

    /* Edges: fn→Cls, fn→meth, meth→fn */
    cbm_gbuf_insert_edge(gb, f, c, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, f, m, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, m, f, "CALLS", "{}");
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 3);

    /* Delete all Class nodes — should remove fn→Cls edge only */
    ASSERT_EQ(cbm_gbuf_delete_by_label(gb, "Class"), 0);
    ASSERT_EQ(cbm_gbuf_node_count(gb), 2);
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 2); /* fn→meth and meth→fn survive */

    /* Verify edge source/target type lookups are consistent after cascade */
    const cbm_gbuf_edge_t **edges = NULL;
    int ecount = 0;
    cbm_gbuf_find_edges_by_source_type(gb, f, "CALLS", &edges, &ecount);
    ASSERT_EQ(ecount, 1); /* only fn→meth remains */

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_delete_by_paths_cascades_edges) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    ASSERT_NOT_NULL(gb);

    int64_t a = cbm_gbuf_upsert_node(gb, "Function", "a", "pkg.a", "a.go", 1, 5, "{}");
    int64_t b = cbm_gbuf_upsert_node(gb, "Function", "b", "pkg.b", "b.go", 1, 5, "{}");
    int64_t c = cbm_gbuf_upsert_node(gb, "Function", "c", "pkg.c", "c.go", 1, 5, "{}");
    cbm_gbuf_insert_edge(gb, a, b, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, b, c, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, c, a, "CALLS", "{}");

    const char *paths[] = {"a.go", NULL, "b.go"};
    ASSERT_EQ(cbm_gbuf_delete_by_paths(gb, paths, (int)(sizeof(paths) / sizeof(paths[0]))), 2);
    ASSERT_EQ(cbm_gbuf_node_count(gb), 1);
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 0);
    ASSERT_NULL(cbm_gbuf_find_by_qn(gb, "pkg.a"));
    ASSERT_NULL(cbm_gbuf_find_by_qn(gb, "pkg.b"));
    ASSERT_NOT_NULL(cbm_gbuf_find_by_qn(gb, "pkg.c"));

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_prune_orphan_folders_removes_nested_empty_context) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    ASSERT_NOT_NULL(gb);

    int64_t project = cbm_gbuf_upsert_node(gb, "Project", "test", "test", "", 0, 0, "{}");
    int64_t source_folder =
        cbm_gbuf_upsert_node(gb, "Folder", "src", "test.src", "src", 0, 0, "{}");
    int64_t package_folder =
        cbm_gbuf_upsert_node(gb, "Folder", "pkg", "test.src.pkg", "src/pkg", 0, 0, "{}");
    int64_t file =
        cbm_gbuf_upsert_node(gb, "File", "a.go", "test.src.pkg.a", "src/pkg/a.go", 0, 0, "{}");
    ASSERT_GT(project, 0);
    ASSERT_GT(source_folder, 0);
    ASSERT_GT(package_folder, 0);
    ASSERT_GT(file, 0);
    ASSERT_GT(cbm_gbuf_insert_edge(gb, project, source_folder, "CONTAINS_FOLDER", "{}"), 0);
    ASSERT_GT(cbm_gbuf_insert_edge(gb, source_folder, package_folder, "CONTAINS_FOLDER", "{}"), 0);
    ASSERT_GT(cbm_gbuf_insert_edge(gb, package_folder, file, "CONTAINS_FILE", "{}"), 0);

    ASSERT_EQ(cbm_gbuf_prune_orphan_folders(gb), 0);
    ASSERT_EQ(cbm_gbuf_delete_by_file(gb, "src/pkg/a.go"), 1);
    ASSERT_EQ(cbm_gbuf_prune_orphan_folders(gb), 2);
    ASSERT_NOT_NULL(cbm_gbuf_find_by_qn(gb, "test"));
    ASSERT_NULL(cbm_gbuf_find_by_qn(gb, "test.src"));
    ASSERT_NULL(cbm_gbuf_find_by_qn(gb, "test.src.pkg"));
    ASSERT_EQ(cbm_gbuf_node_count(gb), 1);
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 0);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_node_count_empty) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    ASSERT_EQ(cbm_gbuf_node_count(gb), 0);
    ASSERT_EQ(cbm_gbuf_node_count(NULL), 0);
    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_upsert_100_nodes_stress) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    for (int i = 0; i < 100; i++) {
        char name[32], qn[64];
        snprintf(name, sizeof(name), "f%d", i);
        snprintf(qn, sizeof(qn), "pkg.f%d", i);
        int64_t id = cbm_gbuf_upsert_node(gb, "Function", name, qn, "f.go", i, i + 1, "{}");
        ASSERT_GT(id, 0);
    }
    ASSERT_EQ(cbm_gbuf_node_count(gb), 100);

    /* Verify each node is findable */
    for (int i = 0; i < 100; i++) {
        char qn[64];
        snprintf(qn, sizeof(qn), "pkg.f%d", i);
        ASSERT_NOT_NULL(cbm_gbuf_find_by_qn(gb, qn));
    }

    cbm_gbuf_free(gb);
    PASS();
}

/* ── Edge edge cases ──────────────────────────────────────────── */

TEST(gbuf_edge_nonexistent_endpoints) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    /* Edge insertion stays append-oriented; the pre-dump invariant validator
     * owns structural endpoint checks so producers can be diagnosed together. */
    int64_t eid = cbm_gbuf_insert_edge(gb, 9999, 8888, "CALLS", "{}");
    ASSERT_GT(eid, 0);
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 1);
    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_validate_invariants_valid_graph) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    ASSERT_NOT_NULL(gb);
    int64_t a = cbm_gbuf_upsert_node(gb, "Function", "a", "pkg.a", "f.go", 1, 5, "{}");
    int64_t b = cbm_gbuf_upsert_node(gb, "Function", "b", "pkg.b", "f.go", 6, 10, "{}");
    ASSERT_GT(a, 0);
    ASSERT_GT(b, 0);
    ASSERT_GT(cbm_gbuf_insert_edge(gb, a, b, "CALLS", "{}"), 0);

    char err[CBM_SZ_256];
    ASSERT_EQ(cbm_gbuf_validate_invariants(gb, err, sizeof(err)), 0);
    ASSERT_STR_EQ(err, "");

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_dump_rejects_missing_edge_endpoint) {
    char path[256];
    ASSERT_EQ(gbuf_make_temp_db(path, sizeof(path)), 0);
    cbm_unlink(path);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    ASSERT_NOT_NULL(gb);
    int64_t a = cbm_gbuf_upsert_node(gb, "Function", "a", "pkg.a", "f.go", 1, 5, "{}");
    ASSERT_GT(a, 0);
    ASSERT_GT(cbm_gbuf_insert_edge(gb, a, 9999, "CALLS", "{}"), 0);

    char err[CBM_SZ_256];
    ASSERT_NEQ(cbm_gbuf_validate_invariants(gb, err, sizeof(err)), 0);
    ASSERT(strstr(err, "endpoint") != NULL);
    ASSERT_NEQ(cbm_gbuf_dump_to_sqlite(gb, path), 0);

    FILE *f = fopen(path, "rb");
    ASSERT_NULL(f);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_edge_dedup_merges_properties) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t a = cbm_gbuf_upsert_node(gb, "Function", "a", "pkg.a", "f.go", 1, 5, "{}");
    int64_t b = cbm_gbuf_upsert_node(gb, "Function", "b", "pkg.b", "f.go", 6, 10, "{}");

    int64_t eid1 = cbm_gbuf_insert_edge(gb, a, b, "CALLS", "{\"weight\":1}");
    int64_t eid2 = cbm_gbuf_insert_edge(gb, a, b, "CALLS", "{\"weight\":5}");
    ASSERT_EQ(eid1, eid2);
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 1);

    /* Verify second insert's properties win (merge = replace) */
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_source_type(gb, a, "CALLS", &edges, &count);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(edges[0]->properties_json, "{\"weight\":5}");

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_edge_count_empty) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 0);
    ASSERT_EQ(cbm_gbuf_edge_count(NULL), 0);
    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_edge_count_by_type_missing) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(gb, "CALLS"), 0);
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(gb, "UNKNOWN"), 0);
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(gb, ""), 0);
    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_delete_edges_preserves_other_types) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t a = cbm_gbuf_upsert_node(gb, "Function", "a", "pkg.a", "f.go", 1, 5, "{}");
    int64_t b = cbm_gbuf_upsert_node(gb, "Function", "b", "pkg.b", "f.go", 6, 10, "{}");
    int64_t c = cbm_gbuf_upsert_node(gb, "Function", "c", "pkg.c", "f.go", 11, 15, "{}");

    cbm_gbuf_insert_edge(gb, a, b, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, b, c, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, a, b, "IMPORTS", "{}");
    cbm_gbuf_insert_edge(gb, a, c, "HTTP_CALLS", "{}");
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 4);

    cbm_gbuf_delete_edges_by_type(gb, "CALLS");
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 2);
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(gb, "CALLS"), 0);
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(gb, "IMPORTS"), 1);
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(gb, "HTTP_CALLS"), 1);

    /* Verify secondary indexes are consistent after rebuild */
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_source_type(gb, a, "IMPORTS", &edges, &count);
    ASSERT_EQ(count, 1);
    cbm_gbuf_find_edges_by_target_type(gb, c, "HTTP_CALLS", &edges, &count);
    ASSERT_EQ(count, 1);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_delete_edges_by_type_matching_props) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t a = cbm_gbuf_upsert_node(gb, "Function", "a", "pkg.a", "f.go", 1, 5, "{}");
    int64_t b = cbm_gbuf_upsert_node(gb, "Route", "/b", "__route__GET__/b", "f.go", 6, 10, "{}");
    int64_t c = cbm_gbuf_upsert_node(gb, "Route", "/c", "__route__GET__/c", "f.go", 11, 15, "{}");

    cbm_gbuf_insert_edge(gb, a, b, "HANDLES", "{\"source\":\"prefix_decorator_bridge\"}");
    cbm_gbuf_insert_edge(gb, a, c, "HANDLES", "{\"handler\":\"pkg.a\"}");
    cbm_gbuf_insert_edge(gb, a, c, "CALLS", "{\"source\":\"prefix_decorator_bridge\"}");
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 3);

    int deleted = cbm_gbuf_delete_edges_by_type_matching_props(
        gb, "HANDLES", "\"source\":\"prefix_decorator_bridge\"");
    ASSERT_EQ(deleted, 1);
    ASSERT_EQ(cbm_gbuf_edge_count(gb), 2);
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(gb, "HANDLES"), 1);
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(gb, "CALLS"), 1);

    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_target_type(gb, b, "HANDLES", &edges, &count);
    ASSERT_EQ(count, 0);
    cbm_gbuf_find_edges_by_target_type(gb, c, "HANDLES", &edges, &count);
    ASSERT_EQ(count, 1);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_find_edges_by_target_type_multiple) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t a = cbm_gbuf_upsert_node(gb, "Function", "a", "pkg.a", "f.go", 1, 5, "{}");
    int64_t b = cbm_gbuf_upsert_node(gb, "Function", "b", "pkg.b", "f.go", 6, 10, "{}");
    int64_t c = cbm_gbuf_upsert_node(gb, "Function", "c", "pkg.c", "f.go", 11, 15, "{}");

    /* Both a and c call b */
    cbm_gbuf_insert_edge(gb, a, b, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, c, b, "CALLS", "{}");

    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_target_type(gb, b, "CALLS", &edges, &count);
    ASSERT_EQ(count, 2);

    /* No IMPORTS edges targeting b */
    cbm_gbuf_find_edges_by_target_type(gb, b, "IMPORTS", &edges, &count);
    ASSERT_EQ(count, 0);

    cbm_gbuf_free(gb);
    PASS();
}

/* ── Merge tests ──────────────────────────────────────────────── */

TEST(gbuf_merge_overlapping_qns) {
    cbm_gbuf_t *dst = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_t *src = cbm_gbuf_new("test", "/tmp");

    /* dst has node with QN "pkg.fn" */
    cbm_gbuf_upsert_node(dst, "Function", "fn_old", "pkg.fn", "old.go", 1, 10,
                         "{\"from\":\"dst\"}");
    cbm_gbuf_upsert_node(dst, "Function", "unique_dst", "pkg.unique_dst", "u.go", 1, 5, "{}");

    /* src has the same QN with a richer source span, so it should win */
    cbm_gbuf_upsert_node(src, "Method", "fn_new", "pkg.fn", "new.go", 20, 30, "{\"from\":\"src\"}");
    cbm_gbuf_upsert_node(src, "Function", "unique_src", "pkg.unique_src", "s.go", 1, 5, "{}");

    int rc = cbm_gbuf_merge(dst, src);
    ASSERT_EQ(rc, 0);

    /* Total: 3 nodes (1 merged + 1 dst-only + 1 src-only) */
    ASSERT_EQ(cbm_gbuf_node_count(dst), 3);

    /* Verify the richer src fields won for the overlapping QN */
    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(dst, "pkg.fn");
    ASSERT_NOT_NULL(n);
    ASSERT_STR_EQ(n->label, "Method");
    ASSERT_STR_EQ(n->name, "fn_new");
    ASSERT_STR_EQ(n->file_path, "new.go");
    ASSERT_EQ(n->start_line, 20);
    ASSERT_STR_EQ(n->properties_json, "{\"from\":\"src\"}");

    /* Both unique nodes present */
    ASSERT_NOT_NULL(cbm_gbuf_find_by_qn(dst, "pkg.unique_dst"));
    ASSERT_NOT_NULL(cbm_gbuf_find_by_qn(dst, "pkg.unique_src"));

    cbm_gbuf_free(dst);
    cbm_gbuf_free(src);
    PASS();
}

TEST(gbuf_merge_reindexes_label_and_name) {
    enum {
        COMPAT_DECL_START = 48,
        COMPAT_DECL_END = 50,
        COMPAT_DEF_START = 197,
        COMPAT_DEF_END = 230,
    };
    cbm_gbuf_t *dst = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_t *src = cbm_gbuf_new("test", "/tmp");

    cbm_gbuf_upsert_node(dst, "Macro", "OLD_NAME", "pkg.compat.cbm_getline", "compat.h",
                         COMPAT_DECL_START, COMPAT_DECL_END, "{}");
    cbm_gbuf_upsert_node(src, "Function", "cbm_getline", "pkg.compat.cbm_getline",
                         "compat.c", COMPAT_DEF_START, COMPAT_DEF_END,
                         "{\"loop_depth\":1,\"self_recursive\":false}");
    ASSERT_EQ(cbm_gbuf_merge(dst, src), 0);

    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    ASSERT_EQ(cbm_gbuf_find_by_label(dst, "Macro", &nodes, &count), 0);
    ASSERT_EQ(count, 0);
    ASSERT_EQ(cbm_gbuf_find_by_label(dst, "Function", &nodes, &count), 0);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(nodes[0]->name, "cbm_getline");

    ASSERT_EQ(cbm_gbuf_find_by_name(dst, "OLD_NAME", &nodes, &count), 0);
    ASSERT_EQ(count, 0);
    ASSERT_EQ(cbm_gbuf_find_by_name(dst, "cbm_getline", &nodes, &count), 0);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(nodes[0]->label, "Function");

    cbm_gbuf_free(dst);
    cbm_gbuf_free(src);
    PASS();
}

TEST(gbuf_merge_route_file_path_is_deterministic) {
    cbm_gbuf_t *dst = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_t *src = cbm_gbuf_new("test", "/tmp");
    const char *route_qn = "__route__GET__/items/{}";

    cbm_gbuf_upsert_node(dst, "Route", "/items/{item_id}", route_qn, "z/last.py", 0, 0, "{}");
    cbm_gbuf_upsert_node(src, "Route", "/items/{id}", route_qn, "a/first.py", 0, 0,
                         "{\"method\":\"GET\",\"source\":\"decorator\"}");

    int rc = cbm_gbuf_merge(dst, src);
    ASSERT_EQ(rc, 0);

    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(dst, route_qn);
    ASSERT_NOT_NULL(n);
    ASSERT_STR_EQ(n->name, "/items/{id}");
    ASSERT_STR_EQ(n->file_path, "a/first.py");
    ASSERT_STR_EQ(n->properties_json, "{\"method\":\"GET\",\"source\":\"decorator\"}");

    cbm_gbuf_free(dst);
    cbm_gbuf_free(src);
    PASS();
}

TEST(gbuf_merge_section_file_path_is_deterministic) {
    cbm_gbuf_t *dst = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_t *src = cbm_gbuf_new("test", "/tmp");
    const char *section_qn = "proj.docs.deployment.Implantacao";

    cbm_gbuf_upsert_node(dst, "Section", "Implantacao", section_qn, "docs/deployment/index.md",
                         1, 2, "{}");
    cbm_gbuf_upsert_node(src, "Section", "Implantacao", section_qn, "docs/deployment.md", 1, 2,
                         "{}");

    int rc = cbm_gbuf_merge(dst, src);
    ASSERT_EQ(rc, 0);

    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(dst, section_qn);
    ASSERT_NOT_NULL(n);
    ASSERT_STR_EQ(n->file_path, "docs/deployment.md");

    cbm_gbuf_free(dst);
    cbm_gbuf_free(src);
    PASS();
}

TEST(gbuf_merge_definition_source_prefers_richer_span) {
    enum {
        DECL_START_LINE = 7,
        DECL_END_LINE = 7,
        FULL_DEF_START_LINE = 34,
        FULL_DEF_END_LINE = 43,
    };
    const char *qn = "proj.TypeName";

    cbm_gbuf_t *dst = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_t *src = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_upsert_node(dst, "Class", "TypeName", qn, "include/type.h", DECL_START_LINE,
                         DECL_END_LINE, "{\"source\":\"declaration\"}");
    cbm_gbuf_upsert_node(src, "Class", "TypeName", qn, "src/type.c", FULL_DEF_START_LINE,
                         FULL_DEF_END_LINE, "{\"source\":\"definition\"}");
    ASSERT_EQ(cbm_gbuf_merge(dst, src), 0);
    ASSERT_EQ(assert_full_definition_source(dst, qn), 0);
    cbm_gbuf_free(dst);
    cbm_gbuf_free(src);

    dst = cbm_gbuf_new("test", "/tmp");
    src = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_upsert_node(dst, "Class", "TypeName", qn, "src/type.c", FULL_DEF_START_LINE,
                         FULL_DEF_END_LINE, "{\"source\":\"definition\"}");
    cbm_gbuf_upsert_node(src, "Class", "TypeName", qn, "include/type.h", DECL_START_LINE,
                         DECL_END_LINE, "{\"source\":\"declaration\"}");
    ASSERT_EQ(cbm_gbuf_merge(dst, src), 0);
    ASSERT_EQ(assert_full_definition_source(dst, qn), 0);
    cbm_gbuf_free(dst);
    cbm_gbuf_free(src);

    PASS();
}

TEST(gbuf_merge_module_source_prefers_richer_span) {
    enum {
        INSTALL_START_LINE = 1,
        INSTALL_PS_END_LINE = 155,
        INSTALL_SH_END_LINE = 221,
    };
    const char *qn = "proj.install";

    cbm_gbuf_t *dst = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_t *src = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_upsert_node(dst, "Module", "install.ps1", qn, "install.ps1", INSTALL_START_LINE,
                         INSTALL_PS_END_LINE, "{\"source\":\"powershell\"}");
    cbm_gbuf_upsert_node(src, "Module", "install.sh", qn, "install.sh", INSTALL_START_LINE,
                         INSTALL_SH_END_LINE, "{\"source\":\"shell\"}");
    ASSERT_EQ(cbm_gbuf_merge(dst, src), 0);
    ASSERT_EQ(assert_install_sh_module_source(dst, qn), 0);
    cbm_gbuf_free(dst);
    cbm_gbuf_free(src);

    dst = cbm_gbuf_new("test", "/tmp");
    src = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_upsert_node(dst, "Module", "install.sh", qn, "install.sh", INSTALL_START_LINE,
                         INSTALL_SH_END_LINE, "{\"source\":\"shell\"}");
    cbm_gbuf_upsert_node(src, "Module", "install.ps1", qn, "install.ps1", INSTALL_START_LINE,
                         INSTALL_PS_END_LINE, "{\"source\":\"powershell\"}");
    ASSERT_EQ(cbm_gbuf_merge(dst, src), 0);
    ASSERT_EQ(assert_install_sh_module_source(dst, qn), 0);
    cbm_gbuf_free(dst);
    cbm_gbuf_free(src);

    PASS();
}

TEST(gbuf_merge_edge_dedup) {
    _Atomic int64_t shared = 1;
    cbm_gbuf_t *dst = cbm_gbuf_new_shared_ids("test", "/tmp", &shared);
    cbm_gbuf_t *src = cbm_gbuf_new_shared_ids("test", "/tmp", &shared);

    int64_t a = cbm_gbuf_upsert_node(dst, "Function", "a", "pkg.a", "f.go", 1, 5, "{}");
    int64_t b = cbm_gbuf_upsert_node(dst, "Function", "b", "pkg.b", "f.go", 6, 10, "{}");
    cbm_gbuf_insert_edge(dst, a, b, "CALLS", "{}");

    /* src has same nodes (by QN) and same edge */
    cbm_gbuf_upsert_node(src, "Function", "a", "pkg.a", "f.go", 1, 5, "{}");
    cbm_gbuf_upsert_node(src, "Function", "b", "pkg.b", "f.go", 6, 10, "{}");
    /* This edge's src/target IDs differ from dst's, but after merge remap
     * it becomes the same (src, tgt, type) tuple → deduped */
    int64_t sa = cbm_gbuf_find_by_qn(src, "pkg.a")->id;
    int64_t sb = cbm_gbuf_find_by_qn(src, "pkg.b")->id;
    cbm_gbuf_insert_edge(src, sa, sb, "CALLS", "{}");

    int rc = cbm_gbuf_merge(dst, src);
    ASSERT_EQ(rc, 0);

    ASSERT_EQ(cbm_gbuf_node_count(dst), 2);
    ASSERT_EQ(cbm_gbuf_edge_count(dst), 1); /* deduped */

    cbm_gbuf_free(dst);
    cbm_gbuf_free(src);
    PASS();
}

TEST(gbuf_merge_empty_src_into_populated_dst) {
    cbm_gbuf_t *dst = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_t *src = cbm_gbuf_new("test", "/tmp");

    cbm_gbuf_upsert_node(dst, "Function", "foo", "pkg.foo", "f.go", 1, 5, "{}");
    cbm_gbuf_upsert_node(dst, "Function", "bar", "pkg.bar", "f.go", 6, 10, "{}");

    int rc = cbm_gbuf_merge(dst, src);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(cbm_gbuf_node_count(dst), 2); /* unchanged */

    cbm_gbuf_free(dst);
    cbm_gbuf_free(src);
    PASS();
}

TEST(gbuf_merge_populated_src_into_empty_dst) {
    cbm_gbuf_t *dst = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_t *src = cbm_gbuf_new("test", "/tmp");

    int64_t a = cbm_gbuf_upsert_node(src, "Function", "foo", "pkg.foo", "f.go", 1, 5, "{}");
    int64_t b = cbm_gbuf_upsert_node(src, "Function", "bar", "pkg.bar", "f.go", 6, 10, "{}");
    cbm_gbuf_insert_edge(src, a, b, "CALLS", "{}");

    int rc = cbm_gbuf_merge(dst, src);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(cbm_gbuf_node_count(dst), 2);
    ASSERT_EQ(cbm_gbuf_edge_count(dst), 1);

    /* Verify nodes are findable in dst */
    ASSERT_NOT_NULL(cbm_gbuf_find_by_qn(dst, "pkg.foo"));
    ASSERT_NOT_NULL(cbm_gbuf_find_by_qn(dst, "pkg.bar"));

    cbm_gbuf_free(dst);
    cbm_gbuf_free(src);
    PASS();
}

TEST(gbuf_merge_null_args) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    ASSERT_EQ(cbm_gbuf_merge(NULL, gb), -1);
    ASSERT_EQ(cbm_gbuf_merge(gb, NULL), -1);
    ASSERT_EQ(cbm_gbuf_merge(NULL, NULL), -1);
    cbm_gbuf_free(gb);
    PASS();
}

/* ── Flush / merge-into-store tests ───────────────────────────── */

TEST(gbuf_flush_to_store_null) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    /* NULL store returns -1 */
    ASSERT_EQ(cbm_gbuf_flush_to_store(gb, NULL), -1);
    /* NULL gbuf returns -1 */
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_EQ(cbm_gbuf_flush_to_store(NULL, store), -1);
    cbm_store_close(store);
    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_flush_verify_store_data) {
    cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp/repo");
    int64_t n1 = cbm_gbuf_upsert_node(gb, "Function", "alpha", "proj::alpha", "a.go", 1, 10, "{}");
    int64_t n2 = cbm_gbuf_upsert_node(gb, "Class", "Beta", "proj::Beta", "b.go", 1, 20, "{}");
    int64_t n3 = cbm_gbuf_upsert_node(gb, "Function", "gamma", "proj::gamma", "c.go", 1, 5, "{}");
    cbm_gbuf_insert_edge(gb, n1, n2, "CALLS", "{}");
    cbm_gbuf_insert_edge(gb, n1, n3, "IMPORTS", "{}");

    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    int rc = cbm_gbuf_flush_to_store(gb, store);
    ASSERT_EQ(rc, 0);

    /* Verify counts */
    ASSERT_EQ(cbm_store_count_nodes(store, "proj"), 3);
    ASSERT_EQ(cbm_store_count_edges(store, "proj"), 2);

    /* Verify node lookup by QN */
    cbm_node_t out;
    rc = cbm_store_find_node_by_qn(store, "proj", "proj::alpha", &out);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(out.name, "alpha");
    ASSERT_STR_EQ(out.label, "Function");
    cbm_node_free_fields(&out);

    rc = cbm_store_find_node_by_qn(store, "proj", "proj::Beta", &out);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_STR_EQ(out.label, "Class");
    cbm_node_free_fields(&out);

    cbm_store_close(store);
    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_flush_begin_failure_preserves_existing_project) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_upsert_project(store, "proj", "/tmp/repo"), CBM_STORE_OK);

    cbm_node_t existing = {
        .project = "proj",
        .label = "Function",
        .name = "existing",
        .qualified_name = "proj::existing",
        .file_path = "old.go",
        .start_line = 1,
        .end_line = 3,
        .properties_json = "{}",
    };
    ASSERT_GT(cbm_store_upsert_node(store, &existing), 0);
    ASSERT_EQ(cbm_store_count_nodes(store, "proj"), 1);

    ASSERT_EQ(cbm_store_begin(store), CBM_STORE_OK);

    cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp/repo");
    ASSERT_NOT_NULL(gb);
    cbm_gbuf_upsert_node(gb, "Function", "replacement", "proj::replacement", "new.go", 1, 5,
                         "{}");

    ASSERT_NEQ(cbm_gbuf_flush_to_store(gb, store), 0);
    ASSERT_EQ(cbm_store_count_nodes(store, "proj"), 1);

    cbm_node_t out = {0};
    ASSERT_EQ(cbm_store_find_node_by_qn(store, "proj", "proj::existing", &out), CBM_STORE_OK);
    cbm_node_free_fields(&out);
    ASSERT_EQ(cbm_store_find_node_by_qn(store, "proj", "proj::replacement", &out),
              CBM_STORE_NOT_FOUND);

    ASSERT_EQ(cbm_store_rollback(store), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_count_nodes(store, "proj"), 1);

    cbm_gbuf_free(gb);
    cbm_store_close(store);
    PASS();
}

TEST(gbuf_merge_into_store_preserves) {
    /* First, flush initial data via flush_to_store */
    cbm_gbuf_t *gb1 = cbm_gbuf_new("proj", "/tmp/repo");
    cbm_gbuf_upsert_node(gb1, "Function", "existing", "proj::existing", "e.go", 1, 10, "{}");

    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    cbm_store_upsert_project(store, "proj", "/tmp/repo");
    int rc = cbm_gbuf_flush_to_store(gb1, store);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(cbm_store_count_nodes(store, "proj"), 1);

    /* Now merge_into_store with new data — should NOT delete existing */
    cbm_gbuf_t *gb2 = cbm_gbuf_new("proj", "/tmp/repo");
    cbm_gbuf_upsert_node(gb2, "Function", "newone", "proj::newone", "n.go", 1, 5, "{}");

    rc = cbm_gbuf_merge_into_store(gb2, store);
    ASSERT_EQ(rc, 0);

    /* Both nodes should exist */
    ASSERT_EQ(cbm_store_count_nodes(store, "proj"), 2);

    cbm_node_t out;
    rc = cbm_store_find_node_by_qn(store, "proj", "proj::existing", &out);
    ASSERT_EQ(rc, CBM_STORE_OK);
    cbm_node_free_fields(&out);

    rc = cbm_store_find_node_by_qn(store, "proj", "proj::newone", &out);
    ASSERT_EQ(rc, CBM_STORE_OK);
    cbm_node_free_fields(&out);

    cbm_store_close(store);
    cbm_gbuf_free(gb1);
    cbm_gbuf_free(gb2);
    PASS();
}

/* ── Shared ID tests ──────────────────────────────────────────── */

TEST(gbuf_shared_ids_unique) {
    _Atomic int64_t shared = 1;
    cbm_gbuf_t *gb1 = cbm_gbuf_new_shared_ids("test", "/tmp", &shared);
    cbm_gbuf_t *gb2 = cbm_gbuf_new_shared_ids("test", "/tmp", &shared);

    int64_t id1 = cbm_gbuf_upsert_node(gb1, "Function", "a", "pkg.a", "f.go", 1, 5, "{}");
    int64_t id2 = cbm_gbuf_upsert_node(gb2, "Function", "b", "pkg.b", "f.go", 6, 10, "{}");
    int64_t id3 = cbm_gbuf_upsert_node(gb1, "Function", "c", "pkg.c", "f.go", 11, 15, "{}");
    int64_t id4 = cbm_gbuf_upsert_node(gb2, "Function", "d", "pkg.d", "f.go", 16, 20, "{}");

    /* All IDs must be unique */
    ASSERT_NEQ(id1, id2);
    ASSERT_NEQ(id1, id3);
    ASSERT_NEQ(id1, id4);
    ASSERT_NEQ(id2, id3);
    ASSERT_NEQ(id2, id4);
    ASSERT_NEQ(id3, id4);

    /* IDs should be sequential from the shared source */
    ASSERT_EQ(id1, 1);
    ASSERT_EQ(id2, 2);
    ASSERT_EQ(id3, 3);
    ASSERT_EQ(id4, 4);

    cbm_gbuf_free(gb1);
    cbm_gbuf_free(gb2);
    PASS();
}

TEST(gbuf_shared_ids_null_fallback) {
    /* NULL id_source → behaves like cbm_gbuf_new() */
    cbm_gbuf_t *gb = cbm_gbuf_new_shared_ids("test", "/tmp", NULL);
    ASSERT_NOT_NULL(gb);
    int64_t id = cbm_gbuf_upsert_node(gb, "Function", "a", "pkg.a", "f.go", 1, 5, "{}");
    ASSERT_EQ(id, 1);
    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_next_id_set_next_id_roundtrip) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");

    /* Initial next_id is 1 */
    ASSERT_EQ(cbm_gbuf_next_id(gb), 1);

    /* Insert a node, next_id advances */
    cbm_gbuf_upsert_node(gb, "Function", "a", "pkg.a", "f.go", 1, 5, "{}");
    ASSERT_EQ(cbm_gbuf_next_id(gb), 2);

    /* Set next_id to an arbitrary value */
    cbm_gbuf_set_next_id(gb, 100);
    ASSERT_EQ(cbm_gbuf_next_id(gb), 100);

    /* Next insert uses the new base */
    int64_t id = cbm_gbuf_upsert_node(gb, "Function", "b", "pkg.b", "f.go", 6, 10, "{}");
    ASSERT_EQ(id, 100);
    ASSERT_EQ(cbm_gbuf_next_id(gb), 101);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_next_id_null_safe) {
    /* next_id on NULL returns 1 (default) */
    ASSERT_EQ(cbm_gbuf_next_id(NULL), 1);
    /* set_next_id on NULL should not crash */
    cbm_gbuf_set_next_id(NULL, 42);
    PASS();
}

/* ── Flush edge case: orphan edges skipped ────────────────────── */

TEST(gbuf_flush_skips_orphan_edges) {
    cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp/repo");
    int64_t n1 = cbm_gbuf_upsert_node(gb, "Function", "real", "proj::real", "f.go", 1, 5, "{}");

    /* Valid edge */
    cbm_gbuf_insert_edge(gb, n1, n1, "CALLS", "{}");
    /* Orphan edge — target ID 9999 does not map to any node */
    cbm_gbuf_insert_edge(gb, n1, 9999, "CALLS", "{}");

    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    int rc = cbm_gbuf_flush_to_store(gb, store);
    ASSERT_EQ(rc, 0);

    /* Only the valid edge should land in the store */
    ASSERT_EQ(cbm_store_count_nodes(store, "proj"), 1);
    ASSERT_EQ(cbm_store_count_edges(store, "proj"), 1);

    cbm_store_close(store);
    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_flush_bulk_edges_preserves_buffer_properties) {
    cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp/repo");
    int64_t ids[40];
    char name[CBM_SZ_32];
    char qn[CBM_SZ_64];
    for (int i = 0; i < 40; i++) {
        snprintf(name, sizeof(name), "n%d", i);
        snprintf(qn, sizeof(qn), "proj::n%d", i);
        ids[i] = cbm_gbuf_upsert_node(gb, "Function", name, qn, "f.c", i + 1, i + 1, "{}");
    }

    cbm_gbuf_insert_edge(gb, ids[0], ids[1], "CALLS", "{\"first\":1}");
    cbm_gbuf_insert_edge(gb, ids[0], ids[1], "CALLS", "{\"second\":2}");
    for (int i = 1; i < 35; i++) {
        cbm_gbuf_insert_edge(gb, ids[i], ids[i + 1], "CALLS", "{}");
    }

    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_gbuf_flush_to_store(gb, store), 0);
    ASSERT_EQ(cbm_store_count_nodes(store, "proj"), 40);
    ASSERT_EQ(cbm_store_count_edges(store, "proj"), 35);

    cbm_node_t first = {0};
    ASSERT_EQ(cbm_store_find_node_by_qn(store, "proj", "proj::n0", &first), CBM_STORE_OK);
    cbm_edge_t *edges = NULL;
    int count = 0;
    ASSERT_EQ(cbm_store_find_edges_by_source_type(store, first.id, "CALLS", &edges, &count),
              CBM_STORE_OK);
    ASSERT_EQ(count, 1);
    ASSERT(strstr(edges[0].properties_json, "\"first\":1") == NULL);
    ASSERT(strstr(edges[0].properties_json, "\"second\":2") != NULL);
    cbm_store_free_edges(edges, count);
    cbm_node_free_fields(&first);

    cbm_store_close(store);
    cbm_gbuf_free(gb);
    PASS();
}

/* ── Suite ─────────────────────────────────────────────────────── */

/* B1 pipeline-path isolation probe (#23): cbm_write_db is clean 10/10 even with
 * variable-length records (decisive negative this session), and the streaming
 * dump for <65536 nodes is byte-identical to cbm_write_db at the writer level
 * (DUMP_PARTITION_NODES=1<<16 → one partition = all nodes). So the ONLY hop the
 * real pipeline runs that the writer test skips is the gbuf→dump handoff:
 * build_dump_nodes / build_dump_edges / temp_to_final ID remap, fed by a
 * merge-populated gbuf (parallel workers → cbm_gbuf_merge). This test drives
 * that exact path with variable-length heap properties_json + a merged worker
 * gbuf. If it corrupts → root cause isolated in the handoff. If clean → the
 * bug lives in extraction-population (tree-sitter), which needs the real repo. */
TEST(gbuf_dump_pipeline_path_integrity) {
    char path[256];
    ASSERT_EQ(gbuf_make_temp_db(path, sizeof(path)), 0);

    const int N = 15000;   /* < 65536 → one dump partition, mirrors fastapi scale */
    const int W = 3000;    /* worker gbuf nodes, merged in (exercises remap) */
    const int E = 50000;

    cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp/gbuf_pipeline_root");
    ASSERT_NOT_NULL(gb);

    /* Variable-length heap properties_json, exactly like real extraction emits. */
    static const int plens[] = {20, 200, 800, 1500, 50, 400, 1000, 100};
    char name[64], qn[96], props[2048];
    for (int i = 0; i < N; i++) {
        snprintf(name, sizeof(name), "fn_%d", i);
        snprintf(qn, sizeof(qn), "proj.mod.fn_%d", i);
        int target = plens[i % 8];
        int padlen = target - 8; /* {"k":""} overhead */
        if (padlen < 0) padlen = 0;
        if (padlen > 2040) padlen = 2040;
        props[0] = '{'; props[1] = '"'; props[2] = 'k'; props[3] = '"'; props[4] = ':';
        props[5] = '"';
        memset(props + 6, 'y', (size_t)padlen);
        props[6 + padlen] = '"';
        props[6 + padlen + 1] = '}';
        props[6 + padlen + 2] = '\0';
        int64_t id = cbm_gbuf_upsert_node(gb, "Function", name, qn,
                                          (i % 400 == 0) ? "src/base.py" : "src/mod.py",
                                          i + 1, i + 2, props);
        ASSERT_GT(id, 0);
    }
    for (int i = 0; i < E; i++) {
        int64_t s = (i % N) + 1;
        int64_t t = ((i / N) % N) + 1;
        if (s == t) t = (t % N) + 1;
        cbm_gbuf_insert_edge(gb, s, t, "CALLS", "{}");
    }

    /* Worker gbuf: some NEW qns + some colliding qns, then merge (parallel-pipeline
     * simulation — exercises cbm_gbuf_merge + the QN-collision ID remap). */
    _Atomic int64_t shared_ids;
    atomic_init(&shared_ids, cbm_gbuf_next_id(gb));
    cbm_gbuf_t *gw = cbm_gbuf_new_shared_ids("proj", "/tmp/gbuf_pipeline_root", &shared_ids);
    ASSERT_NOT_NULL(gw);
    for (int i = 0; i < W; i++) {
        if (i % 2 == 0) {
            /* collide with an existing main qn (merge_update_existing path) */
            snprintf(qn, sizeof(qn), "proj.mod.fn_%d", i % 1000);
        } else {
            /* brand-new qn (merge_copy_new_node path) */
            snprintf(qn, sizeof(qn), "proj.worker.w_%d", i);
        }
        snprintf(name, sizeof(name), "w_%d", i);
        cbm_gbuf_upsert_node(gw, "Function", name, qn, "src/worker.py", 1, 2,
                             "{\"w\":true}");
    }
    ASSERT_EQ(cbm_gbuf_merge(gb, gw), 0);

    /* The dump path under test. */
    ASSERT_EQ(cbm_gbuf_dump_to_sqlite(gb, path), 0);

    /* Verify: structural integrity + exact root_path round-trip + counts. */
    sqlite3 *db = NULL;
    ASSERT_EQ(sqlite3_open(path, &db), SQLITE_OK);
    sqlite3_stmt *stmt = NULL;

    sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &stmt, NULL);
    ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 0), "ok");
    sqlite3_finalize(stmt);

    sqlite3_prepare_v2(db, "SELECT root_path FROM projects", -1, &stmt, NULL);
    ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 0), "/tmp/gbuf_pipeline_root");
    sqlite3_finalize(stmt);

    sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM nodes", -1, &stmt, NULL);
    sqlite3_step(stmt);
    int ncount = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    /* N main + (W/2) new worker qns (the colliding half merges into existing). */
    ASSERT_EQ(ncount, N + (W / 2));

    sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM edges", -1, &stmt, NULL);
    sqlite3_step(stmt);
    int ecount = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    ASSERT_GT(ecount, 0);

    sqlite3_close(db);
    cbm_unlink(path);
    cbm_gbuf_free(gw);
    cbm_gbuf_free(gb);
    PASS();
}

/* Regression: a RELATIVE root_path (e.g. ".") must not cause the post-dump
 * verify to delete a valid DB. The integrity check flags non-absolute
 * root_paths as bad_root_path, but that's a cosmetic project-row defect
 * (path_only) — the node/edge data is intact. The dump-verify must RETAIN
 * (path_only) like #557, not delete. Caught by self-indexing the repo with
 * repo_path="." (which failed with status=error before the fix). */
TEST(gbuf_dump_relative_root_path_retained) {
    char path[256];
    ASSERT_EQ(gbuf_make_temp_db(path, sizeof(path)), 0);

    cbm_gbuf_t *gb = cbm_gbuf_new("proj", ".");
    ASSERT_NOT_NULL(gb);
    cbm_gbuf_upsert_node(gb, "Function", "main", "proj.main", "main.c", 1, 2, "{}");

    /* Must succeed (DB retained) despite the relative "." root_path. */
    ASSERT_EQ(cbm_gbuf_dump_to_sqlite(gb, path), 0);

    /* The DB file must still exist (not deleted by the verify). */
    FILE *f = fopen(path, "rb");
    ASSERT_NOT_NULL(f);
    if (f) {
        fclose(f);
    }

    cbm_unlink(path);
    cbm_gbuf_free(gb);
    PASS();
}

TEST(gbuf_dump_failure_before_replace_keeps_existing_db) {
    static const char *fail_env = CBM_TEST_FAIL_GBUF_DUMP_BEFORE_REPLACE;
    char saved_fail[CBM_SZ_32] = {0};
    bool had_fail = cbm_safe_getenv(fail_env, saved_fail, sizeof(saved_fail), NULL) != NULL;

    char path[256];
    ASSERT_EQ(gbuf_make_temp_db(path, sizeof(path)), 0);

    cbm_gbuf_t *old_gb = cbm_gbuf_new("proj", "/tmp/repo");
    ASSERT_NOT_NULL(old_gb);
    cbm_gbuf_upsert_node(old_gb, "Function", "old", "proj.old", "old.c", 1, 2, "{}");
    ASSERT_EQ(cbm_gbuf_dump_to_sqlite(old_gb, path), 0);
    cbm_gbuf_free(old_gb);

    ASSERT(gbuf_store_has_qn(path, "proj", "proj.old"));
    ASSERT(!gbuf_store_has_qn(path, "proj", "proj.new"));

    cbm_gbuf_t *new_gb = cbm_gbuf_new("proj", "/tmp/repo");
    ASSERT_NOT_NULL(new_gb);
    cbm_gbuf_upsert_node(new_gb, "Function", "new", "proj.new", "new.c", 1, 2, "{}");

    cbm_setenv(fail_env, "1", 1);
    int dump_rc = cbm_gbuf_dump_to_sqlite(new_gb, path);

    if (had_fail) {
        cbm_setenv(fail_env, saved_fail, 1);
    } else {
        cbm_unsetenv(fail_env);
    }

    ASSERT_NEQ(dump_rc, 0);
    ASSERT(gbuf_store_has_qn(path, "proj", "proj.old"));
    ASSERT(!gbuf_store_has_qn(path, "proj", "proj.new"));

    cbm_gbuf_free(new_gb);
    cbm_unlink(path);
    PASS();
}

SUITE(graph_buffer) {
    /* Original tests */
    RUN_TEST(gbuf_create_free);
    RUN_TEST(gbuf_free_null);
    RUN_TEST(gbuf_upsert_node);
    RUN_TEST(gbuf_upsert_updates);
    RUN_TEST(gbuf_find_by_id);
    RUN_TEST(gbuf_find_by_label);
    RUN_TEST(gbuf_upsert_reindexes_label_and_name);
    RUN_TEST(gbuf_find_by_name);
    RUN_TEST(gbuf_delete_by_label);
    RUN_TEST(gbuf_insert_edge);
    RUN_TEST(gbuf_edge_dedup);
    RUN_TEST(gbuf_imports_multi_symbol_dedup);
    RUN_TEST(gbuf_imports_long_local_name_no_collision);
    RUN_TEST(gbuf_find_edges_by_source_type);
    RUN_TEST(gbuf_find_edges_by_target_type);
    RUN_TEST(gbuf_find_edges_by_type);
    RUN_TEST(gbuf_delete_edges_by_type);
    RUN_TEST(gbuf_edge_count_by_type);
    RUN_TEST(gbuf_delete_edges_by_type_matching_props);
    RUN_TEST(gbuf_dump_empty);
    RUN_TEST(gbuf_flush_to_store);
    RUN_TEST(gbuf_many_nodes);

    /* Node edge cases */
    RUN_TEST(gbuf_upsert_null_qn);
    RUN_TEST(gbuf_upsert_empty_qn);
    RUN_TEST(gbuf_upsert_same_qn_updates_all_fields);
    RUN_TEST(gbuf_route_upsert_file_path_is_deterministic);
    RUN_TEST(gbuf_section_upsert_file_path_is_deterministic);
    RUN_TEST(gbuf_upsert_definition_source_prefers_richer_span);
    RUN_TEST(gbuf_upsert_module_source_prefers_richer_span);
    RUN_TEST(gbuf_upsert_long_qn);
    RUN_TEST(gbuf_find_by_qn_missing);
    RUN_TEST(gbuf_find_by_id_missing);
    RUN_TEST(gbuf_find_by_label_no_matches);
    RUN_TEST(gbuf_find_by_name_multiple);
    RUN_TEST(gbuf_delete_by_label_cascades_edges);
    RUN_TEST(gbuf_delete_by_paths_cascades_edges);
    RUN_TEST(gbuf_prune_orphan_folders_removes_nested_empty_context);
    RUN_TEST(gbuf_node_count_empty);
    RUN_TEST(gbuf_upsert_100_nodes_stress);

    /* Edge edge cases */
    RUN_TEST(gbuf_edge_nonexistent_endpoints);
    RUN_TEST(gbuf_validate_invariants_valid_graph);
    RUN_TEST(gbuf_dump_rejects_missing_edge_endpoint);
    RUN_TEST(gbuf_edge_dedup_merges_properties);
    RUN_TEST(gbuf_edge_count_empty);
    RUN_TEST(gbuf_edge_count_by_type_missing);
    RUN_TEST(gbuf_delete_edges_preserves_other_types);
    RUN_TEST(gbuf_find_edges_by_target_type_multiple);

    /* Merge tests */
    RUN_TEST(gbuf_merge_overlapping_qns);
    RUN_TEST(gbuf_merge_reindexes_label_and_name);
    RUN_TEST(gbuf_merge_route_file_path_is_deterministic);
    RUN_TEST(gbuf_merge_section_file_path_is_deterministic);
    RUN_TEST(gbuf_merge_definition_source_prefers_richer_span);
    RUN_TEST(gbuf_merge_module_source_prefers_richer_span);
    RUN_TEST(gbuf_merge_edge_dedup);
    RUN_TEST(gbuf_merge_empty_src_into_populated_dst);
    RUN_TEST(gbuf_merge_populated_src_into_empty_dst);
    RUN_TEST(gbuf_merge_null_args);

    /* Flush/merge-into-store tests */
    RUN_TEST(gbuf_flush_to_store_null);
    RUN_TEST(gbuf_flush_verify_store_data);
    RUN_TEST(gbuf_flush_begin_failure_preserves_existing_project);
    RUN_TEST(gbuf_merge_into_store_preserves);
    RUN_TEST(gbuf_flush_skips_orphan_edges);
    RUN_TEST(gbuf_flush_bulk_edges_preserves_buffer_properties);

    /* Shared ID tests */
    RUN_TEST(gbuf_shared_ids_unique);
    RUN_TEST(gbuf_shared_ids_null_fallback);
    RUN_TEST(gbuf_next_id_set_next_id_roundtrip);
    RUN_TEST(gbuf_next_id_null_safe);

    /* B1 pipeline-path isolation (#23) */
    RUN_TEST(gbuf_dump_pipeline_path_integrity);
    /* Relative root_path retain regression (#57 self-index finding) */
    RUN_TEST(gbuf_dump_relative_root_path_retained);
    RUN_TEST(gbuf_dump_failure_before_replace_keeps_existing_db);
}
