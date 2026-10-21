/*
 * test_java_typed_calls.c — Java CALLS edges follow the receiver's type
 * through the whole pipeline (definitions → cross-file LSP → calls pass).
 *
 * One small project is indexed with cbm_pipeline_run and the CALLS edges of
 * InvoiceService are read back from the store. Each project method carries a
 * same-named decoy in an unrelated class (Filters), so a name-only guess is
 * visible as an edge to Filters. Covered:
 *   - JDK and library receivers (Stream, List, org.jooq.DSLContext), whose
 *     calls get no edge instead of a project method of the same name;
 *   - a project receiver, whose call keeps its edge, also when its type is
 *     spelled with its package;
 *   - a receiver whose type is missing from the graph but whose package is
 *     the project's (a generated class that was not indexed, spelled with
 *     its package): unknown, not external, so its call keeps the name
 *     fallback;
 *   - a project type in the caller's own package that shares its simple
 *     name with a java.lang type (Process), or such a type declared in the
 *     caller's own file (Record): the own package wins.
 * The same fixture runs on the sequential path and, padded past
 * MIN_FILES_FOR_PARALLEL, on the parallel one: both must agree.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "pipeline/pipeline.h"
#include "sqlite3.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Fixture ─────────────────────────────────────────────────────── */

static const char *const JT_CUSTOMER = "package com.acme.model;\n\n"
                                       "public class Customer {\n"
                                       "    public String getName() { return \"\"; }\n"
                                       "}\n";

/* Every method here shares a name with a real target elsewhere. */
static const char *const JT_FILTERS = "package com.acme.other;\n\n"
                                      "public class Filters {\n"
                                      "    public Filters filter(Object p) { return this; }\n"
                                      "    public Filters map(Object f) { return this; }\n"
                                      "    public String getName() { return \"\"; }\n"
                                      "    public Object select(Object a) { return a; }\n"
                                      "    public Object build() { return null; }\n"
                                      "    public String getCity() { return \"\"; }\n"
                                      "    public int size() { return 0; }\n"
                                      "    public int start() { return 0; }\n"
                                      "    public Filters number(String n) { return this; }\n"
                                      "    public void setPaid(boolean p) {}\n"
                                      "    public Object getCustomer() { return null; }\n"
                                      "    public String label() { return \"\"; }\n"
                                      "}\n";

/* Process and Record share their simple names with java.lang types: the own
 * package and the file's own types win (JLS 6.4.1), so these are the types
 * InvoiceService and Process.start mean. */
static const char *const JT_PROCESS = "package com.acme.svc;\n\n"
                                      "public class Process {\n"
                                      "    public Process(String name) {}\n"
                                      "    public int start() { return new Record().id(); }\n"
                                      "}\n\n"
                                      "class Record {\n"
                                      "    int id() { return 0; }\n"
                                      "}\n";

static const char *const JT_SERVICE =
    "package com.acme.svc;\n\n"
    "import com.acme.model.Customer;\n"
    "import java.util.List;\n"
    "import java.util.stream.Collectors;\n"
    "import org.jooq.DSLContext;\n\n"
    "public class InvoiceService {\n"
    "    private final DSLContext dsl;\n\n"
    "    public InvoiceService(DSLContext dsl) {\n"
    "        this.dsl = dsl;\n"
    "    }\n\n"
    "    public List<String> numbers(List<String> names) {\n"
    "        return names.stream().filter(n -> n.isEmpty()).map(n -> n.trim())\n"
    "                .collect(Collectors.toList());\n"
    "    }\n\n"
    "    public int touch(List<String> names) {\n"
    "        dsl.select(\"a\");\n"
    "        return names.size();\n"
    "    }\n\n"
    "    public String customerName(Customer c) {\n"
    "        return c.getName();\n"
    "    }\n\n"
    "    public int run() {\n"
    "        Process p = new Process(\"job\");\n"
    "        return p.start();\n"
    "    }\n\n"
    "    public String qualifiedName(com.acme.other.Filters f) {\n"
    "        return f.label();\n"
    "    }\n\n"
    "    public String label() {\n"
    "        return \"\";\n"
    "    }\n\n"
    "    public String branchCity(com.acme.model.Branch b) {\n"
    "        return b.getCity();\n"
    "    }\n"
    "}\n";

static void jt_write_fixture(const char *tmp, int filler) {
    th_write_file(TH_PATH(tmp, "src/main/java/com/acme/model/Customer.java"), JT_CUSTOMER);
    th_write_file(TH_PATH(tmp, "src/main/java/com/acme/other/Filters.java"), JT_FILTERS);
    th_write_file(TH_PATH(tmp, "src/main/java/com/acme/svc/InvoiceService.java"), JT_SERVICE);
    th_write_file(TH_PATH(tmp, "src/main/java/com/acme/svc/Process.java"), JT_PROCESS);
    for (int i = 0; i < filler; i++) {
        char rel[128];
        char body[256];
        snprintf(rel, sizeof(rel), "src/main/java/com/acme/fill/Fill%d.java", i);
        snprintf(body, sizeof(body),
                 "package com.acme.fill;\n\npublic class Fill%d {\n"
                 "    public int value%d() { return %d; }\n}\n",
                 i, i, i);
        th_write_file(TH_PATH(tmp, rel), body);
    }
}

/* ── Store reads ─────────────────────────────────────────────────── */

typedef struct {
    char tmp[256];
    char db_path[512];
    char project[512];
    sqlite3 *db;
} jt_index_t;

static int jt_index(jt_index_t *ix, int filler) {
    memset(ix, 0, sizeof(*ix));
    snprintf(ix->tmp, sizeof(ix->tmp), "/tmp/cbm_jtyped_XXXXXX");
    if (!cbm_mkdtemp(ix->tmp)) {
        return -1;
    }
    jt_write_fixture(ix->tmp, filler);
    snprintf(ix->db_path, sizeof(ix->db_path), "%s/jt.db", ix->tmp);
    cbm_pipeline_t *p = cbm_pipeline_new(ix->tmp, ix->db_path, CBM_MODE_FULL);
    if (!p) {
        return -1;
    }
    int rc = cbm_pipeline_run(p);
    snprintf(ix->project, sizeof(ix->project), "%s", cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    if (rc != 0 || sqlite3_open_v2(ix->db_path, &ix->db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        return -1;
    }
    return 0;
}

static void jt_close(jt_index_t *ix) {
    if (ix->db) {
        sqlite3_close(ix->db);
    }
    th_cleanup(ix->tmp);
}

/* Targets of the CALLS edges from method `caller` whose textual callee ends
 * in `.leaf` or is `leaf`, as "<qn-without-project>|<strategy>" lines. */
static int jt_targets(jt_index_t *ix, const char *caller, const char *leaf, char *out,
                      size_t out_len) {
    out[0] = '\0';
    const char *sql = "SELECT t.qualified_name, json_extract(e.properties,'$.strategy'), "
                      "json_extract(e.properties,'$.callee') FROM edges e "
                      "JOIN nodes s ON s.id = e.source_id JOIN nodes t ON t.id = e.target_id "
                      "WHERE e.type = 'CALLS' AND s.name = ?1";
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(ix->db, sql, -1, &st, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(st, 1, caller, -1, SQLITE_STATIC);
    int n = 0;
    size_t plen = strlen(ix->project);
    size_t llen = strlen(leaf);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *qn = (const char *)sqlite3_column_text(st, 0);
        const char *strategy = (const char *)sqlite3_column_text(st, 1);
        const char *callee = (const char *)sqlite3_column_text(st, 2);
        if (!qn || !callee) {
            continue;
        }
        size_t clen = strlen(callee);
        bool leaf_match = (clen == llen && strcmp(callee, leaf) == 0) ||
                          (clen > llen && callee[clen - llen - 1] == '.' &&
                           strcmp(callee + clen - llen, leaf) == 0);
        if (!leaf_match) {
            continue;
        }
        if (strncmp(qn, ix->project, plen) == 0 && qn[plen] == '.') {
            qn += plen + 1;
        }
        size_t used = strlen(out);
        snprintf(out + used, out_len - used, "%s|%s\n", qn, strategy ? strategy : "");
        n++;
    }
    sqlite3_finalize(st);
    return n;
}

/* Exactly one CALLS edge for (caller, leaf), and its target QN ends in
 * `want_suffix`. Prints what the graph has on failure. */
static int jt_expect(jt_index_t *ix, const char *caller, const char *leaf,
                     const char *want_suffix) {
    char got[4096];
    int n = jt_targets(ix, caller, leaf, got, sizeof(got));
    if (n == 1) {
        const char *bar = strchr(got, '|');
        size_t qlen = bar ? (size_t)(bar - got) : strlen(got);
        size_t wlen = strlen(want_suffix);
        if (qlen >= wlen && strncmp(got + qlen - wlen, want_suffix, wlen) == 0) {
            return 1;
        }
    }
    printf("\n  %s -> %s: want one edge to ...%s, have %d:\n%s", caller, leaf, want_suffix, n, got);
    return 0;
}

/* No CALLS edge for (caller, leaf) reaches a node whose QN contains `bad`. */
static int jt_expect_none_to(jt_index_t *ix, const char *caller, const char *leaf,
                             const char *bad) {
    char got[4096];
    (void)jt_targets(ix, caller, leaf, got, sizeof(got));
    if (!strstr(got, bad)) {
        return 1;
    }
    printf("\n  %s -> %s: an edge reaches %s:\n%s", caller, leaf, bad, got);
    return 0;
}

/* At least one CALLS edge for (caller, leaf). */
static int jt_expect_some_edge(jt_index_t *ix, const char *caller, const char *leaf) {
    char got[4096];
    if (jt_targets(ix, caller, leaf, got, sizeof(got)) > 0) {
        return 1;
    }
    printf("\n  %s -> %s: want an edge, have none\n", caller, leaf);
    return 0;
}

/* No CALLS edge at all for (caller, leaf). */
static int jt_expect_no_edge(jt_index_t *ix, const char *caller, const char *leaf) {
    char got[4096];
    int n = jt_targets(ix, caller, leaf, got, sizeof(got));
    if (n == 0) {
        return 1;
    }
    printf("\n  %s -> %s: want no edge, have %d:\n%s", caller, leaf, n, got);
    return 0;
}

/* The CALLS edges out of the svc package, one sorted line each, with the
 * project prefix dropped so two indexes compare. The Java resolver's
 * strategies are kept; the textual registry's are one label, as its names
 * already differ between the two paths before this resolver runs (a
 * constructor is import_map on one and unique_name on the other). */
static char *jt_edge_set(jt_index_t *ix) {
    const char *sql =
        "SELECT group_concat(x, char(10)) FROM (SELECT s.name || '|' || "
        "coalesce(json_extract(e.properties,'$.callee'),'') || '|' || "
        "substr(t.qualified_name, length(?1) + 2) || '|' || "
        "CASE WHEN json_extract(e.properties,'$.strategy') LIKE 'lsp%' "
        "THEN json_extract(e.properties,'$.strategy') ELSE 'registry' END AS x FROM edges e "
        "JOIN nodes s ON s.id = e.source_id JOIN nodes t ON t.id = e.target_id "
        "WHERE e.type = 'CALLS' AND s.file_path LIKE '%/svc/%' ORDER BY x)";
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(ix->db, sql, -1, &st, NULL) != SQLITE_OK) {
        return NULL;
    }
    sqlite3_bind_text(st, 1, ix->project, -1, SQLITE_STATIC);
    char *out = NULL;
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0)) {
        out = strdup((const char *)sqlite3_column_text(st, 0));
    }
    sqlite3_finalize(st);
    return out;
}

/* ── Tests ───────────────────────────────────────────────────────── */

static int jt_check_external(jt_index_t *ix) {
    int ok = 1;
    /* JDK and library receivers never bind a project method by name... */
    ok &= jt_expect_none_to(ix, "numbers", "filter", "Filters");
    ok &= jt_expect_none_to(ix, "numbers", "map", "Filters");
    ok &= jt_expect_none_to(ix, "touch", "select", "Filters");
    ok &= jt_expect_none_to(ix, "touch", "size", "Filters");
    /* ...and get no edge: the graph holds no node for a library method. */
    ok &= jt_expect_no_edge(ix, "numbers", "filter");
    ok &= jt_expect_no_edge(ix, "touch", "select");
    ok &= jt_expect_no_edge(ix, "touch", "size");
    /* A project receiver keeps its edge. */
    ok &= jt_expect(ix, "customerName", "getName", "com.acme.model.Customer.getName");
    /* A project type spelled with its package and not imported binds by its
     * type, where the name guess picks the caller's own label(). */
    ok &= jt_expect(ix, "qualifiedName", "label", "com.acme.other.Filters.label");
    /* An unknown type in a project package is not proven external. */
    ok &= jt_expect_some_edge(ix, "branchCity", "getCity");
    /* A project type named like a java.lang one, in the caller's package. */
    /* `new` on a project type binds its class node, as for any project type. */
    ok &= jt_expect(ix, "run", "Process", "com.acme.svc.Process");
    ok &= jt_expect(ix, "run", "start", "com.acme.svc.Process.start");
    ok &= jt_expect(ix, "start", "id", "Record.id");
    return ok;
}

static int jt_run_all(int filler) {
    jt_index_t ix;
    if (jt_index(&ix, filler) != 0) {
        printf("\n  indexing the fixture failed\n");
        jt_close(&ix);
        return 0;
    }
    int ok = jt_check_external(&ix);
    jt_close(&ix);
    return ok;
}

TEST(java_typed_calls_sequential) {
    ASSERT_TRUE(jt_run_all(0));
    PASS();
}

TEST(java_typed_calls_parallel) {
    ASSERT_TRUE(jt_run_all(60));
    PASS();
}

/* The sequential and the parallel path produce the same svc edges. */
TEST(java_typed_calls_paths_agree) {
    jt_index_t seq;
    jt_index_t par;
    int seq_rc = jt_index(&seq, 0);
    int par_rc = jt_index(&par, 60);
    char *a = seq_rc == 0 ? jt_edge_set(&seq) : NULL;
    char *b = par_rc == 0 ? jt_edge_set(&par) : NULL;
    int same = a && b && strcmp(a, b) == 0;
    if (!same) {
        printf("\n  sequential:\n%s\n  parallel:\n%s\n", a ? a : "(none)", b ? b : "(none)");
    }
    free(a);
    free(b);
    jt_close(&seq);
    jt_close(&par);
    ASSERT_TRUE(same);
    PASS();
}

SUITE(java_typed_calls) {
    RUN_TEST(java_typed_calls_sequential);
    RUN_TEST(java_typed_calls_parallel);
    RUN_TEST(java_typed_calls_paths_agree);
}
