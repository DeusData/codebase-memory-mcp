/*
 * test_site_bind.c -- the site hook of the shared cross-file layer
 * (src/pipeline/site_bind.h).
 *
 * A file of a language without a row is inactive: every call and usage site
 * of it gets NO_ANSWER (main's name chain decides, unchanged) and the end
 * step draws no edge. Go has an engine and never gets a row of this layer,
 * so these hold as rows for other languages arrive.
 *
 * Protobuf has a row: a type reference names the definition the scope facts
 * prove it names (protoc's scope walk over the file's imports), on both
 * resolver routes and incrementally as in a full index. The expected targets
 * were decided by protoc 3.21.12 (descriptor set of the fixture, see
 * resolver-evidence/.../shared/scope.md, A3b).
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "../src/pipeline/site_bind.h"
#include "../src/graph_buffer/graph_buffer.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "sqlite3.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

TEST(site_bind_language_without_row_answers_nothing) {
    cbm_gbuf_t *gb = cbm_gbuf_new("p", "/tmp/p");
    ASSERT_NOT_NULL(gb);
    int64_t caller = cbm_gbuf_upsert_node(gb, "Function", "f", "p.a.f", "a.go", 1, 3, "{}");
    int64_t callee = cbm_gbuf_upsert_node(gb, "Function", "g", "p.b.g", "b.go", 1, 3, "{}");
    ASSERT_GT(caller, 0);
    ASSERT_GT(callee, 0);
    const cbm_gbuf_node_t *source = cbm_gbuf_find_by_qn(gb, "p.a.f");
    ASSERT_NOT_NULL(source);

    CBMCall calls[1];
    memset(calls, 0, sizeof(calls));
    calls[0].callee_name = "g";
    calls[0].enclosing_func_qn = "p.a.f";
    CBMUsage usages[1];
    memset(usages, 0, sizeof(usages));
    usages[0].ref_name = "g";
    usages[0].enclosing_func_qn = "p.a.f";
    CBMFileResult result;
    memset(&result, 0, sizeof(result));
    result.calls.items = calls;
    result.calls.count = 1;
    result.usages.items = usages;
    result.usages.count = 1;

    cbm_site_memo_t *memo = NULL;
    cbm_site_file_t sf;
    cbm_pipeline_site_bind_begin(&sf, NULL, gb, "a.go", CBM_LANG_GO, &result, &memo, NULL);
    ASSERT_FALSE(sf.active);

    cbm_site_answer_t at_call = cbm_pipeline_site_bind_call(&sf, &calls[0], source);
    ASSERT_EQ(at_call.verdict, CBM_SITE_NO_ANSWER);
    ASSERT_NULL(at_call.res.qualified_name);
    ASSERT_NULL(at_call.target);

    cbm_site_answer_t at_usage = cbm_pipeline_site_bind_usage(&sf, &usages[0], source);
    ASSERT_EQ(at_usage.verdict, CBM_SITE_NO_ANSWER);
    ASSERT_NULL(at_usage.res.qualified_name);
    ASSERT_NULL(at_usage.target);

    int edges_before = cbm_gbuf_edge_count(gb);
    cbm_pipeline_site_bind_end(&sf, gb);
    ASSERT_EQ(cbm_gbuf_edge_count(gb), edges_before);
    ASSERT_NULL(memo); /* nothing was looked up */

    cbm_gbuf_free(gb);
    PASS();
}

/* ── Indexing helpers ────────────────────────────────────────────────── */

/* Index `repo` into `db` (a second call on the same database takes the
 * incremental route) with `workers` workers (0: leave CBM_WORKERS as it is);
 * `marks` (test seam, NULL-terminated name/target-suffix/verdict triples)
 * are set once the project name is known. */
typedef struct {
    const char *name;
    const char *rel;    /* the target's file */
    const char *symbol; /* the target's name in it */
    cbm_site_verdict_t verdict;
} sb_mark_t;

static int sb_index(const char *repo, const char *db, int workers, const sb_mark_t *marks) {
    const char *saved = getenv("CBM_WORKERS");
    char *saved_copy = saved ? strdup(saved) : NULL;
    if (workers > 0) {
        char w[8];
        snprintf(w, sizeof(w), "%d", workers);
        cbm_setenv("CBM_WORKERS", w, 1);
    }
    int rc = -1;
    cbm_pipeline_t *p = cbm_pipeline_new(repo, db, CBM_MODE_FULL);
    if (p) {
        for (int i = 0; marks && marks[i].name; i++) {
            char *qn = cbm_pipeline_fqn_compute(cbm_pipeline_project_name(p), marks[i].rel,
                                                marks[i].symbol);
            cbm_pipeline_site_bind_test_mark(marks[i].name, qn, marks[i].verdict);
            free(qn);
        }
        rc = cbm_pipeline_run(p);
        cbm_pipeline_site_bind_test_mark(NULL, NULL, CBM_SITE_NO_ANSWER);
        cbm_pipeline_free(p);
    }
    if (saved_copy) {
        cbm_setenv("CBM_WORKERS", saved_copy, 1);
        free(saved_copy);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }
    return rc;
}

static void sb_unlink_db(const char *db) {
    char side[600];
    unlink(db);
    snprintf(side, sizeof(side), "%s-wal", db);
    unlink(side);
    snprintf(side, sizeof(side), "%s-shm", db);
    unlink(side);
}

/* The edges of the given types as "type source -> target [strategy]" lines,
 * sorted: what two indexes of one tree must agree on (D1: property text left
 * out). The caller frees it; NULL when the database cannot be read. */
static char *sb_state(const char *db) {
    sqlite3 *h = NULL;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(h);
        return NULL;
    }
    size_t cap = 4096;
    size_t len = 0;
    char *buf = malloc(cap);
    buf[0] = '\0';
    sqlite3_stmt *st = NULL;
    const char *sql =
        "SELECT e.type || ' ' || s.qualified_name || ' -> ' || t.qualified_name || ' [' || "
        "coalesce(json_extract(e.properties, '$.strategy'), '') || ']' FROM edges e "
        "JOIN nodes s ON s.id = e.source_id JOIN nodes t ON t.id = e.target_id "
        "WHERE e.type IN ('USAGE', 'IMPORTS', 'CALLS', 'HTTP_CALLS', 'ASYNC_CALLS', "
        "'CALL_REFERENCE', 'SPAWNS') ORDER BY 1";
    if (sqlite3_prepare_v2(h, sql, -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *line = (const char *)sqlite3_column_text(st, 0);
            size_t l = strlen(line);
            if (len + l + 2 > cap) {
                cap = (len + l + 2) * 2;
                buf = realloc(buf, cap);
            }
            memcpy(buf + len, line, l);
            len += l;
            buf[len++] = '\n';
            buf[len] = '\0';
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(h);
    return buf;
}

/* The strategy of the `type` edge from the node whose QN ends in `.<src>`
 * to the node whose QN ends in `.<tgt>` ("" when it has none) into out;
 * returns how many such edges there are. */
static int sb_edge(const char *db, const char *type, const char *src, const char *tgt, char *out,
                   size_t cap) {
    out[0] = '\0';
    sqlite3 *h = NULL;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(h);
        return -1;
    }
    sqlite3_stmt *st = NULL;
    int n = 0;
    const char *sql = "SELECT coalesce(json_extract(e.properties, '$.strategy'), '') FROM edges e "
                      "JOIN nodes s ON s.id = e.source_id JOIN nodes t ON t.id = e.target_id "
                      "WHERE e.type = ?1 AND s.qualified_name LIKE '%.' || ?2 "
                      "AND t.qualified_name LIKE '%.' || ?3";
    if (sqlite3_prepare_v2(h, sql, -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, type, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, src, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 3, tgt, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            n++;
            snprintf(out, cap, "%s", (const char *)sqlite3_column_text(st, 0));
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(h);
    return n;
}

/* One edge with this strategy ("*": any; want NULL: no such edge at all). */
static int sb_expect(const char *db, const char *venue, const char *type, const char *src,
                     const char *tgt, const char *want) {
    char got[64];
    int n = sb_edge(db, type, src, tgt, got, sizeof(got));
    if (!want && n == 0) {
        return 1;
    }
    if (want && n == 1 && (strcmp(want, "*") == 0 || strcmp(got, want) == 0)) {
        return 1;
    }
    printf("\n  [%s] %s %s -> %s: want %s%s%s, have %d edge(s), strategy '%s'\n", venue, type, src,
           tgt, want ? "one edge, strategy '" : "no edge", want ? want : "", want ? "'" : "", n,
           got);
    return 0;
}

/* ── The Protobuf fixture ────────────────────────────────────────────── */

static const char *const SB_PROTO[][2] = {
    {"x/common.proto", "syntax = \"proto3\";\npackage common;\n\n"
                       "message Status { int32 code = 1; }\nmessage Only { int32 v = 1; }\n"},
    {"y/twin.proto", "syntax = \"proto3\";\npackage y;\n\nmessage Status { string y = 1; }\n"},
    {"x/shared.proto",
     "syntax = \"proto3\";\npackage shared;\n\nimport public \"x/common.proto\";\n\n"
     "message Shared { int32 s = 1; }\n"},
    /* two imported Status messages: the qualifier decides (protoc: y.Status,
     * common.Status, common.Only); main's name chain gives both the same */
    {"u/user.proto", "syntax = \"proto3\";\npackage u;\n\nimport \"x/common.proto\";\n"
                     "import \"y/twin.proto\";\n\nmessage User {\n  y.Status st = 1;\n"
                     "  common.Status cs = 2;\n  common.Only only = 3;\n}\n"},
    /* common.Status through shared's `import public` */
    {"u/via.proto", "syntax = \"proto3\";\npackage u;\n\nimport \"x/shared.proto\";\n\n"
                    "message Via {\n  common.Status st = 1;\n  shared.Shared sh = 2;\n}\n"},
    /* nested messages; rpc request and response */
    {"n/nested.proto", "syntax = \"proto3\";\npackage n.m;\n\nmessage L1 {\n  message L2 {\n"
                       "    message L3 { int32 v = 1; }\n  }\n  L2.L3 deep = 1;\n}\n\n"
                       "service S { rpc Get(L1) returns (L1.L2.L3); }\n"},
    /* an import outside the repository: one candidate, not proven (main's
     * chain names the same target: nothing added) */
    {"p/outside.proto",
     "syntax = \"proto3\";\npackage p;\n\nimport \"google/protobuf/empty.proto\";\n"
     "import \"y/twin.proto\";\n\nmessage Outside {\n  y.Status st = 1;\n"
     "  google.protobuf.Empty e = 2;\n}\n"},
    /* the same, with common imported too: main's chain names common.Status,
     * the possible answer y.Status; main's edge stays alone */
    {"p/outside2.proto", "syntax = \"proto3\";\npackage p;\n\n"
                         "import \"google/protobuf/empty.proto\";\nimport \"x/common.proto\";\n"
                         "import \"y/twin.proto\";\n\nmessage Outside2 {\n  y.Status st = 1;\n"
                         "  google.protobuf.Empty e = 2;\n}\n"},
};

static int sb_write_proto(const char *dir, int pad) {
    for (size_t i = 0; i < sizeof(SB_PROTO) / sizeof(SB_PROTO[0]); i++) {
        if (th_write_file(TH_PATH(dir, SB_PROTO[i][0]), SB_PROTO[i][1]) != 0) {
            return -1;
        }
    }
    for (int i = 0; i < pad; i++) { /* past MIN_FILES_FOR_PARALLEL (50) */
        char rel[64];
        char body[128];
        snprintf(rel, sizeof(rel), "pad/p%02d.proto", i);
        snprintf(body, sizeof(body),
                 "syntax = \"proto3\";\npackage pad;\n\nmessage P%02d { int32 v = 1; }\n", i);
        if (th_write_file(TH_PATH(dir, rel), body) != 0) {
            return -1;
        }
    }
    return 0;
}

static int sb_check_proto(const char *db, const char *venue) {
    int ok = 1;
    ok &= sb_expect(db, venue, "USAGE", "u.user", "y.twin.Status", "include_scope");
    ok &= sb_expect(db, venue, "USAGE", "u.user", "x.common.Status", "include_scope");
    ok &= sb_expect(db, venue, "USAGE", "u.user", "x.common.Only", "include_scope");
    ok &= sb_expect(db, venue, "USAGE", "u.via", "x.common.Status", "include_scope");
    ok &= sb_expect(db, venue, "USAGE", "u.via", "x.shared.Shared", "include_scope");
    ok &= sb_expect(db, venue, "USAGE", "n.nested.Get", "n.nested.L1", "include_scope");
    ok &= sb_expect(db, venue, "USAGE", "n.nested.Get", "n.nested.L3", "include_scope");
    ok &= sb_expect(db, venue, "USAGE", "p.outside", "y.twin.Status", "");
    ok &= sb_expect(db, venue, "USAGE", "p.outside2", "x.common.Status", "");
    ok &= sb_expect(db, venue, "USAGE", "p.outside2", "y.twin.Status", NULL);
    return ok;
}

/* A type reference binds the definition its qualifier names, on both
 * routes, and the routes agree edge for edge. */
TEST(site_bind_protobuf_reference_binds_what_protoc_resolves) {
    char *tmp = th_mktempdir("sb_proto");
    ASSERT_NOT_NULL(tmp);
    char dir[512];
    snprintf(dir, sizeof(dir), "%s/repo", tmp);
    ASSERT_EQ(sb_write_proto(dir, 55), 0);
    char seq_db[512];
    char par_db[512];
    snprintf(seq_db, sizeof(seq_db), "%s/seq.db", tmp);
    snprintf(par_db, sizeof(par_db), "%s/par.db", tmp);
    ASSERT_EQ(sb_index(dir, seq_db, 1, NULL), 0);
    ASSERT_EQ(sb_index(dir, par_db, 4, NULL), 0);
    int ok = sb_check_proto(seq_db, "sequential") & sb_check_proto(par_db, "parallel");
    char *seq = sb_state(seq_db);
    char *par = sb_state(par_db);
    bool same = seq && par && strcmp(seq, par) == 0;
    if (!same) {
        printf("\n  parallel != sequential\n--- parallel\n%s--- sequential\n%s", par ? par : "",
               seq ? seq : "");
    }
    free(seq);
    free(par);
    th_cleanup(tmp);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(same);
    PASS();
}

/* ── Incremental equals full ─────────────────────────────────────────── */

/* After the caller edited the tree: index incrementally into `inc_db`, then
 * fully into a fresh `full_db`; the edges must be the same and the route the
 * expected one. Returns 0, or -1 after printing what differs. */
static int sb_step(const char *repo, const char *inc_db, const char *full_db, const char *what,
                   cbm_incremental_route_t want_route, const char *want_decline) {
    cbm_pipeline_incremental_test_reset_faults();
    if (sb_index(repo, inc_db, 0, NULL) != 0) {
        printf("  %s: incremental index failed\n", what);
        return -1;
    }
    cbm_incremental_route_t route = cbm_pipeline_incremental_test_last_route();
    cbm_incremental_closure_plan_t plan = cbm_pipeline_incremental_test_last_closure_plan();
    sb_unlink_db(full_db);
    if (sb_index(repo, full_db, 0, NULL) != 0) {
        printf("  %s: full index failed\n", what);
        return -1;
    }
    char *inc = sb_state(inc_db);
    char *full = sb_state(full_db);
    int rc = 0;
    if (!inc || !full || strcmp(inc, full) != 0) {
        printf("  %s: incremental != full\n--- incremental (route %d)\n%s--- full\n%s", what,
               (int)route, inc ? inc : "(null)", full ? full : "(null)");
        rc = -1;
    } else if (route != want_route) {
        printf("  %s: route %d (decline %s), expected %d\n", what, (int)route,
               plan.decline ? plan.decline : "-", (int)want_route);
        rc = -1;
    } else if (want_decline && (!plan.decline || strcmp(plan.decline, want_decline) != 0)) {
        printf("  %s: decline %s, expected %s\n", what, plan.decline ? plan.decline : "(none)",
               want_decline);
        rc = -1;
    }
    free(inc);
    free(full);
    return rc;
}

/* Remove the stored scope facts of one file (a row an older binary wrote). */
static int sb_strip_facts(const char *db, const char *rel) {
    sqlite3 *h = NULL;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READWRITE, NULL) != SQLITE_OK) {
        sqlite3_close(h);
        return -1;
    }
    sqlite3_stmt *st = NULL;
    int rc = -1;
    if (sqlite3_prepare_v2(h,
                           "UPDATE lsp_surface SET defs_json = json_remove(defs_json, '$.is') "
                           "WHERE rel_path = ?1",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, rel, -1, SQLITE_TRANSIENT);
        rc = sqlite3_step(st) == SQLITE_DONE && sqlite3_changes(h) == 1 ? 0 : -1;
    }
    sqlite3_finalize(st);
    sqlite3_close(h);
    return rc;
}

/* An import added or removed repairs incrementally to what a full index
 * draws; a stored row without scope facts declines to a full index. */
TEST(site_bind_protobuf_incremental_equals_full) {
    char *tmp = th_mktempdir("sb_proto_incr");
    ASSERT_NOT_NULL(tmp);
    char dir[512];
    snprintf(dir, sizeof(dir), "%s/repo", tmp);
    ASSERT_EQ(sb_write_proto(dir, 0), 0);
    char inc_db[512];
    char full_db[512];
    snprintf(inc_db, sizeof(inc_db), "%s/inc.db", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    ASSERT_EQ(sb_index(dir, inc_db, 0, NULL), 0);
    ASSERT_TRUE(sb_check_proto(inc_db, "full"));

    /* via imports twin and names y.Status */
    ASSERT_EQ(th_write_file(TH_PATH(dir, "u/via.proto"),
                            "syntax = \"proto3\";\npackage u;\n\nimport \"x/shared.proto\";\n"
                            "import \"y/twin.proto\";\n\nmessage Via {\n  common.Status st = 1;\n"
                            "  shared.Shared sh = 2;\n  y.Status ys = 3;\n}\n"),
              0);
    ASSERT_EQ(
        sb_step(dir, inc_db, full_db, "import added", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR, NULL),
        0);
    ASSERT_TRUE(
        sb_expect(inc_db, "import added", "USAGE", "u.via", "y.twin.Status", "include_scope"));

    /* user drops twin: its y.Status is no longer seen, common.Status is */
    ASSERT_EQ(
        th_write_file(TH_PATH(dir, "u/user.proto"),
                      "syntax = \"proto3\";\npackage u;\n\nimport \"x/common.proto\";\n\n"
                      "message User {\n  common.Status cs = 2;\n  common.Only only = 3;\n}\n"),
        0);
    ASSERT_EQ(
        sb_step(dir, inc_db, full_db, "import removed", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR, NULL),
        0);
    ASSERT_TRUE(sb_expect(inc_db, "import removed", "USAGE", "u.user", "y.twin.Status", NULL));
    ASSERT_TRUE(
        sb_expect(inc_db, "import removed", "USAGE", "u.user", "x.common.Status", "include_scope"));

    /* a re-exported file's surface changes (shared re-exports common): the
     * re-exporter's includers have no edge into it, so the repair declines */
    ASSERT_EQ(th_write_file(TH_PATH(dir, "x/common.proto"),
                            "syntax = \"proto3\";\npackage common;\n\n"
                            "message Status { int32 code = 1; }\n"),
              0);
    ASSERT_EQ(sb_step(dir, inc_db, full_db, "re-exported file changed",
                      CBM_INCREMENTAL_ROUTE_FORCED_FULL, "scope_reexport_changed"),
              0);

    /* a stored row without facts: the repair cannot read that file's scope */
    ASSERT_EQ(sb_strip_facts(inc_db, "x/common.proto"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(dir, "u/via.proto"),
                            "syntax = \"proto3\";\npackage u;\n\nimport \"x/shared.proto\";\n\n"
                            "message Via {\n  common.Status st = 1;\n  shared.Shared sh = 2;\n}\n"),
              0);
    ASSERT_EQ(sb_step(dir, inc_db, full_db, "stored row without facts",
                      CBM_INCREMENTAL_ROUTE_FORCED_FULL, "scope_facts_missing"),
              0);
    th_cleanup(tmp);
    PASS();
}

/* ── The guarded call shapes, through the test seam ──────────────────── */

/* Every mark has its own target, so an edge shows which site it came from. */
static const char *const SB_SEAM[][2] = {
    {"lib.lua", "function target_fn()\n  return 1\nend\n\nfunction other_fn()\n  return 2\nend\n\n"
                "function third_fn()\n  return 3\nend\n\nfunction svc_fn()\n  return 5\nend\n\n"
                "function swift_fn()\n  return 6\nend\n\nfunction php_fn()\n  return 7\nend\n\n"
                "function rust_fn()\n  return 8\nend\n\nfunction java_fn()\n  return 9\nend\n"},
    /* markd proven, markp possible where main draws nothing, markq possible
     * where main draws another target; markv a proven usage; a service call */
    {"app.lua", "function markq()\n  return 4\nend\n\nfunction run()\n  markd()\n  markp()\n"
                "  markq()\n  local v = markv\n  requests.get(\"http://svc/api/items\")\n"
                "  return v\nend\n"},
    /* Swift label candidates: one compatible overload, and none */
    {"s/Labels.swift", "func marks(a: Int) {}\nfunc marks(b: Int) {}\n\n"
                       "func use() {\n    marks(a: 1)\n    marks(c: 2)\n}\n"},
    /* a PHP call on a vendor class */
    {"php/Go.php", "<?php\nuse GuzzleHttp\\Client;\n\nfunction go() {\n    $c = new Client();\n"
                   "    $c->markh();\n}\n"},
    /* a Rust call the engine places on an external type */
    {"r/main.rs", "use std::path::Path;\n\nfn main() {\n    let p = Path::new(\"x\");\n"
                  "    p.markr();\n}\n"},
    /* a Java call on a JDK type */
    {"j/J.java", "import java.util.List;\n\nclass J {\n    void f(List<String> l) {\n"
                 "        l.markj();\n    }\n}\n"},
};

static int sb_write_seam(const char *dir) {
    for (size_t i = 0; i < sizeof(SB_SEAM) / sizeof(SB_SEAM[0]); i++) {
        if (th_write_file(TH_PATH(dir, SB_SEAM[i][0]), SB_SEAM[i][1]) != 0) {
            return -1;
        }
    }
    for (int i = 0; i < 55; i++) {
        char rel[64];
        char body[128];
        snprintf(rel, sizeof(rel), "pad/p%02d.lua", i);
        snprintf(body, sizeof(body), "function pad%02d()\n  return %d\nend\n", i, i);
        if (th_write_file(TH_PATH(dir, rel), body) != 0) {
            return -1;
        }
    }
    return 0;
}

static const sb_mark_t SB_MARKS[] = {
    {"markd", "lib.lua", "target_fn", CBM_SITE_PROVEN},
    {"markp", "lib.lua", "other_fn", CBM_SITE_POSSIBLE},
    {"markq", "lib.lua", "third_fn", CBM_SITE_POSSIBLE},
    {"markv", "lib.lua", "target_fn", CBM_SITE_PROVEN},
    {"requests.get", "lib.lua", "svc_fn", CBM_SITE_PROVEN},
    {"marks", "lib.lua", "swift_fn", CBM_SITE_PROVEN},
    {"markh", "lib.lua", "php_fn", CBM_SITE_PROVEN},
    {"markr", "lib.lua", "rust_fn", CBM_SITE_PROVEN},
    {"markj", "lib.lua", "java_fn", CBM_SITE_PROVEN},
    {NULL, NULL, NULL, CBM_SITE_NO_ANSWER},
};

static int sb_check_seam(const char *db, const char *venue) {
    int ok = 1;
    ok &= sb_expect(db, venue, "CALLS", "app.run", "lib.target_fn", "include_scope");
    ok &= sb_expect(db, venue, "CALLS", "app.run", "lib.other_fn", "include_scope_may");
    ok &= sb_expect(db, venue, "CALLS", "app.run", "lib.third_fn", NULL);
    ok &= sb_expect(db, venue, "CALLS", "app.run", "app.markq", "*");
    ok &= sb_expect(db, venue, "USAGE", "app.run", "lib.target_fn", "include_scope");
    /* the guards keep main's answer: a service call is classified as such, a
     * Swift label set, a vendor call and an engine-placed call bind nothing
     * of the layer */
    ok &= sb_expect(db, venue, "CALLS", "app.run", "lib.svc_fn", NULL);
    ok &= sb_expect(db, venue, "CALLS", "Labels.use", "lib.swift_fn", NULL);
    ok &= sb_expect(db, venue, "CALLS", "Go.go", "lib.php_fn", NULL);
    return ok;
}

/* A proven call answer is applied the same way on both routes at every
 * guarded shape (Swift label candidates, a vendor call, an engine-placed
 * external call, a service call); a possible one is drawn only where main
 * drew nothing at the site. */
TEST(site_bind_seam_answers_agree_on_both_routes) {
    char *tmp = th_mktempdir("sb_seam");
    ASSERT_NOT_NULL(tmp);
    char dir[512];
    snprintf(dir, sizeof(dir), "%s/repo", tmp);
    ASSERT_EQ(sb_write_seam(dir), 0);
    char seq_db[512];
    char par_db[512];
    snprintf(seq_db, sizeof(seq_db), "%s/seq.db", tmp);
    snprintf(par_db, sizeof(par_db), "%s/par.db", tmp);
    ASSERT_EQ(sb_index(dir, seq_db, 1, SB_MARKS), 0);
    ASSERT_EQ(sb_index(dir, par_db, 4, SB_MARKS), 0);
    int ok = sb_check_seam(seq_db, "sequential") & sb_check_seam(par_db, "parallel");
    char *seq = sb_state(seq_db);
    char *par = sb_state(par_db);
    bool same = seq && par && strcmp(seq, par) == 0;
    if (!same) {
        printf("\n  parallel != sequential\n--- parallel\n%s--- sequential\n%s", par ? par : "",
               seq ? seq : "");
    }
    free(seq);
    free(par);
    th_cleanup(tmp);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(same);
    PASS();
}

/* USAGE edges proven by the layer (strategy include_scope) into a node whose
 * QN ends in `.tgt`. */
static int sb_proven_to(const char *db, const char *tgt) {
    sqlite3 *h = NULL;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(h);
        return -1;
    }
    sqlite3_stmt *st = NULL;
    int n = -1;
    if (sqlite3_prepare_v2(h,
                           "SELECT count(*) FROM edges e JOIN nodes t ON t.id = e.target_id "
                           "WHERE e.type = 'USAGE' AND t.qualified_name LIKE '%.' || ?1 "
                           "AND json_extract(e.properties, '$.strategy') = 'include_scope'",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, tgt, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            n = sqlite3_column_int(st, 0);
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(h);
    return n;
}

/* One tree of the protoc fixtures (review 1, B2), indexed on one route; the
 * proven USAGE edges into `tgt`. */
static int sb_protoc_case(const char *const files[][2], int nfiles, const char *tgt) {
    char *tmp = th_mktempdir("sb_protoc");
    if (!tmp) {
        return -1;
    }
    char dir[512];
    snprintf(dir, sizeof(dir), "%s/repo", tmp);
    int rc = 0;
    for (int i = 0; i < nfiles && rc == 0; i++) {
        rc = th_write_file(TH_PATH(dir, files[i][0]), files[i][1]);
    }
    char db[512];
    snprintf(db, sizeof(db), "%s/p.db", tmp);
    int n = rc == 0 && sb_index(dir, db, 1, NULL) == 0 ? sb_proven_to(db, tgt) : -1;
    th_cleanup(tmp);
    return n;
}

/* One protoc case: the proven edges it should have; a miss is printed, so
 * every shape is reported. */
static int sb_protoc_expect(const char *shape, int got, int want) {
    if (got != want) {
        printf("\n  protoc case %s: %d proven edge(s), want %d\n", shape, got, want);
    }
    return got == want;
}

/* Proven never where protoc resolves nothing (protoc 3.21.12 on each tree,
 * review 1 fixtures, messages quoted): the first component decides the scope
 * with no backtracking; a MISSING token; an import cycle; one file under two
 * spellings; a path protoc refuses. The compiling partner stays proven. */
TEST(site_bind_protobuf_never_proven_where_protoc_rejects) {
    int ok = 1;
    /* "common.Status" is resolved to "x.common.Status", which is not defined */
    static const char *const prefix[][2] = {
        {"common.proto", "syntax = \"proto3\";\npackage common;\nmessage Status { int32 a = 1; }\n"},
        {"x/y.proto", "syntax = \"proto3\";\npackage x.common;\nimport \"common.proto\";\n"
                      "message M { common.Status s = 1; }\n"}};
    ok &= sb_protoc_expect("prefix", sb_protoc_case(prefix, 2, "common.Status"), 0);
    /* "b.C" is resolved to "a.b.C", which is not defined */
    static const char *const pk2[][2] = {
        {"c2.proto", "syntax = \"proto3\";\npackage b;\nmessage C { int32 a = 1; }\n"},
        {"pk2.proto", "syntax = \"proto3\";\npackage a.b;\nimport \"c2.proto\";\n"
                      "message M2 { b.C x = 1; }\n"}};
    ok &= sb_protoc_expect("pk2", sb_protoc_case(pk2, 2, "c2.C"), 0);
    /* compiles: b.D is .a.b.b.D (the innermost scope holding `b`) */
    static const char *const pk[][2] = {
        {"pk.proto", "syntax = \"proto3\";\npackage a.b;\nmessage b { message D {} }\n"
                     "message M { b.D x = 1; }\n"}};
    ok &= sb_protoc_expect("pk", sb_protoc_case(pk, 1, "pk.D"), 1); /* node pk.D: the file and its name */
    /* Expected ";" (a MISSING token after the field), Expected field number */
    static const char *const missing[][2] = {
        {"t.proto", "syntax = \"proto3\";\npackage t;\nmessage T { int32 a = 1; }\n"},
        {"u.proto", "syntax = \"proto3\";\npackage u;\nimport \"t.proto\";\n"
                    "message M { t.T x = 1 }\n"},
        {"w.proto", "syntax = \"proto3\";\npackage w;\nimport \"t.proto\";\n"
                    "message P { int32 y = ; t.T x = 1; }\n"}};
    ok &= sb_protoc_expect("missing", sb_protoc_case(missing, 3, "t.T"), 0);
    /* File recursively imports itself */
    static const char *const cycle[][2] = {
        {"k1.proto", "syntax = \"proto3\";\npackage k;\nimport public \"k2.proto\";\n"
                     "message K1 { int32 a = 1; }\n"},
        {"k2.proto", "syntax = \"proto3\";\npackage k;\nimport public \"k1.proto\";\n"
                     "message K2 { K1 a = 1; }\n"}};
    ok &= sb_protoc_expect("cycle", sb_protoc_case(cycle, 2, "k1.K1"), 0);
    /* "cc.T" is already defined in file "common/c.proto" (two spellings) */
    static const char *const spell[][2] = {
        {"proto/common/c.proto", "syntax = \"proto3\";\npackage cc;\nmessage T { int32 a = 1; }\n"},
        {"x.proto", "syntax = \"proto3\";\nimport public \"common/c.proto\";\n"},
        {"y.proto", "syntax = \"proto3\";\nimport public \"proto/common/c.proto\";\n"},
        {"z.proto", "syntax = \"proto3\";\nimport \"x.proto\";\nimport \"y.proto\";\n"
                    "message Z { cc.T t = 1; }\n"}};
    ok &= sb_protoc_expect("spell", sb_protoc_case(spell, 4, "c.T"), 0);
    /* Backslashes, consecutive slashes, ".", or ".." are not allowed in the
     * virtual path */
    static const char *const dot[][2] = {
        {"t.proto", "syntax = \"proto3\";\npackage t;\nmessage T { int32 a = 1; }\n"},
        {"dot.proto", "syntax = \"proto3\";\npackage dot;\nimport \"./t.proto\";\n"
                      "message D { t.T x = 1; }\n"}};
    ok &= sb_protoc_expect("dot", sb_protoc_case(dot, 2, "t.T"), 0);
    ASSERT_TRUE(ok);
    PASS();
}

/* protoc 3.21.12 on review 3's fx4/scope: A's field is .p.A.Type (the
 * enclosing message is searched first), B's is .p.Type. Both are proven to
 * those, and the outer Type never at A's site, where the inner one shadows
 * it. */
TEST(site_bind_protobuf_enclosing_message_decides_first) {
    static const char *const scope[][2] = {
        {"t.proto", "syntax = \"proto3\";\npackage p;\nenum Type { T0 = 0; }\n"},
        {"a.proto", "syntax = \"proto3\";\npackage p;\nimport \"t.proto\";\n"
                    "message A {\n  enum Type { A0 = 0; }\n  Type t = 1;\n}\n"
                    "message B {\n  Type t = 1;\n}\n"}};
    char *tmp = th_mktempdir("sb_scope");
    ASSERT_NOT_NULL(tmp);
    char dir[512];
    snprintf(dir, sizeof(dir), "%s/repo", tmp);
    int rc = 0;
    for (int i = 0; i < 2 && rc == 0; i++) {
        rc = th_write_file(TH_PATH(dir, scope[i][0]), scope[i][1]);
    }
    char db[512];
    snprintf(db, sizeof(db), "%s/p.db", tmp);
    int indexed = rc == 0 ? sb_index(dir, db, 1, NULL) : -1;
    int inner = indexed == 0 ? sb_proven_to(db, "a.Type") : -1; /* A's site */
    int outer = indexed == 0 ? sb_proven_to(db, "t.Type") : -1; /* B's site only */
    th_cleanup(tmp);
    printf("\n  proven: a.Type %d, t.Type %d\n", inner, outer);
    ASSERT_EQ(indexed, 0);
    ASSERT_EQ(inner, 1);
    ASSERT_EQ(outer, 1);
    PASS();
}

SUITE(site_bind) {
    RUN_TEST(site_bind_protobuf_never_proven_where_protoc_rejects);
    RUN_TEST(site_bind_protobuf_enclosing_message_decides_first);
    RUN_TEST(site_bind_language_without_row_answers_nothing);
    RUN_TEST(site_bind_protobuf_reference_binds_what_protoc_resolves);
    RUN_TEST(site_bind_protobuf_incremental_equals_full);
    RUN_TEST(site_bind_seam_answers_agree_on_both_routes);
}
