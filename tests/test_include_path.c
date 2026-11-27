/*
 * test_include_path.c -- the path rule (src/pipeline/include_path.h): which
 * indexed file a Protobuf or Make import names.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "../src/pipeline/include_path.h"
#include "pipeline/pipeline.h"
#include "sqlite3.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* A universe of the given indexed paths (NULL-terminated). */
static cbm_include_universe_t *ip_universe(const char *const *paths) {
    cbm_file_info_t files[64];
    int n = 0;
    for (; paths[n] && n < 64; n++) {
        memset(&files[n], 0, sizeof(files[n]));
        files[n].rel_path = (char *)paths[n];
    }
    return cbm_include_universe_new(files, n);
}

/* The verdict for an import `path` of the file `rel`, the file in *out. */
static cbm_include_path_verdict_t ip_ask(cbm_include_universe_t *u, CBMLanguage lang,
                                         const char *rel, const char *path, char *out,
                                         size_t out_sz) {
    CBMImport imp;
    memset(&imp, 0, sizeof(imp));
    imp.module_path = path;
    out[0] = '\0';
    return cbm_include_path_resolve(u, NULL, NULL, rel, lang, &imp, out, out_sz);
}

TEST(include_path_proto_tail_guess_and_ambiguity) {
    static const char *const files[] = {"api/a.proto",      "x/common.proto",   "y/z/shared.proto",
                                        "w/shared.proto",   "api/shared.proto", "main.go",
                                        NULL};
    cbm_include_universe_t *u = ip_universe(files);
    ASSERT_NOT_NULL(u);
    char got[1024];

    /* the whole path, and a tail only one file ends with */
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "api/a.proto", "x/common.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_FILE);
    ASSERT_STR_EQ(got, "x/common.proto");
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "api/a.proto", "common.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_FILE);
    ASSERT_STR_EQ(got, "x/common.proto");
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "api/a.proto", "z/shared.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_FILE);
    ASSERT_STR_EQ(got, "y/z/shared.proto");

    /* three files end with the tail: the one beside the importer is a guess
     * (protoc -I . and -I api read different files), never FILE */
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "api/a.proto", "shared.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_GUESS);
    ASSERT_STR_EQ(got, "api/shared.proto");
    /* ... and none from a directory without one */
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "x/common.proto", "shared.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_NONE);

    /* no such file (proven); only a case variant exists (a case-insensitive
     * disk opens it, so not proven); absolute */
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "api/a.proto", "nope/none.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_NO_FILE);
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "api/a.proto", "X/Common.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_NONE);
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "api/a.proto", "/abs/common.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_NONE);
    /* paths protoc refuses ("Backslashes, consecutive slashes, \".\", or \"..\"
     * are not allowed in the virtual path"): not read, never normalised */
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "a.proto", "../common.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_NONE);
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "api/a.proto", "./x/common.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_NONE);
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "api/a.proto", "x//common.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_NONE);
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "api/a.proto", "x\\common.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_NONE);
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "api/a.proto", "x/./common.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_NONE);

    /* a row that names no .proto file is not read as a path */
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "api/a.proto", "x.common", got, sizeof(got)),
              CBM_INCLUDE_PATH_NOT_A_PATH);
    /* a language without a row */
    ASSERT_EQ(ip_ask(u, CBM_LANG_GO, "api/a.go", "x/common.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_NOT_A_PATH);
    cbm_include_universe_free(u);
    PASS();
}

/* GNU make reads a relative include from the directory it runs in (then -I
 * dirs): a root makefile decides, a makefile below the root only guesses
 * (GNU make 4.3: `include b.mk` in sub/a.mk, included from the root, does not
 * read sub/b.mk). */
TEST(include_path_make_root_decides_below_is_guess) {
    static const char *const files[] = {"Makefile",         "sub/Makefile",   "sub/local.mk",
                                        "rules.mk",         "both.mk",        "sub/both.mk",
                                        "other/elsewhere.mk", "sub/a.mk",     "sub/b.mk",
                                        NULL};
    cbm_include_universe_t *u = ip_universe(files);
    ASSERT_NOT_NULL(u);
    char got[1024];
    /* the root makefile: its own directory is the root */
    ASSERT_EQ(ip_ask(u, CBM_LANG_MAKEFILE, "Makefile", "rules.mk", got, sizeof(got)),
              CBM_INCLUDE_PATH_FILE);
    ASSERT_STR_EQ(got, "rules.mk");
    ASSERT_EQ(ip_ask(u, CBM_LANG_MAKEFILE, "Makefile", "sub/local.mk", got, sizeof(got)),
              CBM_INCLUDE_PATH_FILE);
    ASSERT_STR_EQ(got, "sub/local.mk");
    /* below the root: beside it, or at the root, depends on where make runs */
    ASSERT_EQ(ip_ask(u, CBM_LANG_MAKEFILE, "sub/Makefile", "local.mk", got, sizeof(got)),
              CBM_INCLUDE_PATH_GUESS);
    ASSERT_EQ(ip_ask(u, CBM_LANG_MAKEFILE, "sub/Makefile", "rules.mk", got, sizeof(got)),
              CBM_INCLUDE_PATH_GUESS);
    ASSERT_EQ(ip_ask(u, CBM_LANG_MAKEFILE, "sub/a.mk", "b.mk", got, sizeof(got)),
              CBM_INCLUDE_PATH_GUESS);
    /* both: none */
    ASSERT_EQ(ip_ask(u, CBM_LANG_MAKEFILE, "sub/Makefile", "both.mk", got, sizeof(got)),
              CBM_INCLUDE_PATH_NONE);
    /* none (proven); climbing out of its directory, from any directory make
     * runs in it still names a file ending in the rest (none); a name with a
     * variable */
    ASSERT_EQ(ip_ask(u, CBM_LANG_MAKEFILE, "Makefile", "missing.mk", got, sizeof(got)),
              CBM_INCLUDE_PATH_NO_FILE);
    ASSERT_EQ(ip_ask(u, CBM_LANG_MAKEFILE, "sub/x/Makefile", "../../gone.mk", got, sizeof(got)),
              CBM_INCLUDE_PATH_NO_FILE);
    /* not at either base, but elsewhere (make may run there): not proven absent */
    ASSERT_EQ(ip_ask(u, CBM_LANG_MAKEFILE, "sub/Makefile", "elsewhere.mk", got, sizeof(got)),
              CBM_INCLUDE_PATH_NONE);
    ASSERT_EQ(ip_ask(u, CBM_LANG_MAKEFILE, "Makefile", "$(TOP)/rules.mk", got, sizeof(got)),
              CBM_INCLUDE_PATH_NONE);
    cbm_include_universe_free(u);
    PASS();
}

/* The universe is the indexed list: a file the run did not index (an ignored
 * or skipped directory) is not seen, whatever is on the disk. */
TEST(include_path_universe_is_the_indexed_files) {
    static const char *const files[] = {"a.proto", "common/t.proto", "real/l.proto", NULL};
    cbm_include_universe_t *u = ip_universe(files);
    ASSERT_NOT_NULL(u);
    char got[1024];
    /* node_modules/pkg/common/t.proto exists on a disk, not in the index: the
     * indexed one is the only file the tail names */
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "a.proto", "common/t.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_FILE);
    ASSERT_STR_EQ(got, "common/t.proto");
    /* only under an ignored directory: no indexed file, proven */
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "a.proto", "only.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_NO_FILE);
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "a.proto", "l.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_FILE);
    ASSERT_STR_EQ(got, "real/l.proto");
    cbm_include_universe_free(u);
    PASS();
}

/* The manifest key follows the set of indexed names a row keeps: not their
 * order, nor files no row names. */
TEST(include_path_inventory_digest_follows_indexed_names) {
    cbm_file_info_t f[4];
    memset(f, 0, sizeof(f));
    char d0[CBM_SHA256_HEX_LEN + 1];
    char d1[CBM_SHA256_HEX_LEN + 1];
    char d2[CBM_SHA256_HEX_LEN + 1];
    char d3[CBM_SHA256_HEX_LEN + 1];
    f[0].rel_path = "a.proto";
    f[1].rel_path = "main.go";
    ASSERT_EQ(cbm_include_path_inventory_digest(f, 1, d0), 0);
    ASSERT_EQ(cbm_include_path_inventory_digest(f, 2, d1), 0);
    f[2].rel_path = "b/rules.mk";
    ASSERT_EQ(cbm_include_path_inventory_digest(f, 3, d2), 0);
    f[0].rel_path = "b/rules.mk";
    f[2].rel_path = "a.proto";
    ASSERT_EQ(cbm_include_path_inventory_digest(f, 3, d3), 0);
    ASSERT_STR_EQ(d1, d0);
    ASSERT_STR_NEQ(d2, d1);
    ASSERT_STR_EQ(d3, d2);
    PASS();
}

/* Counted work of building the tail index: n files at 1x, 4x, 16x (a
 * question makes no work the universe keeps). */
static bool ip_work(int n, uint64_t *steps) {
    cbm_file_info_t *files = calloc((size_t)n * 2, sizeof(*files));
    char (*names)[64] = calloc((size_t)n * 2, sizeof(*names));
    if (!files || !names) {
        free(files);
        free(names);
        return false;
    }
    for (int i = 0; i < n; i++) {
        snprintf(names[2 * i], 64, "d%d/e%d/f%d.proto", i % 10, i % 7, i);
        snprintf(names[2 * i + 1], 64, "d%d/src%d.go", i % 10, i);
        files[2 * i].rel_path = names[2 * i];
        files[2 * i + 1].rel_path = names[2 * i + 1];
    }
    cbm_include_universe_t *u = cbm_include_universe_new(files, n * 2);
    char got[1024];
    bool ok = u != NULL;
    for (int i = 0; ok && i < n; i++) {
        char want[64];
        snprintf(want, sizeof(want), "f%d.proto", i);
        ok = ip_ask(u, CBM_LANG_PROTOBUF, "top.proto", want, got, sizeof(got)) ==
             CBM_INCLUDE_PATH_FILE;
    }
    *steps = cbm_include_universe_tail_steps(u);
    cbm_include_universe_free(u);
    free(files);
    free(names);
    return ok;
}

TEST(include_path_universe_grows_linearly) {
    uint64_t s1, s4, s16;
    ASSERT_TRUE(ip_work(50, &s1));
    ASSERT_TRUE(ip_work(200, &s4));
    ASSERT_TRUE(ip_work(800, &s16));
    /* index steps: at most 4.5 times per 4 times */
    ASSERT_TRUE(s4 * 2 <= s1 * 9 && s16 * 2 <= s4 * 9);
    ASSERT_GT(s16, s4);
    PASS();
}

/* The IMPORTS decision: a statement that names a file is decided by the path
 * rule when it names one indexed file (its node) or provably none (no edge: a
 * symbol of the name is never its target); with several candidate files, or
 * a guess, main's strategies decide. */
TEST(include_path_names_no_file_decides_no_target) {
    static const char *const files[] = {"a.proto", "x/common.proto", "y/dup.proto",
                                        "z/dup.proto", "Makefile", NULL};
    cbm_gbuf_t *gb = cbm_gbuf_new("p", "/tmp/p");
    char *file_qn = cbm_pipeline_fqn_compute("p", "x/common.proto", "__file__");
    ASSERT_GT(
        cbm_gbuf_upsert_node(gb, "File", "common.proto", file_qn, "x/common.proto", 1, 1, "{}"), 0);
    cbm_pipeline_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.gbuf = gb;
    ctx.project_name = "p";
    ctx.include_universe = ip_universe(files);
    CBMImport imp;
    memset(&imp, 0, sizeof(imp));
    const cbm_gbuf_node_t *out = NULL;

    imp.module_path = "google/protobuf/empty.proto"; /* no indexed file ends with it */
    ASSERT_TRUE(cbm_include_path_decides(&ctx, "a.proto", CBM_LANG_PROTOBUF, &imp, &out));
    ASSERT_NULL(out);
    imp.module_path = "skipped/only.proto"; /* only in a directory the run did not index */
    ASSERT_TRUE(cbm_include_path_decides(&ctx, "a.proto", CBM_LANG_PROTOBUF, &imp, &out));
    ASSERT_NULL(out);
    imp.module_path = "missing.mk";
    ASSERT_TRUE(cbm_include_path_decides(&ctx, "Makefile", CBM_LANG_MAKEFILE, &imp, &out));
    ASSERT_NULL(out);

    /* partners: the named file's node; several files: main decides; a guess:
     * main decides */
    imp.module_path = "x/common.proto";
    ASSERT_TRUE(cbm_include_path_decides(&ctx, "a.proto", CBM_LANG_PROTOBUF, &imp, &out));
    ASSERT_NOT_NULL(out);
    ASSERT_STR_EQ(out->qualified_name, file_qn);
    imp.module_path = "dup.proto";
    ASSERT_FALSE(cbm_include_path_decides(&ctx, "a.proto", CBM_LANG_PROTOBUF, &imp, &out));
    ASSERT_FALSE(cbm_include_path_decides(&ctx, "y/b.proto", CBM_LANG_PROTOBUF, &imp, &out));
    cbm_include_universe_free(ctx.include_universe);
    free(file_qn);
    cbm_gbuf_free(gb);
    PASS();
}

/* ── IMPORTS edges of a real index ───────────────────────────────────────── */

/* How many IMPORTS edges go from the node whose QN ends in `.src` to the node
 * whose QN ends in `.tgt`. */
static int ip_imports(const char *db, const char *src, const char *tgt) {
    sqlite3 *h = NULL;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(h);
        return -1;
    }
    sqlite3_stmt *st = NULL;
    int n = -1;
    if (sqlite3_prepare_v2(h,
                           "SELECT count(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                           "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'IMPORTS' "
                           "AND s.qualified_name LIKE '%.' || ?1 AND t.qualified_name LIKE '%.' || ?2",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, src, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, tgt, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            n = sqlite3_column_int(st, 0);
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(h);
    return n;
}

/* Red on main: an import that names no repository file got main's edge to a
 * same-named symbol (here the Go module mvccpb/kv); it has none now, and the
 * import of a real file keeps its edge. A copy under node_modules (not
 * indexed) is not seen: alone it names no file, beside an indexed copy the
 * indexed one is the only file. Several files and a guess keep main's edge. */
TEST(include_path_edges_name_only_indexed_files) {
    char *tmp = th_mktempdir("cbm_ip_edges");
    ASSERT_NOT_NULL(tmp);
    char dir[512];
    snprintf(dir, sizeof(dir), "%s/repo", tmp);
    ASSERT_EQ(th_write_file(TH_PATH(dir, "api/rpc.proto"),
                            "syntax = \"proto3\";\npackage api;\nimport \"etcd/api/mvccpb/kv.proto\";\n"
                            "import \"kvs/real.proto\";\nimport \"only.proto\";\n"
                            "import \"common/t.proto\";\nmessage R { int32 a = 1; }\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(dir, "kvs/real.proto"),
                            "syntax = \"proto3\";\npackage kvs;\nmessage Real { int32 a = 1; }\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(dir, "common/t.proto"),
                            "syntax = \"proto3\";\npackage common;\nmessage T { int32 a = 1; }\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(dir, "mvccpb/kv.go"),
                            "package mvccpb\n\ntype KeyValue struct{}\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(dir, "node_modules/pkg/only.proto"),
                            "syntax = \"proto3\";\npackage only;\nmessage O { int32 a = 1; }\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(dir, "node_modules/pkg/common/t.proto"),
                            "syntax = \"proto3\";\npackage common;\nmessage T { int32 b = 1; }\n"),
              0);
    /* B1: two files end with common.proto; protoc -I . reads the root one, -I
     * svc the other: main's edge (to the root file) stays, never svc/'s */
    ASSERT_EQ(th_write_file(TH_PATH(dir, "status.proto"),
                            "syntax = \"proto3\";\nmessage Status { int32 root_field = 1; }\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(dir, "svc/status.proto"),
                            "syntax = \"proto3\";\nmessage Status { int32 svc_field = 1; }\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(dir, "svc/api.proto"),
                            "syntax = \"proto3\";\npackage api;\nimport \"status.proto\";\n"
                            "message Req { Status s = 1; }\n"),
              0);
    char db[512];
    snprintf(db, sizeof(db), "%s/ip.db", tmp);
    cbm_pipeline_t *p = cbm_pipeline_new(dir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);
    /* main drew api/rpc.proto -> mvccpb.kv for the etcd path */
    ASSERT_EQ(ip_imports(db, "api.rpc.proto.__file__", "mvccpb.kv"), 0);
    ASSERT_EQ(ip_imports(db, "api.rpc.proto.__file__", "kvs.real"), 1);
    ASSERT_EQ(ip_imports(db, "api.rpc.proto.__file__", "common.t"), 1);
    ASSERT_EQ(ip_imports(db, "api.rpc.proto.__file__", "pkg.only"), 0);
    ASSERT_EQ(ip_imports(db, "api.rpc.proto.__file__", "pkg.common.t"), 0);
    ASSERT_EQ(ip_imports(db, "svc.api.proto.__file__", "svc.status"), 0);
    th_cleanup(tmp);
    PASS();
}

/* ── A path whose dots and slashes are swapped ───────────────────────────── */

/* A File node's name joins a path's segments with dots, so a.b.proto and
 * a/b.proto share one. protoc reads only the file the path names ("a.b.proto:
 * File not found." when only a/b.proto exists), GNU make the same ("a.b.mk: No
 * such file or directory"; `-include` skips it); the other spelling is no
 * answer, in both directions. Asked with the run's File nodes: the rule takes
 * existence from the listed path, never from a node found by name. */
TEST(include_path_swapped_spelling_names_no_file) {
    static const char *const files[] = {"top.proto", "a/b.proto", "x/y.proto", "r.s.proto",
                                        "Makefile",  "a/b.mk",    "x/y.mk",    "r.s.mk", NULL};
    cbm_include_universe_t *u = ip_universe(files);
    ASSERT_NOT_NULL(u);
    cbm_gbuf_t *g = cbm_gbuf_new("ipx", "/");
    ASSERT_NOT_NULL(g);
    for (int i = 0; files[i]; i++) {
        char *qn = cbm_pipeline_fqn_compute("ipx", files[i], "__file__");
        const char *slash = strrchr(files[i], '/');
        (void)cbm_gbuf_upsert_node(g, "File", slash ? slash + 1 : files[i], qn, files[i], 0, 0,
                                   "{}");
        free(qn);
    }
    char got[1024];
    CBMImport imp;
    memset(&imp, 0, sizeof(imp));
    static const struct {
        CBMLanguage lang;
        const char *rel;
        const char *path;
        cbm_include_path_verdict_t want;
        const char *file;
    } cases[] = {
        {CBM_LANG_PROTOBUF, "top.proto", "a.b.proto", CBM_INCLUDE_PATH_NO_FILE, ""},
        {CBM_LANG_PROTOBUF, "top.proto", "r/s.proto", CBM_INCLUDE_PATH_NO_FILE, ""},
        {CBM_LANG_PROTOBUF, "top.proto", "x/y.proto", CBM_INCLUDE_PATH_FILE, "x/y.proto"},
        {CBM_LANG_MAKEFILE, "Makefile", "a.b.mk", CBM_INCLUDE_PATH_NO_FILE, ""},
        {CBM_LANG_MAKEFILE, "Makefile", "r/s.mk", CBM_INCLUDE_PATH_NO_FILE, ""},
        {CBM_LANG_MAKEFILE, "Makefile", "x/y.mk", CBM_INCLUDE_PATH_FILE, "x/y.mk"},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        imp.module_path = cases[i].path;
        got[0] = '\0';
        ASSERT_EQ(cbm_include_path_resolve(u, g, "ipx", cases[i].rel, cases[i].lang, &imp, got,
                                           sizeof(got)),
                  cases[i].want);
        if (cases[i].want == CBM_INCLUDE_PATH_FILE) {
            ASSERT_STR_EQ(got, cases[i].file);
        }
    }
    cbm_gbuf_free(g);
    cbm_include_universe_free(u);
    PASS();
}

/* Both spellings indexed: they share one node name, so the graph holds one
 * node for the two, with the smaller path (a.b.mk: '.' sorts before '/').
 * `include a/b.mk` names a/b.mk: its target is a node of a/b.mk or none, never
 * the node of a.b.mk. */
TEST(include_path_target_is_that_file) {
    static const char *const files[] = {"Makefile", "a.b.mk", "a/b.mk", NULL};
    cbm_gbuf_t *gb = cbm_gbuf_new("p", "/tmp/p");
    ASSERT_NOT_NULL(gb);
    for (int i = 0; files[i]; i++) {
        char *qn = cbm_pipeline_fqn_compute("p", files[i], "__file__");
        const char *slash = strrchr(files[i], '/');
        (void)cbm_gbuf_upsert_node(gb, "File", slash ? slash + 1 : files[i], qn, files[i], 1, 1,
                                   "{}");
        free(qn);
    }
    cbm_pipeline_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.gbuf = gb;
    ctx.project_name = "p";
    ctx.include_universe = ip_universe(files);
    CBMImport imp;
    memset(&imp, 0, sizeof(imp));
    const cbm_gbuf_node_t *out = NULL;
    imp.module_path = "a/b.mk";
    ASSERT_TRUE(cbm_include_path_decides(&ctx, "Makefile", CBM_LANG_MAKEFILE, &imp, &out));
    ASSERT_TRUE(out == NULL || (out->file_path && strcmp(out->file_path, "a/b.mk") == 0));
    cbm_include_universe_free(ctx.include_universe);
    cbm_gbuf_free(gb);
    PASS();
}

/* How many IMPORTS edges go from a node of the file `src` to a node of the
 * file `tgt` with the label `label` (tgt or label NULL: any). */
static int ip_targets(const char *db, const char *src, const char *tgt, const char *label) {
    sqlite3 *h = NULL;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(h);
        return -1;
    }
    sqlite3_stmt *st = NULL;
    int n = -1;
    if (sqlite3_prepare_v2(h,
                           "SELECT count(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                           "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'IMPORTS' "
                           "AND s.file_path = ?1 AND (?2 IS NULL OR t.file_path = ?2) "
                           "AND (?3 IS NULL OR t.label = ?3)",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, src, -1, SQLITE_TRANSIENT);
        if (tgt) {
            sqlite3_bind_text(st, 2, tgt, -1, SQLITE_TRANSIENT);
        }
        if (label) {
            sqlite3_bind_text(st, 3, label, -1, SQLITE_TRANSIENT);
        }
        if (sqlite3_step(st) == SQLITE_ROW) {
            n = sqlite3_column_int(st, 0);
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(h);
    return n;
}

/* The IMPORTS edges of a stored index, one sorted line each. */
static char *ip_listing(const char *db_path) {
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return NULL;
    }
    sqlite3_stmt *st = NULL;
    char *buf = calloc(1, 1);
    size_t len = 0;
    if (buf && sqlite3_prepare_v2(db,
                                  "SELECT s.file_path||'|'||coalesce(t.file_path,'')||'|'||t.label "
                                  "FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
                                  "t.id=e.target_id WHERE e.type='IMPORTS' ORDER BY 1",
                                  -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *row = (const char *)sqlite3_column_text(st, 0);
            size_t n = row ? strlen(row) : 0;
            char *grown = realloc(buf, len + n + 2);
            if (!grown) {
                break;
            }
            buf = grown;
            memcpy(buf + len, row ? row : "", n);
            buf[len + n] = '\n';
            len += n + 1;
            buf[len] = '\0';
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return buf;
}

/* Index `repo` into `db` (incremental when `db` holds its index). */
static int ip_run(const char *repo, const char *db) {
    cbm_pipeline_t *p = cbm_pipeline_new(repo, db, CBM_MODE_FULL);
    if (!p) {
        return -1;
    }
    (void)cbm_pipeline_set_project_name(p, "ipswap");
    cbm_pipeline_set_persistence(p, false);
    int rc = cbm_pipeline_run(p);
    cbm_pipeline_free(p);
    return rc;
}

/* The kept index, indexed again (incremental), holds the IMPORTS edges a fresh
 * index of the tree holds. */
static bool ip_kept_equals_fresh(const char *tmp, const char *repo, const char *kept, int step) {
    char fresh[512];
    snprintf(fresh, sizeof(fresh), "%s/fresh-%d.db", tmp, step);
    if (ip_run(repo, kept) != 0 || ip_run(repo, fresh) != 0) {
        return false;
    }
    char *a = ip_listing(kept);
    char *b = ip_listing(fresh);
    bool same = a && b && strcmp(a, b) == 0;
    if (!same) {
        fprintf(stderr, "  step %d\n--- incremental ---\n%s--- full ---\n%s", step, a ? a : "",
                b ? b : "");
    }
    free(a);
    free(b);
    (void)remove(fresh);
    return same;
}

/* The swapped spellings on a real index, on the sequential and the parallel
 * route (60 more files than the parallel route's floor), then incrementally:
 * another file edited (the false edge stays gone); the really named file
 * created and the other spelling moved away (the edge moves to it); then
 * back (no edge). Red on main, whose
 * name lookup drew Makefile -> a/b.mk (File) and, through its own strategies
 * after the rule's guess, top.proto -> pa/b.proto (Module). */
static const char *ip_swapped_repo(const char *tmp, bool parallel) {
    static char repo[512];
    snprintf(repo, sizeof(repo), "%s/%s", tmp, parallel ? "par" : "seq");
    char path[600];
    const char *files[][2] = {
        {"Makefile", "include a.b.mk\n-include c.d.mk\ninclude r/s.mk\ninclude x/y.mk\n\nall:\n"},
        {"a/b.mk", "B = 1\n"},
        {"c/d.mk", "D = 1\n"},
        {"r.s.mk", "S = 1\n"},
        {"x/y.mk", "Y = 1\n"},
        {"top.proto", "syntax = \"proto3\";\nimport \"pa.b.proto\";\nimport \"pr/s.proto\";\n"
                      "import \"px/y.proto\";\nmessage T { int32 t = 1; }\n"},
        {"pa/b.proto", "syntax = \"proto3\";\nmessage B { int32 b = 1; }\n"},
        {"pr.s.proto", "syntax = \"proto3\";\nmessage S { int32 s = 1; }\n"},
        {"px/y.proto", "syntax = \"proto3\";\nmessage Y { int32 y = 1; }\n"},
    };
    for (size_t i = 0; i < sizeof(files) / sizeof(files[0]); i++) {
        snprintf(path, sizeof(path), "%s/%s", repo, files[i][0]);
        if (th_write_file(path, files[i][1]) != 0) {
            return NULL;
        }
    }
    for (int i = 0; i < 60; i++) {
        snprintf(path, sizeof(path), "%s/fill/f%02d.go", repo, i);
        char body[64];
        snprintf(body, sizeof(body), "package fill\n\nfunc F%02d() {}\n", i);
        if (th_write_file(path, body) != 0) {
            return NULL;
        }
    }
    return repo;
}

/* Sets CBM_WORKERS / CBM_INDEX_SINGLE_THREAD for one route, or restores them. */
static void ip_route_env(bool parallel, bool restore) {
    static char *saved_workers;
    static char *saved_single;
    if (restore) {
        saved_workers ? cbm_setenv("CBM_WORKERS", saved_workers, 1) : cbm_unsetenv("CBM_WORKERS");
        saved_single ? cbm_setenv("CBM_INDEX_SINGLE_THREAD", saved_single, 1)
                     : cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
        free(saved_workers);
        free(saved_single);
        saved_workers = saved_single = NULL;
        return;
    }
    const char *w = getenv("CBM_WORKERS");
    const char *s = getenv("CBM_INDEX_SINGLE_THREAD");
    saved_workers = w ? strdup(w) : NULL;
    saved_single = s ? strdup(s) : NULL;
    if (parallel) {
        cbm_setenv("CBM_WORKERS", "4", 1);
        cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    } else {
        cbm_setenv("CBM_INDEX_SINGLE_THREAD", "1", 1);
    }
}

#define IP_SWAPPED_ROUTE(parallel)                                                                 \
    do {                                                                                           \
        char *made = th_mktempdir("cbm_ip_swap");                                                  \
        ASSERT_NOT_NULL(made);                                                                     \
        char tmp[256];                                                                             \
        snprintf(tmp, sizeof(tmp), "%s", made);                                                    \
        const char *repo = ip_swapped_repo(tmp, (parallel));                                       \
        ASSERT_NOT_NULL(repo);                                                                     \
        char kept[512];                                                                            \
        snprintf(kept, sizeof(kept), "%s/kept.db", tmp);                                           \
        ip_route_env((parallel), false);                                                           \
        cbm_pipeline_worker_count_test_reset();                                                    \
        int rc = ip_run(repo, kept);                                                               \
        int workers = cbm_pipeline_worker_count_test_last();                                       \
        ip_route_env((parallel), true);                                                            \
        ASSERT_EQ(rc, 0);                                                                          \
        ASSERT_TRUE((parallel) ? workers > 1 : workers == 1);                                      \
        ASSERT_EQ(ip_targets(kept, "Makefile", "a/b.mk", NULL), 0);                                \
        ASSERT_EQ(ip_targets(kept, "Makefile", "c/d.mk", NULL), 0);                                \
        ASSERT_EQ(ip_targets(kept, "Makefile", "r.s.mk", NULL), 0);                                \
        ASSERT_EQ(ip_targets(kept, "Makefile", "x/y.mk", "Module"), 1);                            \
        ASSERT_EQ(ip_targets(kept, "Makefile", NULL, NULL), 1);                                    \
        ASSERT_EQ(ip_targets(kept, "top.proto", "pa/b.proto", NULL), 0);                           \
        ASSERT_EQ(ip_targets(kept, "top.proto", "pr.s.proto", NULL), 0);                           \
        ASSERT_EQ(ip_targets(kept, "top.proto", "px/y.proto", "Module"), 1);                       \
        ASSERT_EQ(ip_targets(kept, "top.proto", NULL, NULL), 1);                                   \
        ip_route_env((parallel), false);                                                           \
        /* 1: another file edited */                                                               \
        char path[600];                                                                            \
        snprintf(path, sizeof(path), "%s/fill/f00.go", repo);                                      \
        bool same1 = th_write_file(path, "package fill\n\nfunc F00() { F01() }\n") == 0 &&         \
                     ip_kept_equals_fresh(tmp, repo, kept, 1);                                     \
        int s1 = ip_targets(kept, "Makefile", "a/b.mk", NULL) +                                    \
                 ip_targets(kept, "top.proto", "pa/b.proto", NULL);                                \
        /* 2: the really named files created, the other spelling moved away (the two     \
         * spellings share one node name, so the graph holds one node for both): the    \
         * edges move to them */                                                                   \
        snprintf(path, sizeof(path), "%s/a/b.mk", repo);                                           \
        bool w2 = remove(path) == 0;                                                               \
        snprintf(path, sizeof(path), "%s/pa/b.proto", repo);                                       \
        w2 = w2 && remove(path) == 0;                                                              \
        snprintf(path, sizeof(path), "%s/a.b.mk", repo);                                           \
        w2 = w2 && th_write_file(path, "AB = 1\n") == 0;                                           \
        snprintf(path, sizeof(path), "%s/pa.b.proto", repo);                                       \
        w2 = w2 && th_write_file(path, "syntax = \"proto3\";\nmessage AB { int32 a = 1; }\n") == 0; \
        bool same2 = w2 && ip_kept_equals_fresh(tmp, repo, kept, 2);                               \
        int s2_new = ip_targets(kept, "Makefile", "a.b.mk", NULL) +                                \
                     ip_targets(kept, "top.proto", "pa.b.proto", NULL);                            \
        int s2_old = ip_targets(kept, "Makefile", "a/b.mk", NULL) +                                \
                     ip_targets(kept, "top.proto", "pa/b.proto", NULL);                            \
        /* 3: deleted again, the other spelling back */                                            \
        snprintf(path, sizeof(path), "%s/a.b.mk", repo);                                           \
        bool d3 = remove(path) == 0;                                                               \
        snprintf(path, sizeof(path), "%s/pa.b.proto", repo);                                       \
        d3 = d3 && remove(path) == 0;                                                              \
        snprintf(path, sizeof(path), "%s/a/b.mk", repo);                                           \
        d3 = d3 && th_write_file(path, "B = 1\n") == 0;                                            \
        snprintf(path, sizeof(path), "%s/pa/b.proto", repo);                                       \
        d3 = d3 && th_write_file(path, "syntax = \"proto3\";\nmessage B { int32 b = 1; }\n") == 0; \
        bool same3 = d3 && ip_kept_equals_fresh(tmp, repo, kept, 3);                               \
        int s3 = ip_targets(kept, "Makefile", "a/b.mk", NULL) +                                    \
                 ip_targets(kept, "top.proto", "pa/b.proto", NULL) +                               \
                 ip_targets(kept, "Makefile", "a.b.mk", NULL) +                                    \
                 ip_targets(kept, "top.proto", "pa.b.proto", NULL);                                \
        ip_route_env((parallel), true);                                                            \
        ASSERT_TRUE(same1);                                                                        \
        ASSERT_EQ(s1, 0);                                                                          \
        ASSERT_TRUE(same2);                                                                        \
        ASSERT_EQ(s2_new, 2);                                                                      \
        ASSERT_EQ(s2_old, 0);                                                                      \
        ASSERT_TRUE(same3);                                                                        \
        ASSERT_EQ(s3, 0);                                                                          \
        th_cleanup(tmp);                                                                           \
    } while (0)

TEST(include_path_swapped_spelling_sequential) {
    IP_SWAPPED_ROUTE(false);
    PASS();
}

TEST(include_path_swapped_spelling_parallel) {
    IP_SWAPPED_ROUTE(true);
    PASS();
}

SUITE(include_path) {
    RUN_TEST(include_path_swapped_spelling_names_no_file);
    RUN_TEST(include_path_target_is_that_file);
    RUN_TEST(include_path_swapped_spelling_sequential);
    RUN_TEST(include_path_swapped_spelling_parallel);
    RUN_TEST(include_path_names_no_file_decides_no_target);
    RUN_TEST(include_path_proto_tail_guess_and_ambiguity);
    RUN_TEST(include_path_make_root_decides_below_is_guess);
    RUN_TEST(include_path_universe_is_the_indexed_files);
    RUN_TEST(include_path_inventory_digest_follows_indexed_names);
    RUN_TEST(include_path_universe_grows_linearly);
    RUN_TEST(include_path_edges_name_only_indexed_files);
}
