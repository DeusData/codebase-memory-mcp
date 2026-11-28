/*
 * test_include_path.c -- the path rule (src/pipeline/include_path.h): which
 * indexed file a CFML, Protobuf or Make import names.
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
    if (lang == CBM_LANG_CFML) {
        imp.kind = CBM_IMPORT_KIND_TEMPLATE;
    }
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

TEST(include_path_cfml_long_paths_preserve_tail_counts) {
    char long_path[1200];
    size_t prefix = 0;
    for (int i = 0; i < 6; i++) {
        memset(long_path + prefix, 'a', 180);
        prefix += 180;
        long_path[prefix++] = '/';
    }
    strcpy(long_path + prefix, "views/shared.cfm");
    cbm_file_info_t files[] = {
        {.rel_path = "index.cfm", .language = CBM_LANG_CFML},
        {.rel_path = "site/views/local.cfm"},
        {.rel_path = "site/views/shared.cfm"},
        {.rel_path = long_path},
    };
    cbm_include_universe_t *u = cbm_include_universe_new(files, 4);
    ASSERT_NOT_NULL(u);
    char got[1024];
    ASSERT_EQ(ip_ask(u, CBM_LANG_CFML, "index.cfm", "/views/local.cfm", got, sizeof(got)),
              CBM_INCLUDE_PATH_FILE);
    ASSERT_STR_EQ(got, "site/views/local.cfm");
    ASSERT_EQ(ip_ask(u, CBM_LANG_CFML, "index.cfm", "/views/shared.cfm", got, sizeof(got)),
              CBM_INCLUDE_PATH_NO_FILE);
    cbm_include_universe_free(u);
    PASS();
}

TEST(include_path_cfml_template_edges) {
    static const struct {
        const char *path;
        const char *text;
    } files[] = {
        {"app/index.cfm", "<cfinclude template='local.cfm'>\n"
                          "<cfif true>\n"
                          "    <CFMODULE TEMPLATE='/views/panel.cfm' />\n"
                          "<cfelse>\n"
                          "    <cfinclude template='../shared/branch.cfm' />\n"
                          "</cfif>\n"
                          "<cfinclude template='/views/footer.html' />\n"
                          "<cfinclude template='o''brien.cfm' />\n"
                          "<cfinclude template='#var#.cfm' />\n"
                          "<cfinclude template='missing.cfm' />\n"
                          "<cfinclude template='elsewhere.cfm' />\n"
                          "<cfinclude template='/duplicate.cfm' />\n"},
        {"app/Widget.cfc", "<cfcomponent><cfinclude template='local.cfm'></cfcomponent>\n"},
        {"app/local.cfm", "<p>Local</p>\n"},
        {"app/o'brien.cfm", "<p>Actual target</p>\n"},
        {"app/o''brien.cfm", "<p>Doubled-quote filename</p>\n"},
        {"site/views/panel.cfm", "<p>Panel</p>\n"},
        {"site/views/footer.html", "<p>Footer</p>\n"},
        {"shared/branch.cfm", "<p>Branch</p>\n"},
        {"app/#var#.cfm", "<p>Dynamic bait</p>\n"},
        {"other/elsewhere.cfm", "<p>Relative path bait</p>\n"},
        {"app/duplicate.cfm", "<p>First candidate</p>\n"},
        {"other/duplicate.cfm", "<p>Second candidate</p>\n"},
    };
    char *tmp = th_mktempdir("cbm_cfml_edges");
    ASSERT_NOT_NULL(tmp);
    char dir[512], db[512];
    snprintf(dir, sizeof(dir), "%s/repo", tmp);
    snprintf(db, sizeof(db), "%s/ip.db", tmp);
    for (size_t i = 0; i < sizeof(files) / sizeof(files[0]); i++) {
        ASSERT_EQ(th_write_file(TH_PATH(dir, files[i].path), files[i].text), 0);
    }
    cbm_pipeline_t *p = cbm_pipeline_new(dir, db, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_EQ(cbm_pipeline_run(p), 0);
    cbm_pipeline_free(p);
    ASSERT_EQ(ip_imports(db, "app.index.cfm.__file__", "app.local"), 1);
    ASSERT_EQ(ip_imports(db, "app.index.cfm.__file__", "site.views.panel"), 1);
    ASSERT_EQ(ip_imports(db, "app.index.cfm.__file__", "site.views.footer"), 1);
    ASSERT_EQ(ip_imports(db, "app.index.cfm.__file__", "shared.branch"), 1);
    ASSERT_EQ(ip_imports(db, "app.index.cfm.__file__", "app.o'brien"), 1);
    ASSERT_EQ(ip_imports(db, "app.index.cfm.__file__", "app.o''brien"), 0);
    ASSERT_EQ(ip_imports(db, "app.index.cfm.__file__", "%"), 5);
    ASSERT_EQ(ip_imports(db, "app.Widget.cfc.__file__", "app.local"), 1);
    th_cleanup(tmp);
    PASS();
}

SUITE(include_path) {
    RUN_TEST(include_path_names_no_file_decides_no_target);
    RUN_TEST(include_path_proto_tail_guess_and_ambiguity);
    RUN_TEST(include_path_make_root_decides_below_is_guess);
    RUN_TEST(include_path_universe_is_the_indexed_files);
    RUN_TEST(include_path_inventory_digest_follows_indexed_names);
    RUN_TEST(include_path_universe_grows_linearly);
    RUN_TEST(include_path_edges_name_only_indexed_files);
    RUN_TEST(include_path_cfml_long_paths_preserve_tail_counts);
    RUN_TEST(include_path_cfml_template_edges);
}
