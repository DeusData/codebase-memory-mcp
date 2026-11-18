/*
 * test_include_path.c -- the path rule (src/pipeline/include_path.h): which
 * repository file a Protobuf or Make import names, read from a real tree.
 */
#include "test_framework.h"
#include "test_helpers.h"
#include "../src/pipeline/include_path.h"
#include "../src/foundation/compat_fs.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static char *ip_tree(void) {
    char *dir = th_mktempdir("cbm_ip");
    return dir ? strdup(dir) : NULL;
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

TEST(include_path_proto_tail_beside_and_ambiguity) {
    char *root = ip_tree();
    ASSERT_NOT_NULL(root);
    th_write_file(TH_PATH(root, "api/a.proto"), "syntax = \"proto3\";\n");
    th_write_file(TH_PATH(root, "x/common.proto"), "syntax = \"proto3\";\n");
    th_write_file(TH_PATH(root, "y/z/shared.proto"), "syntax = \"proto3\";\n");
    th_write_file(TH_PATH(root, "w/shared.proto"), "syntax = \"proto3\";\n");
    th_write_file(TH_PATH(root, "api/shared.proto"), "syntax = \"proto3\";\n");
    cbm_include_universe_t *u = cbm_include_universe_new(root);
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

    /* three files end with the tail: the one beside the importer */
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "api/a.proto", "shared.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_FILE);
    ASSERT_STR_EQ(got, "api/shared.proto");
    /* ... and none from a directory without one */
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "x/common.proto", "shared.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_NONE);

    /* no such file; a path the rule cannot read; leaving the repository */
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "api/a.proto", "nope/none.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_NONE);
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "api/a.proto", "/abs/common.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_NONE);
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "a.proto", "../common.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_NONE);

    /* a row that names no .proto file is not read as a path */
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "api/a.proto", "x.common", got, sizeof(got)),
              CBM_INCLUDE_PATH_NOT_A_PATH);
    /* a language without a row */
    ASSERT_EQ(ip_ask(u, CBM_LANG_GO, "api/a.go", "x/common.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_NOT_A_PATH);
    cbm_include_universe_free(u);
    th_rmtree(root);
    free(root);
    PASS();
}

TEST(include_path_make_exactly_one_base) {
    char *root = ip_tree();
    ASSERT_NOT_NULL(root);
    th_write_file(TH_PATH(root, "Makefile"), "include rules.mk\n");
    th_write_file(TH_PATH(root, "sub/Makefile"), "include local.mk\n");
    th_write_file(TH_PATH(root, "sub/local.mk"), "X = 1\n");
    th_write_file(TH_PATH(root, "rules.mk"), "Y = 1\n");
    th_write_file(TH_PATH(root, "both.mk"), "Z = 1\n");
    th_write_file(TH_PATH(root, "sub/both.mk"), "Z = 2\n");
    cbm_include_universe_t *u = cbm_include_universe_new(root);
    ASSERT_NOT_NULL(u);
    char got[1024];
    /* only beside the makefile; only at the root */
    ASSERT_EQ(ip_ask(u, CBM_LANG_MAKEFILE, "sub/Makefile", "local.mk", got, sizeof(got)),
              CBM_INCLUDE_PATH_FILE);
    ASSERT_STR_EQ(got, "sub/local.mk");
    ASSERT_EQ(ip_ask(u, CBM_LANG_MAKEFILE, "sub/Makefile", "rules.mk", got, sizeof(got)),
              CBM_INCLUDE_PATH_FILE);
    ASSERT_STR_EQ(got, "rules.mk");
    /* both: which one make reads depends on where it runs */
    ASSERT_EQ(ip_ask(u, CBM_LANG_MAKEFILE, "sub/Makefile", "both.mk", got, sizeof(got)),
              CBM_INCLUDE_PATH_NONE);
    /* none; a name with a variable */
    ASSERT_EQ(ip_ask(u, CBM_LANG_MAKEFILE, "Makefile", "missing.mk", got, sizeof(got)),
              CBM_INCLUDE_PATH_NONE);
    ASSERT_EQ(ip_ask(u, CBM_LANG_MAKEFILE, "Makefile", "$(TOP)/rules.mk", got, sizeof(got)),
              CBM_INCLUDE_PATH_NONE);
    cbm_include_universe_free(u);
    th_rmtree(root);
    free(root);
    PASS();
}

/* `.git` and a symbolic link to a directory are not entered; a file under
 * either is no answer. */
TEST(include_path_listing_skips_git_and_links) {
    char *root = ip_tree();
    ASSERT_NOT_NULL(root);
    th_write_file(TH_PATH(root, "a.proto"), "");
    th_write_file(TH_PATH(root, ".git/hidden/g.proto"), "");
    th_write_file(TH_PATH(root, "real/l.proto"), "");
    char target[1024];
    snprintf(target, sizeof(target), "%s/real", root);
    ASSERT_EQ(symlink(target, TH_PATH(root, "linked")), 0);
    cbm_include_universe_t *u = cbm_include_universe_new(root);
    char got[1024];
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "a.proto", "hidden/g.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_NONE);
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "a.proto", "linked/l.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_NONE);
    /* the file itself, where it really is, is one answer (the link adds none) */
    ASSERT_EQ(ip_ask(u, CBM_LANG_PROTOBUF, "a.proto", "l.proto", got, sizeof(got)),
              CBM_INCLUDE_PATH_FILE);
    ASSERT_STR_EQ(got, "real/l.proto");
    cbm_include_universe_free(u);
    th_rmtree(root);
    free(root);
    PASS();
}

/* The manifest digest follows the set of listed names, not their contents
 * nor files the rows do not name. */
TEST(include_path_listing_digest_follows_names) {
    char *root = ip_tree();
    ASSERT_NOT_NULL(root);
    th_write_file(TH_PATH(root, "a.proto"), "one");
    char d0[CBM_SHA256_HEX_LEN + 1];
    char d1[CBM_SHA256_HEX_LEN + 1];
    char d2[CBM_SHA256_HEX_LEN + 1];
    char d3[CBM_SHA256_HEX_LEN + 1];
    ASSERT_EQ(cbm_include_path_listing_digest(root, d0), 0);
    th_write_file(TH_PATH(root, "a.proto"), "two");
    th_write_file(TH_PATH(root, "main.go"), "package main\n");
    ASSERT_EQ(cbm_include_path_listing_digest(root, d1), 0);
    th_write_file(TH_PATH(root, "vendor/pruned/b.proto"), "");
    ASSERT_EQ(cbm_include_path_listing_digest(root, d2), 0);
    th_write_file(TH_PATH(root, "build/rules.mk"), "");
    ASSERT_EQ(cbm_include_path_listing_digest(root, d3), 0);
    ASSERT_STR_EQ(d1, d0);
    ASSERT_STR_NEQ(d2, d1);
    ASSERT_STR_NEQ(d3, d2);
    th_rmtree(root);
    free(root);
    PASS();
}

/* Counted work of the listing and the tail index: n files at 1x, 4x, 16x. */
static bool ip_work(int n, uint64_t *visited, uint64_t *steps) {
    char *root = ip_tree();
    if (!root) {
        return false;
    }
    for (int i = 0; i < n; i++) {
        char rel[64];
        snprintf(rel, sizeof(rel), "d%d/e%d/f%d.proto", i % 10, i % 7, i);
        th_write_file(TH_PATH(root, rel), "");
        snprintf(rel, sizeof(rel), "d%d/src%d.go", i % 10, i);
        th_write_file(TH_PATH(root, rel), "");
    }
    cbm_include_universe_t *u = cbm_include_universe_new(root);
    char got[1024];
    bool ok = u != NULL;
    for (int i = 0; ok && i < n; i++) {
        char want[64];
        snprintf(want, sizeof(want), "f%d.proto", i);
        ok = ip_ask(u, CBM_LANG_PROTOBUF, "top.proto", want, got, sizeof(got)) ==
             CBM_INCLUDE_PATH_FILE;
    }
    *visited = cbm_include_universe_visited(u);
    *steps = cbm_include_universe_tail_steps(u);
    cbm_include_universe_free(u);
    th_rmtree(root);
    free(root);
    return ok;
}

TEST(include_path_listing_grows_linearly) {
    uint64_t v1, s1, v4, s4, v16, s16;
    ASSERT_TRUE(ip_work(50, &v1, &s1));
    ASSERT_TRUE(ip_work(200, &v4, &s4));
    ASSERT_TRUE(ip_work(800, &v16, &s16));
    /* entries read and index steps: at most 4.5 times per 4 times */
    ASSERT_TRUE(v4 * 2 <= v1 * 9 && v16 * 2 <= v4 * 9);
    ASSERT_TRUE(s4 * 2 <= s1 * 9 && s16 * 2 <= s4 * 9);
    ASSERT_GT(v16, v4);
    PASS();
}

SUITE(include_path) {
    RUN_TEST(include_path_proto_tail_beside_and_ambiguity);
    RUN_TEST(include_path_make_exactly_one_base);
    RUN_TEST(include_path_listing_skips_git_and_links);
    RUN_TEST(include_path_listing_digest_follows_names);
    RUN_TEST(include_path_listing_grows_linearly);
}
