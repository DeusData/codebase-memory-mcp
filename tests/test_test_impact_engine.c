/*
 * test_test_impact_engine.c — detect_changes scope:"tests" end to end
 * (src/mcp/test_impact_engine*.c) on small real git repositories: a base
 * commit, a change, and the answer for it.
 *
 * Without an admitted coverage map every runner suite runs whole by design;
 * what the static walk found shows in each suite's reasons ("STATIC" for a
 * reached suite, "CHANGED" for a changed one).
 */
#include "test_framework.h"
#include "test_helpers.h"

#include "cli/cli.h"
#include "foundation/compat.h"
#include "foundation/platform.h"
#include "foundation/subprocess.h"
#include "mcp/test_impact_engine.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* glibc's _FORTIFY_SOURCE realpath() aborts unless the output buffer is at
 * least PATH_MAX bytes, independent of the actual path length. */
#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

typedef struct {
    char git[1024];
    char home[1024];
    char repo[1100];
    char work[1100];
    char log[1100];
} tie_fixture_t;

static bool tie_git(tie_fixture_t *fx, const char *const *tail) {
    const char *argv[32] = {fx->git,
                            "-C",
                            fx->repo,
                            "-c",
                            "commit.gpgSign=false",
                            "-c",
                            "core.autocrlf=false",
                            "-c",
                            "user.name=t",
                            "-c",
                            "user.email=t@example.invalid"};
    size_t n = 11;
    for (size_t i = 0; tail[i] && n + 1 < sizeof(argv) / sizeof(argv[0]); i++) {
        argv[n++] = tail[i];
    }
    argv[n] = NULL;
    cbm_proc_opts_t opts = {.bin = fx->git,
                            .argv = argv,
                            .log_file = fx->log,
                            .strip_git_repo_env = true,
                            .quiet_timeout_ms = 20000};
    cbm_proc_result_t result = {0};
    return cbm_subprocess_run(&opts, &result) == 0 && result.outcome == CBM_PROC_CLEAN &&
           result.exit_code == 0;
}

static bool tie_write(tie_fixture_t *fx, const char *rel, const char *content) {
    return th_write_file(TH_PATH(fx->repo, rel), content) == 0;
}

static bool tie_commit(tie_fixture_t *fx, const char *message) {
    const char *add[] = {"add", "-A", NULL};
    const char *commit[] = {"commit", "--quiet", "-m", message, NULL};
    return tie_git(fx, add) && tie_git(fx, commit);
}

static const char tie_config[] =
    "{\"test_impact\":{\"version\":1,\"tests\":{\"presets\":{\"c-cbm\":true}},"
    "\"rules\":[{\"id\":\"build\",\"paths\":[\"Makefile\"],\"action\":\"run_all\"}]}}\n";

static const char tie_main[] = "#include \"test_framework.h\"\n"
                               "int main(int argc, char **argv) {\n"
                               "    RUN_SELECTED_SUITE(alpha);\n"
                               "    RUN_SELECTED_SUITE(beta);\n"
                               "    RUN_SELECTED_SUITE(matrix);\n"
                               "    return 0;\n"
                               "}\n";

static const char tie_alpha[] = "int lib_value(void);\n"
                                "TEST(alpha_uses_lib) {\n"
                                "    return lib_value() == 1 ? 0 : 1;\n"
                                "}\n"
                                "TEST(alpha_plain) {\n"
                                "    return 0;\n"
                                "}\n"
                                "SUITE(alpha) {\n"
                                "    RUN_TEST(alpha_uses_lib);\n"
                                "    RUN_TEST(alpha_plain);\n"
                                "}\n";

static const char tie_beta[] = "TEST(beta_alone) {\n"
                               "    return 0;\n"
                               "}\n"
                               "SUITE(beta) {\n"
                               "    RUN_TEST(beta_alone);\n"
                               "}\n";

/* Tests registered by a macro, as the cbm repro matrices do. */
static const char tie_matrix[] = "int lib_other(void);\n"
                                 "TEST(matrix_one) {\n"
                                 "    return lib_other() == 2 ? 0 : 1;\n"
                                 "}\n"
                                 "#define MATRIX_CASES(X) X(matrix_one)\n"
                                 "#define RUN_ONE(n) RUN_TEST(n);\n"
                                 "SUITE(matrix) {\n"
                                 "    MATRIX_CASES(RUN_ONE)\n"
                                 "}\n";

static const char tie_lib[] = "int lib_value(void) {\n"
                              "    return 1;\n"
                              "}\n"
                              "int lib_other(void) {\n"
                              "    return 2;\n"
                              "}\n";

static bool tie_open_files(tie_fixture_t *fx, const char *const *extra, int extra_count);

static bool tie_open(tie_fixture_t *fx) {
    return tie_open_files(fx, NULL, 0);
}

/* The fixture, plus one more file in the base commit. */
static bool tie_open_with(tie_fixture_t *fx, const char *extra_path, const char *extra) {
    const char *const files[] = {extra_path, extra};
    return tie_open_files(fx, files, 1);
}

/* A directory's canonical form: realpath on POSIX (macOS temp dirs sit behind
 * the /var -> /private/var link), _fullpath on Windows, which has no realpath,
 * with forward slashes like every path the fixture builds from it. */
static bool tie_full_path(char out[PATH_MAX], const char *path) {
#ifdef _WIN32
    return path && _fullpath(out, path, PATH_MAX) != NULL && cbm_normalize_path_sep(out) != NULL;
#else
    return path && realpath(path, out) != NULL;
#endif
}

/* The fixture, plus (path, content) pairs written over it in the base commit. */
static bool tie_open_files(tie_fixture_t *fx, const char *const *extra, int extra_count) {
    memset(fx, 0, sizeof(*fx));
    const char *git = cbm_find_cli("git", cbm_get_home_dir());
    const char *home = th_mktempdir("cbm-ti-engine");
    char real[PATH_MAX];
    if (!git || !home || !tie_full_path(real, home) || strlen(git) >= sizeof(fx->git)) {
        return false;
    }
    snprintf(fx->git, sizeof(fx->git), "%s", git);
    snprintf(fx->home, sizeof(fx->home), "%s", real);
    snprintf(fx->repo, sizeof(fx->repo), "%s/repo", real);
    snprintf(fx->work, sizeof(fx->work), "%s/work", real);
    snprintf(fx->log, sizeof(fx->log), "%s/git.log", real);
    const char *init[] = {"-c",      "init.templateDir=",     "init",
                          "--quiet", "--initial-branch=main", NULL};
    const char *topic[] = {"checkout", "--quiet", "-b", "topic", NULL};
    bool ok = th_mkdir_p(fx->repo) == 0 && th_mkdir_p(fx->work) == 0 &&
              chmod(fx->work, 0700) == 0 && tie_git(fx, init) &&
              tie_write(fx, ".codebase-memory.json", tie_config) &&
              tie_write(fx, "tests/test_main.c", tie_main) &&
              tie_write(fx, "tests/test_alpha.c", tie_alpha) &&
              tie_write(fx, "tests/test_beta.c", tie_beta) &&
              tie_write(fx, "tests/test_matrix.c", tie_matrix) &&
              tie_write(fx, "src/lib.c", tie_lib) && tie_write(fx, "Makefile", "all:\n");
    for (int i = 0; ok && i < extra_count; i++) {
        ok = tie_write(fx, extra[2 * i], extra[2 * i + 1]);
    }
    return ok && tie_commit(fx, "base") && tie_git(fx, topic);
}

static void tie_close(tie_fixture_t *fx) {
    (void)th_rmtree(fx->home);
}

/* The answer for the topic branch against main, as JSON (owned); with a
 * team artifact bundle when one is given. */
static char *tie_answer_with(tie_fixture_t *fx, const char *bundle, const char *commit,
                             bool verified) {
    cbm_test_impact_request_t rq = {.repo_root = fx->repo,
                                    .base_ref = "main",
                                    .work_parent = fx->work,
                                    .deadline_ms = cbm_now_ms() + 120000U,
                                    .artifact_dir = bundle,
                                    .artifact_commit = commit,
                                    .artifact_verified = verified,
                                    .platform = "fixture"};
    cbm_test_result_t *out = NULL;
    char diagnostic[512];
    if (cbm_test_impact_run(&rq, &out, diagnostic, sizeof(diagnostic)) != CBM_TEST_IMPACT_OK) {
        printf("  engine: %s\n", diagnostic);
        return NULL;
    }
    if (diagnostic[0]) {
        printf("  engine note: %s\n", diagnostic);
    }
    size_t len = 0;
    const char *json = cbm_test_result_json(out, &len);
    char *copy = json ? strdup(json) : NULL;
    cbm_test_result_free(out);
    return copy;
}

static char *tie_answer(tie_fixture_t *fx) {
    return tie_answer_with(fx, NULL, NULL, false);
}

/* The "reasons" array of one suite's entry in the answer. */
static bool tie_suite_has(const char *json, const char *suite, const char *reason) {
    char key[128];
    snprintf(key, sizeof(key), "\"suite\":\"%s\"", suite);
    const char *at = strstr(json, key);
    if (!at) {
        return false;
    }
    const char *end = strstr(at, "\"suite\":\"");
    end = end && end != at ? end : strstr(at + strlen(key), "\"suite\":\"");
    char want[128];
    snprintf(want, sizeof(want), "\"%s\"", reason);
    const char *hit = strstr(at, want);
    return hit && (!end || hit < end);
}

/* A change the walk follows to one test's suite marks that suite reached;
 * a suite it does not reach is not. */
TEST(test_impact_engine_reached_suites_carry_static) {
    tie_fixture_t fx;
    ASSERT_TRUE(tie_open(&fx));
    ASSERT_TRUE(tie_write(&fx, "src/lib.c",
                          "int lib_value(void) {\n    return 1 + 0;\n}\n"
                          "int lib_other(void) {\n    return 2;\n}\n"));
    ASSERT_TRUE(tie_commit(&fx, "change lib_value"));
    char *json = tie_answer(&fx);
    ASSERT_NOT_NULL(json);
    if (!tie_suite_has(json, "alpha", "STATIC")) {
        printf("  %.600s\n", json);
    }
    ASSERT_TRUE(tie_suite_has(json, "alpha", "STATIC"));
    ASSERT_FALSE(tie_suite_has(json, "beta", "STATIC"));
    ASSERT_FALSE(tie_suite_has(json, "matrix", "STATIC"));
    free(json);
    tie_close(&fx);
    PASS();
}

/* A test a macro registers has no explicit registration: reaching it must
 * still select its suite (whole), as the measured reference does. */
TEST(test_impact_engine_macro_registered_tests_select_their_suite) {
    tie_fixture_t fx;
    ASSERT_TRUE(tie_open(&fx));
    ASSERT_TRUE(tie_write(&fx, "src/lib.c",
                          "int lib_value(void) {\n    return 1;\n}\n"
                          "int lib_other(void) {\n    return 2 + 0;\n}\n"));
    ASSERT_TRUE(tie_commit(&fx, "change lib_other"));
    char *json = tie_answer(&fx);
    ASSERT_NOT_NULL(json);
    if (!tie_suite_has(json, "matrix", "STATIC")) {
        printf("  %.900s\n", json);
    }
    ASSERT_TRUE(tie_suite_has(json, "matrix", "STATIC"));
    ASSERT_FALSE(tie_suite_has(json, "alpha", "STATIC"));
    free(json);
    tie_close(&fx);
    PASS();
}

/* A path a run-all rule matches runs everything; so does a change the
 * test-runner lane cannot read (M-10); an empty change selects nothing. */
TEST(test_impact_engine_rules_unmapped_and_empty) {
    tie_fixture_t fx;
    ASSERT_TRUE(tie_open(&fx));
    char *json = tie_answer(&fx);
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "\"decision\":\"nothing\""));
    free(json);

    ASSERT_TRUE(tie_write(&fx, "Makefile", "all:\n\techo\n"));
    ASSERT_TRUE(tie_commit(&fx, "build change"));
    json = tie_answer(&fx);
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "\"decision\":\"run_all\""));
    ASSERT_NOT_NULL(strstr(json, "RULE_RUN_ALL"));
    free(json);

    const char *reset[] = {"reset", "--quiet", "--hard", "main", NULL};
    ASSERT_TRUE(tie_git(&fx, reset));
    ASSERT_TRUE(tie_write(&fx, "data/table.csv", "a,b\n1,2\n"));
    ASSERT_TRUE(tie_commit(&fx, "runtime data"));
    json = tie_answer(&fx);
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "UNMAPPED_FILE"));
    free(json);
    tie_close(&fx);
    PASS();
}

/* A parse gap hides the edges written in its lines: a helper whose broken
 * line calls the changed function has no CALLS edge to it. The gap's text
 * still names the function, so the helper is reached, and with it the test
 * that calls the helper (per-file PARSE_GAP rule, user decision 2026-10-04). */
TEST(test_impact_engine_parse_gaps_reach_what_they_name) {
    static const char gapped[] = "int lib_value(void);\n"
                                 "static int helper(void) {\n"
                                 "    int x = 0;\n"
                                 "    x = (lib_value() ;\n"
                                 "    return x;\n"
                                 "}\n"
                                 "TEST(gap_case) {\n"
                                 "    return helper();\n"
                                 "}\n"
                                 "SUITE(beta) {\n"
                                 "    RUN_TEST(gap_case);\n"
                                 "}\n";
    tie_fixture_t fx;
    ASSERT_TRUE(tie_open_with(&fx, "tests/test_beta.c", gapped));
    ASSERT_TRUE(tie_write(&fx, "src/lib.c",
                          "int lib_value(void) {\n    return 1 + 0;\n}\n"
                          "int lib_other(void) {\n    return 2;\n}\n"));
    ASSERT_TRUE(tie_commit(&fx, "change lib_value"));
    char *json = tie_answer(&fx);
    ASSERT_NOT_NULL(json);
    if (!tie_suite_has(json, "beta", "STATIC")) {
        printf("  %.900s\n", json);
    }
    ASSERT_TRUE(tie_suite_has(json, "beta", "STATIC"));
    free(json);
    tie_close(&fx);
    PASS();
}

/* The base commit's object id, read from the branch ref (never packed here). */
static bool tie_base_sha(tie_fixture_t *fx, char sha[65]) {
    char path[1200];
    snprintf(path, sizeof(path), "%s/.git/refs/heads/main", fx->repo);
    FILE *f = fopen(path, "rb");
    if (!f) {
        return false;
    }
    size_t n = fread(sha, 1, 64, f);
    (void)fclose(f);
    while (n && (sha[n - 1] == '\n' || sha[n - 1] == '\r')) {
        n--;
    }
    sha[n] = '\0';
    return n == 40 || n == 64;
}

/* A coverage map of the base: lib_value runs only under alpha_uses_lib,
 * lib_other only under matrix_one; every setup row is complete and empty. */
static bool tie_coverage(tie_fixture_t *fx, const char *sha, char *dir, size_t cap) {
    snprintf(dir, cap, "%s/coverage", fx->home);
    char meta[1024];
    snprintf(meta, sizeof(meta),
             "{\"format\":1,\"commit\":\"%s\",\"functions\":2,\"tests\":4,"
             "\"incomplete\":0,\"platform\":\"fixture\",\"llvm_profdata\":\"fixture\","
             "\"suites\":[{\"suite\":\"alpha\",\"tests\":2,\"incomplete\":0,\"exit\":0},"
             "{\"suite\":\"beta\",\"tests\":1,\"incomplete\":0,\"exit\":0},"
             "{\"suite\":\"matrix\",\"tests\":1,\"incomplete\":0,\"exit\":0}]}\n",
             sha);
    return th_mkdir_p(dir) == 0 &&
           th_write_file(TH_PATH(dir, "functions.tsv"),
                         "0\tsrc/lib.c\tlib_value\n1\tsrc/lib.c\tlib_other\n") == 0 &&
           th_write_file(TH_PATH(dir, "tests.tsv"),
                         "alpha:*\tcomplete\t\t\nalpha:alpha_plain\tcomplete\t\t\n"
                         "alpha:alpha_uses_lib\tcomplete\t\t0\nbeta:*\tcomplete\t\t\n"
                         "beta:beta_alone\tcomplete\t\t\nmatrix:*\tcomplete\t\t\n"
                         "matrix:matrix_one\tcomplete\t\t1\n") == 0 &&
           th_write_file(TH_PATH(dir, "meta.json"), meta) == 0;
}

/* The team artifact: the merge base's bundle, verified by the caller, is the
 * graph base and its admitted map narrows a suite to the tests that ran the
 * changed function; the same bundle unverified is ignored, and without a
 * bundle every suite stays whole. */
TEST(test_impact_engine_admitted_coverage_narrows_only_when_verified) {
    tie_fixture_t fx;
    ASSERT_TRUE(tie_open(&fx));
    char sha[65];
    char coverage[1200];
    char bundle[1200];
    ASSERT_TRUE(tie_base_sha(&fx, sha));
    ASSERT_TRUE(tie_coverage(&fx, sha, coverage, sizeof(coverage)));
    snprintf(bundle, sizeof(bundle), "%s/bundle", fx.home);
    cbm_test_impact_publish_t publish = {.repo_root = fx.repo,
                                         .work_parent = fx.work,
                                         .out_dir = bundle,
                                         .coverage_dir = coverage,
                                         .observed_at = (int64_t)time(NULL),
                                         .platform = "fixture",
                                         .deadline_ms = cbm_now_ms() + 120000U};
    char diagnostic[512] = "";
    cbm_test_impact_status_t published =
        cbm_test_impact_publish(&publish, diagnostic, sizeof(diagnostic));
    if (published != CBM_TEST_IMPACT_OK) {
        printf("  publish: %s\n", diagnostic);
    }
    ASSERT_EQ(published, CBM_TEST_IMPACT_OK);
    ASSERT_TRUE(tie_write(&fx, "src/lib.c",
                          "int lib_value(void) {\n    return 1 + 0;\n}\n"
                          "int lib_other(void) {\n    return 2;\n}\n"));
    ASSERT_TRUE(tie_commit(&fx, "change lib_value"));

    char *plain = tie_answer(&fx);
    char *verified = tie_answer_with(&fx, bundle, sha, true);
    char *unverified = tie_answer_with(&fx, bundle, sha, false);
    ASSERT_NOT_NULL(plain);
    ASSERT_NOT_NULL(verified);
    ASSERT_NOT_NULL(unverified);
    if (!strstr(verified, "\"mode\":\"tests\",\"suite\":\"alpha\"")) {
        printf("  %.1200s\n", verified);
    }
    /* Narrowed: alpha to the one test that ran lib_value; beta not at all. */
    ASSERT_NOT_NULL(strstr(verified, "\"mode\":\"tests\",\"suite\":\"alpha\""));
    ASSERT_NOT_NULL(strstr(verified, "\"test\":\"alpha_uses_lib\""));
    ASSERT_NULL(strstr(verified, "\"test\":\"alpha_plain\""));
    ASSERT_NULL(strstr(verified, "\"suite\":\"beta\""));
    /* Unverified and absent: no narrowing. */
    ASSERT_NOT_NULL(strstr(unverified, "\"mode\":\"whole\",\"suite\":\"beta\""));
    ASSERT_NOT_NULL(strstr(unverified, "SOURCE_UNVERIFIED"));
    ASSERT_NOT_NULL(strstr(plain, "\"mode\":\"whole\",\"suite\":\"beta\""));
    free(plain);
    free(verified);
    free(unverified);
    tie_close(&fx);
    PASS();
}

/* A changed function the map does not know (here: not in the map's image,
 * as platform code would not be) reaches the coverage of its nearest mapped
 * callers, so the tests that ran lib_value are selected BY COVERAGE when only
 * lib_win changed. */
static const char tie_lib_win[] = "int lib_win(void) {\n"
                                  "    return 0;\n"
                                  "}\n"
                                  "int lib_value(void) {\n"
                                  "    return 1 + lib_win();\n"
                                  "}\n"
                                  "int lib_other(void) {\n"
                                  "    return 2;\n"
                                  "}\n";

TEST(test_impact_engine_coverage_reaches_unmapped_code_through_mapped_callers) {
    tie_fixture_t fx;
    ASSERT_TRUE(tie_open_with(&fx, "src/lib.c", tie_lib_win));
    char sha[65];
    char coverage[1200];
    char bundle[1200];
    ASSERT_TRUE(tie_base_sha(&fx, sha));
    ASSERT_TRUE(tie_coverage(&fx, sha, coverage, sizeof(coverage)));
    snprintf(bundle, sizeof(bundle), "%s/bundle", fx.home);
    cbm_test_impact_publish_t publish = {.repo_root = fx.repo,
                                         .work_parent = fx.work,
                                         .out_dir = bundle,
                                         .coverage_dir = coverage,
                                         .observed_at = (int64_t)time(NULL),
                                         .platform = "fixture",
                                         .deadline_ms = cbm_now_ms() + 120000U};
    char diagnostic[512] = "";
    ASSERT_EQ(cbm_test_impact_publish(&publish, diagnostic, sizeof(diagnostic)),
              CBM_TEST_IMPACT_OK);
    ASSERT_TRUE(tie_write(&fx, "src/lib.c",
                          "int lib_win(void) {\n    return 0 + 0;\n}\n"
                          "int lib_value(void) {\n    return 1 + lib_win();\n}\n"
                          "int lib_other(void) {\n    return 2;\n}\n"));
    ASSERT_TRUE(tie_commit(&fx, "change lib_win only"));
    char *verified = tie_answer_with(&fx, bundle, sha, true);
    ASSERT_NOT_NULL(verified);
    const char *covered = "\"id\":\"alpha:alpha_uses_lib\",\"line\":2,\"reasons\":[\"COVERAGE\"";
    if (!strstr(verified, covered)) {
        printf("  %.1200s\n", verified);
    }
    ASSERT_NOT_NULL(strstr(verified, covered));
    ASSERT_NULL(strstr(verified, "\"test\":\"alpha_plain\""));
    free(verified);
    tie_close(&fx);
    PASS();
}

/* One test written once per #if branch is one graph node (graph_buffer.c
 * "Definition variants"): the engine maps it as the graph counts it, never as
 * an ambiguous name. */
static const char tie_alpha_variants[] = "int lib_value(void);\n"
                                         "#ifdef ALPHA_WIDE\n"
                                         "TEST(alpha_uses_lib) {\n"
                                         "    return lib_value() == 1 ? 0 : 1;\n"
                                         "}\n"
                                         "#else\n"
                                         "TEST(alpha_uses_lib) {\n"
                                         "    return lib_value() > 0 ? 0 : 1;\n"
                                         "}\n"
                                         "#endif\n"
                                         "TEST(alpha_plain) {\n"
                                         "    return 0;\n"
                                         "}\n"
                                         "SUITE(alpha) {\n"
                                         "    RUN_TEST(alpha_uses_lib);\n"
                                         "    RUN_TEST(alpha_plain);\n"
                                         "}\n";

TEST(test_impact_engine_variant_tests_are_one_test) {
    tie_fixture_t fx;
    ASSERT_TRUE(tie_open_with(&fx, "tests/test_alpha.c", tie_alpha_variants));
    ASSERT_TRUE(tie_write(&fx, "src/lib.c",
                          "int lib_value(void) {\n    return 1 + 0;\n}\n"
                          "int lib_other(void) {\n    return 2;\n}\n"));
    ASSERT_TRUE(tie_commit(&fx, "change lib_value"));
    char *json = tie_answer(&fx);
    ASSERT_NOT_NULL(json);
    if (!tie_suite_has(json, "alpha", "STATIC") || tie_suite_has(json, "alpha", "UNMAPPED")) {
        printf("  %.900s\n", json);
    }
    ASSERT_TRUE(tie_suite_has(json, "alpha", "STATIC"));
    ASSERT_FALSE(tie_suite_has(json, "alpha", "UNMAPPED"));
    free(json);
    tie_close(&fx);
    PASS();
}

/* A test that starts a program runs code no CALLS edge leads to; the program
 * runs main, and main reaches everything. A change that reaches the product's
 * main reaches the tests that spawn; one that does not, does not. */
static const char tie_spawn_main[] = "int lib_value(void);\n"
                                     "int main(void) {\n"
                                     "    return lib_value();\n"
                                     "}\n";
static const char tie_spawn_beta[] = "#include <stdlib.h>\n"
                                     "TEST(beta_alone) {\n"
                                     "    return system(\"./app --version\");\n"
                                     "}\n"
                                     "SUITE(beta) {\n"
                                     "    RUN_TEST(beta_alone);\n"
                                     "}\n";

TEST(test_impact_engine_spawning_tests_follow_main) {
    const char *const extra[] = {"src/main.c", tie_spawn_main, "tests/test_beta.c", tie_spawn_beta};
    tie_fixture_t fx;
    ASSERT_TRUE(tie_open_files(&fx, extra, 2));
    ASSERT_TRUE(tie_write(&fx, "src/lib.c",
                          "int lib_value(void) {\n    return 1 + 0;\n}\n"
                          "int lib_other(void) {\n    return 2;\n}\n"));
    ASSERT_TRUE(tie_commit(&fx, "change lib_value"));
    char *json = tie_answer(&fx);
    ASSERT_NOT_NULL(json);
    if (!tie_suite_has(json, "beta", "STATIC")) {
        printf("  %.900s\n", json);
    }
    ASSERT_TRUE(tie_suite_has(json, "alpha", "STATIC"));
    ASSERT_TRUE(tie_suite_has(json, "beta", "STATIC"));
    ASSERT_FALSE(tie_suite_has(json, "matrix", "STATIC"));
    free(json);

    /* lib_other: main never calls it, so the spawning test is not reached. */
    ASSERT_TRUE(tie_write(&fx, "src/lib.c",
                          "int lib_value(void) {\n    return 1;\n}\n"
                          "int lib_other(void) {\n    return 2 + 0;\n}\n"));
    ASSERT_TRUE(tie_commit(&fx, "change lib_other instead"));
    json = tie_answer(&fx);
    ASSERT_NOT_NULL(json);
    ASSERT_TRUE(tie_suite_has(json, "matrix", "STATIC"));
    ASSERT_FALSE(tie_suite_has(json, "beta", "STATIC"));
    free(json);
    tie_close(&fx);
    PASS();
}

SUITE(test_impact_engine) {
    RUN_TEST(test_impact_engine_variant_tests_are_one_test);
    RUN_TEST(test_impact_engine_spawning_tests_follow_main);
    RUN_TEST(test_impact_engine_reached_suites_carry_static);
    RUN_TEST(test_impact_engine_macro_registered_tests_select_their_suite);
    RUN_TEST(test_impact_engine_rules_unmapped_and_empty);
    RUN_TEST(test_impact_engine_parse_gaps_reach_what_they_name);
    RUN_TEST(test_impact_engine_admitted_coverage_narrows_only_when_verified);
    RUN_TEST(test_impact_engine_coverage_reaches_unmapped_code_through_mapped_callers);
}
