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
#include "mcp/test_impact_engine_internal.h"
#include "foundation/mem_core.h"
#include "store/store_resolver.h"
#include <sqlite3.h>

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

/* The engine's diagnostic note of the last tie_answer_with call. */
static char tie_last_note[512];

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
    snprintf(tie_last_note, sizeof(tie_last_note), "%s", diagnostic);
    size_t len = 0;
    const char *json = cbm_test_result_json(out, &len);
    char *copy = json ? strdup(json) : NULL;
    cbm_test_result_free(out);
    return copy;
}

static char *tie_answer(tie_fixture_t *fx) {
    return tie_answer_with(fx, NULL, NULL, false);
}

/* The answer, plus in *vetoed the selection the runner veto discarded (NULL
 * when the engine wrote none). */
static char *tie_answer_vetoed(tie_fixture_t *fx, char **vetoed) {
    cbm_test_result_t *discarded = NULL;
    cbm_test_impact_request_t rq = {.repo_root = fx->repo,
                                    .base_ref = "main",
                                    .work_parent = fx->work,
                                    .deadline_ms = cbm_now_ms() + 120000U,
                                    .platform = "fixture",
                                    .vetoed_out = &discarded};
    cbm_test_result_t *out = NULL;
    char diagnostic[512];
    *vetoed = NULL;
    if (cbm_test_impact_run(&rq, &out, diagnostic, sizeof(diagnostic)) != CBM_TEST_IMPACT_OK) {
        printf("  engine: %s\n", diagnostic);
        return NULL;
    }
    size_t len = 0;
    const char *json = cbm_test_result_json(out, &len);
    const char *masked = discarded ? cbm_test_result_json(discarded, &len) : NULL;
    char *copy = json ? strdup(json) : NULL;
    *vetoed = masked ? strdup(masked) : NULL;
    cbm_test_result_free(out);
    cbm_test_result_free(discarded);
    return copy;
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
    ASSERT_NOT_NULL(strstr(json, "\"run_all_rules\":[]"));
    free(json);

    ASSERT_TRUE(tie_write(&fx, "Makefile", "all:\n\techo\n"));
    ASSERT_TRUE(tie_commit(&fx, "build change"));
    json = tie_answer(&fx);
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "\"decision\":\"run_all\""));
    ASSERT_NOT_NULL(strstr(json, "RULE_RUN_ALL"));
    /* The answer names the rule that ran everything (the repository rule wins). */
    ASSERT_NOT_NULL(strstr(json, "\"run_all_rules\":[\"build\"]"));
    free(json);

    const char *reset[] = {"reset", "--quiet", "--hard", "main", NULL};
    ASSERT_TRUE(tie_git(&fx, reset));
    ASSERT_TRUE(tie_write(&fx, "data/table.csv", "a,b\n1,2\n"));
    ASSERT_TRUE(tie_commit(&fx, "runtime data"));
    json = tie_answer(&fx);
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "UNMAPPED_FILE"));
    ASSERT_NOT_NULL(strstr(json, "\"run_all_rules\":[]"));
    free(json);
    tie_close(&fx);
    PASS();
}

/* A referencing_tests rule keyed by the whole path, unreferenced: ignore. A
 * changed data file a test names selects that test; one no test names selects
 * nothing — instead of running everything (scripts, packaging and docs that no
 * C test reads cannot change a C test's outcome). */
static const char tie_config_named_data[] =
    "{\"test_impact\":{\"version\":1,\"tests\":{\"presets\":{\"c-cbm\":true}},"
    "\"rules\":[{\"id\":\"build\",\"paths\":[\"Makefile\"],\"action\":\"run_all\"},"
    "{\"id\":\"data\",\"paths\":[\"data/**\"],\"action\":\"referencing_tests\","
    "\"key\":\"path\",\"unreferenced\":\"ignore\"}]}}\n";

static const char tie_beta_reads_data[] = "TEST(beta_reads_table) {\n"
                                          "    const char *path = \"data/named.txt\";\n"
                                          "    return path[0] == 'd' ? 0 : 1;\n"
                                          "}\n"
                                          "SUITE(beta) {\n"
                                          "    RUN_TEST(beta_reads_table);\n"
                                          "}\n";

/* The answer's top-level decision: its keys are written sorted, so the
 * document starts with it (a lane's own "decision" comes later). */
static bool tie_top_decision(const char *json, const char *want) {
    char prefix[64];
    snprintf(prefix, sizeof(prefix), "{\"decision\":\"%s\"", want);
    return json && strncmp(json, prefix, strlen(prefix)) == 0;
}

TEST(test_impact_engine_path_key_selects_naming_tests_or_ignores) {
    tie_fixture_t fx;
    const char *const files[] = {".codebase-memory.json", tie_config_named_data,
                                 "tests/test_beta.c",     tie_beta_reads_data,
                                 "data/named.txt",        "one\n",
                                 "data/other.txt",        "two\n"};
    ASSERT_TRUE(tie_open_files(&fx, files, 4));
    ASSERT_TRUE(tie_write(&fx, "data/other.txt", "two, changed\n"));
    ASSERT_TRUE(tie_commit(&fx, "data no test names"));
    char *json = tie_answer(&fx);
    ASSERT_NOT_NULL(json);
    if (tie_top_decision(json, "run_all")) {
        printf("  %.900s\n", json);
    }
    ASSERT_FALSE(tie_top_decision(json, "run_all"));
    ASSERT_NULL(strstr(json, "RULE_TARGET_UNKNOWN"));
    free(json);

    const char *reset[] = {"reset", "--quiet", "--hard", "main", NULL};
    ASSERT_TRUE(tie_git(&fx, reset));
    ASSERT_TRUE(tie_write(&fx, "data/named.txt", "one, changed\n"));
    ASSERT_TRUE(tie_commit(&fx, "data a test names"));
    json = tie_answer(&fx);
    ASSERT_NOT_NULL(json);
    if (!tie_suite_has(json, "beta", "STATIC")) {
        printf("  named: %.1400s\n", json);
    }
    ASSERT_FALSE(tie_top_decision(json, "run_all"));
    ASSERT_TRUE(tie_suite_has(json, "beta", "STATIC"));
    ASSERT_FALSE(tie_suite_has(json, "alpha", "STATIC"));
    free(json);
    tie_close(&fx);
    PASS();
}

/* ── Child-process roles: the runner's TF_ROLE table ─────────────────
 * The fixture runner dispatches its roles from a table by argv[1]; a change
 * that reaches a role handler selects the tests that spawn the role, not
 * every test (src/mcp/test_impact_engine_reach.c te_role_lift). */
static const char tie_roles_main[] =
    "#include \"test_framework.h\"\n"
    "#include <string.h>\n"
    "int lib_value(void);\n"
    "int lib_other(void);\n"
    "int lib_third(void);\n"
    "static int role_probe(int argc, char **argv) {\n"
    "    return lib_value();\n"
    "}\n"
    "static int role_inner(int argc, char **argv) {\n"
    "    return lib_third();\n"
    "}\n"
    "static int role_outer(int argc, char **argv) {\n"
    "    return role_inner(argc, argv);\n"
    "}\n"
    "static int role_orphan(int argc, char **argv) {\n"
    "    return lib_other();\n"
    "}\n"
    "typedef struct { const char *literal; int (*run)(int, char **); } tf_role_t;\n"
    "#define TF_ROLE(phase, literal, handler) {literal, handler}\n"
    "static const tf_role_t tf_roles[] = {\n"
    "    TF_ROLE(0, \"__fx_probe_role\", role_probe),\n"
    "    TF_ROLE(0, \"__fx_inner_role\", role_inner),\n"
    "    TF_ROLE(0, \"__fx_outer_role\", role_outer),\n"
    "    TF_ROLE(0, \"__fx_orphan_role\", role_orphan),\n"
    "};\n"
    "int main(int argc, char **argv) {\n"
    "    for (int i = 0; argc > 1 && i < 4; i++) {\n"
    "        if (strcmp(argv[1], tf_roles[i].literal) == 0) {\n"
    "            return tf_roles[i].run(argc, argv);\n"
    "        }\n"
    "    }\n"
    "    RUN_SELECTED_SUITE(alpha);\n"
    "    RUN_SELECTED_SUITE(beta);\n"
    "    RUN_SELECTED_SUITE(matrix);\n"
    "    return 0;\n"
    "}\n";

/* beta spawns the probe role directly, the outer and inner roles directly,
 * and the probe role once more through a helper. */
static const char tie_roles_beta[] = "static int spawn_probe(void) {\n"
                                     "    const char *argv1 = \"__fx_probe_role\";\n"
                                     "    return argv1[0] == '_' ? 0 : 1;\n"
                                     "}\n"
                                     "TEST(beta_spawns_probe) {\n"
                                     "    const char *argv1 = \"__fx_probe_role\";\n"
                                     "    return argv1[0] == '_' ? 0 : 1;\n"
                                     "}\n"
                                     "TEST(beta_spawns_outer) {\n"
                                     "    const char *argv1 = \"__fx_outer_role\";\n"
                                     "    return argv1[0] == '_' ? 0 : 1;\n"
                                     "}\n"
                                     "TEST(beta_spawns_inner) {\n"
                                     "    const char *argv1 = \"__fx_inner_role\";\n"
                                     "    return argv1[0] == '_' ? 0 : 1;\n"
                                     "}\n"
                                     "TEST(beta_plain) {\n"
                                     "    return 0;\n"
                                     "}\n"
                                     "SUITE(beta) {\n"
                                     "    RUN_TEST(beta_spawns_probe);\n"
                                     "    RUN_TEST(beta_spawns_outer);\n"
                                     "    RUN_TEST(beta_spawns_inner);\n"
                                     "    RUN_TEST(beta_plain);\n"
                                     "}\n";

/* matrix reaches the probe role only through a helper that names it. */
static const char tie_roles_matrix[] = "int spawn_probe_helper(void) {\n"
                                       "    const char *argv1 = \"__fx_probe_role\";\n"
                                       "    return argv1[0] == '_' ? 0 : 1;\n"
                                       "}\n"
                                       "TEST(matrix_via_helper) {\n"
                                       "    return spawn_probe_helper();\n"
                                       "}\n"
                                       "SUITE(matrix) {\n"
                                       "    RUN_TEST(matrix_via_helper);\n"
                                       "}\n";

static const char tie_roles_lib[] = "int lib_value(void) {\n"
                                    "    return 1;\n"
                                    "}\n"
                                    "int lib_other(void) {\n"
                                    "    return 2;\n"
                                    "}\n"
                                    "int lib_third(void) {\n"
                                    "    return 3;\n"
                                    "}\n";

static bool tie_open_roles(tie_fixture_t *fx) {
    const char *const files[] = {"tests/test_main.c", tie_roles_main,        "tests/test_beta.c",
                                 tie_roles_beta,      "tests/test_matrix.c", tie_roles_matrix,
                                 "src/lib.c",         tie_roles_lib};
    return tie_open_files(fx, files, 4);
}

/* A change reaching a role handler selects the tests that spawn the role
 * (directly, or through a helper that names it) — not every test. */
TEST(test_impact_engine_role_reach_selects_its_spawners) {
    tie_fixture_t fx;
    ASSERT_TRUE(tie_open_roles(&fx));
    ASSERT_TRUE(tie_write(&fx, "src/lib.c",
                          "int lib_value(void) {\n    return 1 + 0;\n}\n"
                          "int lib_other(void) {\n    return 2;\n}\n"
                          "int lib_third(void) {\n    return 3;\n}\n"));
    ASSERT_TRUE(tie_commit(&fx, "change lib_value"));
    char *json = tie_answer(&fx);
    ASSERT_NOT_NULL(json);
    if (tie_top_decision(json, "run_all") || !tie_suite_has(json, "beta", "STATIC")) {
        printf("  %.1600s\n", json);
    }
    ASSERT_FALSE(tie_top_decision(json, "run_all"));
    ASSERT_TRUE(tie_suite_has(json, "alpha", "STATIC"));  /* calls lib_value itself */
    ASSERT_TRUE(tie_suite_has(json, "beta", "STATIC"));   /* spawns the probe role */
    ASSERT_TRUE(tie_suite_has(json, "matrix", "STATIC")); /* through its helper */
    free(json);
    tie_close(&fx);
    PASS();
}

/* A handler that calls another runs it in its own child: a change reaching
 * the inner handler selects the outer role's spawners as well. */
TEST(test_impact_engine_chained_roles_select_both_spawners) {
    tie_fixture_t fx;
    ASSERT_TRUE(tie_open_roles(&fx));
    ASSERT_TRUE(tie_write(&fx, "src/lib.c",
                          "int lib_value(void) {\n    return 1;\n}\n"
                          "int lib_other(void) {\n    return 2;\n}\n"
                          "int lib_third(void) {\n    return 3 + 0;\n}\n"));
    ASSERT_TRUE(tie_commit(&fx, "change lib_third"));
    char *json = tie_answer(&fx);
    ASSERT_NOT_NULL(json);
    if (tie_top_decision(json, "run_all")) {
        printf("  %.1600s\n", json);
    }
    ASSERT_FALSE(tie_top_decision(json, "run_all"));
    ASSERT_TRUE(tie_suite_has(json, "beta", "STATIC"));
    ASSERT_FALSE(tie_suite_has(json, "alpha", "STATIC"));
    free(json);
    tie_close(&fx);
    PASS();
}

/* Fail safe: a reached role no test names runs everything, and so does a
 * change reaching main by any other path. */
TEST(test_impact_engine_unspawned_role_and_main_still_run_all) {
    tie_fixture_t fx;
    ASSERT_TRUE(tie_open_roles(&fx));
    ASSERT_TRUE(tie_write(&fx, "src/lib.c",
                          "int lib_value(void) {\n    return 1;\n}\n"
                          "int lib_other(void) {\n    return 2 + 0;\n}\n"
                          "int lib_third(void) {\n    return 3;\n}\n"));
    ASSERT_TRUE(tie_commit(&fx, "change lib_other"));
    char *vetoed = NULL;
    char *json = tie_answer_vetoed(&fx, &vetoed);
    ASSERT_NOT_NULL(json);
    ASSERT_TRUE(tie_top_decision(json, "run_all"));
    ASSERT_NOT_NULL(strstr(json, "RUNNER_REACHED"));
    /* The veto is the one reason: CI also gets the selection it discarded. */
    ASSERT_NOT_NULL(vetoed);
    ASSERT_FALSE(tie_top_decision(vetoed, "run_all"));
    ASSERT_NULL(strstr(vetoed, "RUNNER_REACHED"));
    free(vetoed);
    free(json);

    const char *reset[] = {"reset", "--quiet", "--hard", "main", NULL};
    ASSERT_TRUE(tie_git(&fx, reset));
    ASSERT_TRUE(tie_write(&fx, "tests/test_main.c",
                          "#include \"test_framework.h\"\n"
                          "int lib_value(void);\n"
                          "static int worker(int argc, char **argv) {\n"
                          "    return argc > 9 ? lib_value() : -1;\n"
                          "}\n"
                          "int main(int argc, char **argv) {\n"
                          "    if (worker(argc, argv) >= 0) {\n"
                          "        return 0;\n"
                          "    }\n"
                          "    RUN_SELECTED_SUITE(alpha);\n"
                          "    RUN_SELECTED_SUITE(beta);\n"
                          "    RUN_SELECTED_SUITE(matrix);\n"
                          "    return 0;\n"
                          "}\n"));
    ASSERT_TRUE(tie_commit(&fx, "a role main calls directly"));
    const char *base[] = {"branch", "--quiet", "-f", "main", "HEAD", NULL};
    ASSERT_TRUE(tie_git(&fx, base));
    ASSERT_TRUE(tie_write(&fx, "src/lib.c",
                          "int lib_value(void) {\n    return 1 + 0;\n}\n"
                          "int lib_other(void) {\n    return 2;\n}\n"
                          "int lib_third(void) {\n    return 3;\n}\n"));
    ASSERT_TRUE(tie_commit(&fx, "change lib_value"));
    json = tie_answer(&fx);
    ASSERT_NOT_NULL(json);
    ASSERT_TRUE(tie_top_decision(json, "run_all"));
    ASSERT_NOT_NULL(strstr(json, "RUNNER_REACHED"));
    /* The note names the way in: main, the role it calls, the changed code. */
    ASSERT_NOT_NULL(strstr(tie_last_note, "test_main.main -> test_main.worker -> lib.lib_value"));
    free(json);
    tie_close(&fx);
    PASS();
}

/* The role fixture with a second, never-dispatched literal for the orphan role
 * and `beta` as given; the answer for a change that reaches only that role
 * (NULL on a fixture failure). */
static char *tie_answer_orphan_switch(tie_fixture_t *fx, const char *beta) {
    const char *anchor = "    TF_ROLE(0, \"__fx_orphan_role\", role_orphan),\n";
    const char *at = strstr(tie_roles_main, anchor);
    size_t head = at ? (size_t)(at - tie_roles_main) + strlen(anchor) : 0;
    char runner[4096];
    int n = snprintf(runner, sizeof(runner),
                     "%.*s    TF_ROLE(9, \"__FX_ORPHAN_SWITCH\", role_orphan),\n%s", (int)head,
                     tie_roles_main, tie_roles_main + head);
    const char *base[] = {"branch", "--quiet", "-f", "main", "HEAD", NULL};
    if (!at || n <= 0 || (size_t)n >= sizeof(runner) || !tie_open_roles(fx) ||
        !tie_write(fx, "tests/test_main.c", runner) || !tie_write(fx, "tests/test_beta.c", beta) ||
        !tie_commit(fx, "a second literal for the orphan role") || !tie_git(fx, base) ||
        !tie_write(fx, "src/lib.c",
                   "int lib_value(void) {\n    return 1;\n}\n"
                   "int lib_other(void) {\n    return 2 + 0;\n}\n"
                   "int lib_third(void) {\n    return 3;\n}\n") ||
        !tie_commit(fx, "change lib_other")) {
        return NULL;
    }
    char *json = tie_answer(fx);
    if (json && (strstr(json, "RUNNER_REACHED") || !tie_suite_has(json, "beta", "STATIC"))) {
        printf("  note: %s\n  %.1600s\n", tie_last_note, json);
    }
    return json;
}

/* A handler with two literals (the index worker: its argv flag and the switch
 * that lets production code start it): a test naming either one is a spawner,
 * and the unnamed one alone vetoes nothing. */
TEST(test_impact_engine_role_with_two_literals_selects_both_spawners) {
    tie_fixture_t fx;
    char *json = tie_answer_orphan_switch(&fx, "TEST(beta_sets_switch) {\n"
                                               "    const char *name = \"__FX_ORPHAN_SWITCH\";\n"
                                               "    return name[0] == '_' ? 0 : 1;\n"
                                               "}\n"
                                               "TEST(beta_plain) {\n    return 0;\n}\n"
                                               "SUITE(beta) {\n"
                                               "    RUN_TEST(beta_sets_switch);\n"
                                               "    RUN_TEST(beta_plain);\n"
                                               "}\n");
    ASSERT_NOT_NULL(json);
    ASSERT_NULL(strstr(json, "RUNNER_REACHED"));
    ASSERT_TRUE(tie_suite_has(json, "beta", "STATIC"));
    free(json);
    tie_close(&fx);
    PASS();
}

/* The literal behind a #define (the blocking-git marker): the tests that use
 * the macro are the spawners. */
TEST(test_impact_engine_role_literal_behind_a_define_selects_its_users) {
    tie_fixture_t fx;
    char *json = tie_answer_orphan_switch(&fx, "#define BETA_SWITCH \"__FX_ORPHAN_SWITCH\"\n"
                                               "TEST(beta_sets_switch) {\n"
                                               "    const char *name = BETA_SWITCH;\n"
                                               "    return name[0] == '_' ? 0 : 1;\n"
                                               "}\n"
                                               "TEST(beta_plain) {\n    return 0;\n}\n"
                                               "SUITE(beta) {\n"
                                               "    RUN_TEST(beta_sets_switch);\n"
                                               "    RUN_TEST(beta_plain);\n"
                                               "}\n");
    ASSERT_NOT_NULL(json);
    ASSERT_NULL(strstr(json, "RUNNER_REACHED"));
    ASSERT_TRUE(tie_suite_has(json, "beta", "STATIC"));
    free(json);
    tie_close(&fx);
    PASS();
}

/* A macro that names that macro is followed too. A chain longer than the scan
 * follows runs everything: the tests that use its last macro are unknown. */
TEST(test_impact_engine_role_literal_behind_a_macro_chain) {
    tie_fixture_t fx;
    char *json = tie_answer_orphan_switch(&fx, "#define BETA_SWITCH \"__FX_ORPHAN_SWITCH\"\n"
                                               "#define BETA_ALIAS BETA_SWITCH\n"
                                               "TEST(beta_sets_switch) {\n"
                                               "    const char *name = BETA_ALIAS;\n"
                                               "    return name[0] == '_' ? 0 : 1;\n"
                                               "}\n"
                                               "TEST(beta_plain) {\n    return 0;\n}\n"
                                               "SUITE(beta) {\n"
                                               "    RUN_TEST(beta_sets_switch);\n"
                                               "    RUN_TEST(beta_plain);\n"
                                               "}\n");
    ASSERT_NOT_NULL(json);
    ASSERT_NULL(strstr(json, "RUNNER_REACHED"));
    ASSERT_TRUE(tie_suite_has(json, "beta", "STATIC"));
    free(json);
    tie_close(&fx);

    json = tie_answer_orphan_switch(&fx, "#define BETA_M0 \"__FX_ORPHAN_SWITCH\"\n"
                                         "#define BETA_M1 BETA_M0\n"
                                         "#define BETA_M2 BETA_M1\n"
                                         "#define BETA_M3 BETA_M2\n"
                                         "#define BETA_M4 BETA_M3\n"
                                         "#define BETA_M5 BETA_M4\n"
                                         "TEST(beta_sets_switch) {\n"
                                         "    const char *name = BETA_M5;\n"
                                         "    return name[0] == '_' ? 0 : 1;\n"
                                         "}\n"
                                         "TEST(beta_plain) {\n    return 0;\n}\n"
                                         "SUITE(beta) {\n"
                                         "    RUN_TEST(beta_sets_switch);\n"
                                         "    RUN_TEST(beta_plain);\n"
                                         "}\n");
    ASSERT_NOT_NULL(json);
    ASSERT_TRUE(tie_top_decision(json, "run_all"));
    ASSERT_NOT_NULL(strstr(json, "SELECTION_UNAVAILABLE"));
    free(json);
    tie_close(&fx);
    PASS();
}

/* `src` with `insert` placed after `anchor` (which must occur in it) into
 * `out`; false when it does not fit. */
static bool tie_insert(char *out, size_t cap, const char *src, const char *anchor,
                       const char *insert) {
    const char *at = strstr(src, anchor);
    if (!at) {
        return false;
    }
    size_t head = (size_t)(at - src) + strlen(anchor);
    int n = snprintf(out, cap, "%.*s%s%s", (int)head, src, insert, src + head);
    return n > 0 && (size_t)n < cap;
}

static bool tie_runner_with(char *out, size_t cap, const char *anchor, const char *insert) {
    return tie_insert(out, cap, tie_roles_main, anchor, insert);
}

/* The role fixture's runner with a suite of its own (`body`, placed after the
 * role table), which main runs as the real runner runs runner_isolation. */
static bool tie_runner_with_suite(char *out, size_t cap, const char *body) {
    char first[4096];
    return tie_runner_with(first, sizeof(first), "};\n", body) &&
           tie_insert(out, cap, first, "    RUN_SELECTED_SUITE(matrix);\n",
                      "    RUN_SELECTED_SUITE(runner_isolation);\n");
}

/* The role fixture with `runner` as its base, then a change to lib_value or
 * lib_other; the answer (NULL on a fixture failure). */
static char *tie_answer_runner(tie_fixture_t *fx, const char *runner, bool other) {
    const char *base[] = {"branch", "--quiet", "-f", "main", "HEAD", NULL};
    if (!tie_open_roles(fx) || !tie_write(fx, "tests/test_main.c", runner) ||
        !tie_commit(fx, "the runner under test") || !tie_git(fx, base) ||
        !tie_write(fx, "src/lib.c",
                   other ? "int lib_value(void) {\n    return 1;\n}\n"
                           "int lib_other(void) {\n    return 2 + 0;\n}\n"
                           "int lib_third(void) {\n    return 3;\n}\n"
                         : "int lib_value(void) {\n    return 1 + 0;\n}\n"
                           "int lib_other(void) {\n    return 2;\n}\n"
                           "int lib_third(void) {\n    return 3;\n}\n") ||
        !tie_commit(fx, other ? "change lib_other" : "change lib_value")) {
        return NULL;
    }
    char *json = tie_answer(fx);
    if (json) {
        printf("  note: %s\n", tie_last_note);
    }
    return json;
}

/* main dispatches through the role table, so no edge leads from main to a
 * handler: an entry the engine cannot read, or whose handler is no single
 * function, hides that role's spawners, and every change runs everything. */
TEST(test_impact_engine_unusable_role_entry_runs_all) {
    const char *anchor = "    TF_ROLE(0, \"__fx_orphan_role\", role_orphan),\n";
    const char *const entries[] = {"    TF_ROLE(0, FX_GHOST_LITERAL, role_orphan),\n",
                                   "    TF_ROLE(0, \"__fx_ghost_role\", role_ghost),\n"};
    for (int k = 0; k < 2; k++) {
        char runner[4096];
        ASSERT_TRUE(tie_runner_with(runner, sizeof(runner), anchor, entries[k]));
        tie_fixture_t fx;
        char *json = tie_answer_runner(&fx, runner, false);
        ASSERT_NOT_NULL(json);
        ASSERT_TRUE(tie_top_decision(json, "run_all"));
        ASSERT_NOT_NULL(strstr(json, "SELECTION_UNAVAILABLE"));
        free(json);
        tie_close(&fx);
    }
    PASS();
}

/* A handler first reached by the spawn lift (it starts the product, whose
 * main the change reaches) is lifted like any other: its literal is named by
 * no test here, so everything runs. */
TEST(test_impact_engine_role_reached_by_the_spawn_lift_is_lifted) {
    static const char product[] = "int lib_other(void);\n"
                                  "int main(void) {\n"
                                  "    return lib_other();\n"
                                  "}\n";
    const char *call = "    return lib_other();\n";
    const char *at = strstr(tie_roles_main, call);
    ASSERT_NOT_NULL(at);
    char runner[4096];
    int n = snprintf(runner, sizeof(runner),
                     "#include <stdlib.h>\n%.*s    return system(\"./app --version\");\n%s",
                     (int)(at - tie_roles_main), tie_roles_main, at + strlen(call));
    ASSERT_TRUE(n > 0 && (size_t)n < sizeof(runner));
    const char *const files[] = {
        "tests/test_main.c",   runner,           "tests/test_beta.c", tie_roles_beta,
        "tests/test_matrix.c", tie_roles_matrix, "src/lib.c",         tie_roles_lib,
        "src/main.c",          product};
    tie_fixture_t fx;
    ASSERT_TRUE(tie_open_files(&fx, files, 5));
    ASSERT_TRUE(tie_write(&fx, "src/lib.c",
                          "int lib_value(void) {\n    return 1;\n}\n"
                          "int lib_other(void) {\n    return 2 + 0;\n}\n"
                          "int lib_third(void) {\n    return 3;\n}\n"));
    ASSERT_TRUE(tie_commit(&fx, "change lib_other"));
    char *json = tie_answer(&fx);
    ASSERT_NOT_NULL(json);
    if (!strstr(json, "RUNNER_REACHED")) {
        printf("  note: %s\n  %.1600s\n", tie_last_note, json);
    }
    ASSERT_NOT_NULL(strstr(json, "RUNNER_REACHED"));
    free(json);
    tie_close(&fx);
    PASS();
}

/* The runner's own use of a role's literal (its helper reading the marker)
 * names no spawner, so a role only the runner names still runs everything;
 * a test case in the runner file that names it is a spawner. */
TEST(test_impact_engine_runner_helper_names_no_spawner) {
    char runner[4096];
    ASSERT_TRUE(
        tie_runner_with_suite(runner, sizeof(runner),
                              "static int runner_reads_marker(void) {\n"
                              "    const char *s = \"__fx_orphan_role\";\n"
                              "    return s[0] == '_' ? 0 : 1;\n"
                              "}\n"
                              "TEST(runner_plain) {\n    return 0;\n}\n"
                              "SUITE(runner_isolation) {\n    RUN_TEST(runner_plain);\n}\n"));
    tie_fixture_t fx;
    char *json = tie_answer_runner(&fx, runner, true);
    ASSERT_NOT_NULL(json);
    ASSERT_TRUE(tie_top_decision(json, "run_all"));
    ASSERT_NOT_NULL(strstr(json, "RUNNER_REACHED"));
    free(json);
    tie_close(&fx);

    ASSERT_TRUE(tie_runner_with_suite(runner, sizeof(runner),
                                      "TEST(runner_spawns_orphan) {\n"
                                      "    const char *s = \"__fx_orphan_role\";\n"
                                      "    return s[0] == '_' ? 0 : 1;\n"
                                      "}\n"
                                      "SUITE(runner_isolation) {\n"
                                      "    RUN_TEST(runner_spawns_orphan);\n}\n"));
    json = tie_answer_runner(&fx, runner, true);
    ASSERT_NOT_NULL(json);
    ASSERT_NULL(strstr(json, "RUNNER_REACHED"));
    free(json);
    tie_close(&fx);
    PASS();
}

/* A struct member named like a suite: the extractor can take the macro argument
 * of RUN_SELECTED_SUITE(alpha) for a use of it. A changed member reaches main
 * only through that edge into a Field: no veto. */
TEST(test_impact_engine_field_call_is_no_way_into_main) {
    tie_fixture_t fx;
    ASSERT_TRUE(
        tie_open_with(&fx, "tests/shapes.h",
                      "typedef struct {\n    char alpha[8];\n    int sides;\n} shape_t;\n"));
    ASSERT_TRUE(tie_write(&fx, "tests/shapes.h",
                          "typedef struct {\n    char alpha[16];\n    int sides;\n} shape_t;\n"));
    ASSERT_TRUE(tie_commit(&fx, "widen the member"));
    char *json = tie_answer(&fx);
    ASSERT_NOT_NULL(json);
    if (strstr(json, "RUNNER_REACHED")) {
        printf("  note: %s\n", tie_last_note);
    }
    ASSERT_NULL(strstr(json, "RUNNER_REACHED"));
    free(json);
    tie_close(&fx);
    PASS();
}

/* main may call a role handler itself, as the runner's main calls the
 * blocking-git probe. A reached handler is a sink whose spawners the role lift
 * selects, so that call is no way into main. Here main is reached otherwise
 * only through the edge into the suite-named member (above): no veto. */
TEST(test_impact_engine_reached_role_handler_is_no_way_into_main) {
    static const char shapes[] =
        "typedef struct {\n    char alpha[8];\n    int sides;\n} shape_t;\n";
    const char *anchor = "int main(int argc, char **argv) {\n";
    const char *at = strstr(tie_roles_main, anchor);
    ASSERT_NOT_NULL(at);
    size_t head = (size_t)(at - tie_roles_main) + strlen(anchor);
    char runner[4096];
    int n = snprintf(runner, sizeof(runner),
                     "%.*s    if (argc > 9) {\n        return role_probe(argc, argv);\n    }\n%s",
                     (int)head, tie_roles_main, tie_roles_main + head);
    ASSERT_TRUE(n > 0 && (size_t)n < sizeof(runner));
    const char *const files[] = {"tests/test_main.c",   runner,
                                 "tests/test_beta.c",   tie_roles_beta,
                                 "tests/test_matrix.c", tie_roles_matrix,
                                 "src/lib.c",           tie_roles_lib,
                                 "tests/shapes.h",      shapes};
    tie_fixture_t fx;
    ASSERT_TRUE(tie_open_files(&fx, files, 5));
    ASSERT_TRUE(tie_write(&fx, "tests/shapes.h",
                          "typedef struct {\n    char alpha[16];\n    int sides;\n} shape_t;\n"));
    ASSERT_TRUE(tie_write(&fx, "src/lib.c",
                          "int lib_value(void) {\n    return 1 + 0;\n}\n"
                          "int lib_other(void) {\n    return 2;\n}\n"
                          "int lib_third(void) {\n    return 3;\n}\n"));
    ASSERT_TRUE(tie_commit(&fx, "widen the member, change lib_value"));
    char *json = tie_answer(&fx);
    ASSERT_NOT_NULL(json);
    if (strstr(json, "RUNNER_REACHED")) {
        printf("  note: %s\n", tie_last_note);
    }
    ASSERT_NULL(strstr(json, "RUNNER_REACHED"));
    ASSERT_TRUE(tie_suite_has(json, "beta", "STATIC")); /* spawns the probe role */
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

/* Platform variants of one function (pick_posix.c / pick_win.c) are separate
 * graph nodes sharing a variant_key, and the test's call binds to one of
 * them. A change confined to EITHER variant reaches the test, whichever
 * variant the call bound to: the other platform's build runs the other. */
static const char tie_pick_posix[] = "int pick(void) {\n"
                                     "    return 1;\n"
                                     "}\n";
static const char tie_pick_win[] = "int pick(void) {\n"
                                   "    return 2;\n"
                                   "}\n";
static const char tie_pick_beta[] = "int pick(void);\n"
                                    "TEST(beta_picks) {\n"
                                    "    return pick() > 0 ? 0 : 1;\n"
                                    "}\n"
                                    "SUITE(beta) {\n"
                                    "    RUN_TEST(beta_picks);\n"
                                    "}\n";

TEST(test_impact_engine_platform_variants_reach_each_others_callers) {
    const char *const extra[] = {"src/pick_posix.c",  tie_pick_posix, /* variant A */
                                 "src/pick_win.c",    tie_pick_win,   /* variant B */
                                 "tests/test_beta.c", tie_pick_beta};
    static const char *const changes[][2] = {
        {"src/pick_win.c", "int pick(void) {\n    return 2 + 0;\n}\n"},
        {"src/pick_posix.c", "int pick(void) {\n    return 1 + 0;\n}\n"}};
    const char *reset[] = {"reset", "--quiet", "--hard", "main", NULL};
    tie_fixture_t fx;
    ASSERT_TRUE(tie_open_files(&fx, extra, 3));
    for (int i = 0; i < 2; i++) {
        ASSERT_TRUE(tie_git(&fx, reset));
        ASSERT_TRUE(tie_write(&fx, changes[i][0], changes[i][1]));
        ASSERT_TRUE(tie_commit(&fx, "change one platform variant"));
        char *json = tie_answer(&fx);
        ASSERT_NOT_NULL(json);
        if (!tie_suite_has(json, "beta", "STATIC")) {
            printf("  %s changed: %.900s\n", changes[i][0], json);
        }
        ASSERT_TRUE(tie_suite_has(json, "beta", "STATIC"));
        ASSERT_FALSE(tie_suite_has(json, "alpha", "STATIC"));
        free(json);
    }
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

/* Consumer fixtures deliberately contain no CALLS edges for their withheld
 * calls. Persisted misses are selection uncertainty, never graph evidence. */
typedef struct {
    te_ctx_t c;
    char diagnostic[512];
} tie_miss_fixture_t;

static bool tie_miss_open(tie_miss_fixture_t *f, const char *const *files, int count) {
    memset(f, 0, sizeof(*f));
    cbm_arena_init(&f->c.arena);
    f->c.diagnostic = f->diagnostic;
    f->c.diagnostic_len = sizeof(f->diagnostic);
    f->c.store = cbm_store_open_memory();
    f->c.source = cbm_ti_source_new();
    f->c.model = cbm_test_model_new(cbm_test_conventions_cbm());
    const char *config_dir = th_mktempdir("cbm-ti-miss-config");
    if (!config_dir)
        return false;
    /* Optional absence is represented by a loaded config object. A NULL
     * config is an initialization failure, not the built-in policy. */
    f->c.config = cbm_test_config_load(TH_PATH(config_dir, "absent.json"), true);
    int config_cleanup = th_rmtree(config_dir);
    f->c.policy = cbm_test_policy_new(f->c.config);
    if (config_cleanup != 0)
        return false;
    if (!f->c.store || !f->c.source || !f->c.model || !f->c.policy ||
        cbm_store_upsert_project(f->c.store, "test-impact", "/tmp/test-impact") != CBM_STORE_OK)
        return false;
    f->c.graph = cbm_ti_graph_new(f->c.store, "test-impact");
    if (!f->c.graph)
        return false;
    for (int i = 0; i < count; i++) {
        const char *path = files[2 * i], *source = files[2 * i + 1];
        if (!cbm_ti_source_add(f->c.source, path, source, strlen(source)) ||
            (strncmp(path, "tests/", 6) == 0 &&
             !cbm_test_model_add_source(f->c.model, path, source, strlen(source))))
            return false;
    }
    return cbm_ti_source_finish(f->c.source) && cbm_test_model_finish(f->c.model);
}

static const char *tie_miss_qn(tie_miss_fixture_t *f, const char *file, const char *name) {
    char qn[512];
    int n = snprintf(qn, sizeof(qn), "test-impact.%s.%s", file, name);
    return n > 0 && (size_t)n < sizeof(qn) ? cbm_arena_strdup(&f->c.arena, qn) : NULL;
}

static int64_t tie_miss_node(tie_miss_fixture_t *f, const char *file, const char *name,
                             const char *label, int first, int last) {
    cbm_node_t node = {.project = "test-impact",
                       .label = label,
                       .name = name,
                       .qualified_name = tie_miss_qn(f, file, name),
                       .file_path = file,
                       .start_line = first,
                       .end_line = last};
    return node.qualified_name ? cbm_store_upsert_node(f->c.store, &node) : -1;
}

static cbm_resolve_record_t tie_miss_row(tie_miss_fixture_t *f, const char *file, const char *owner,
                                         const char *spelling) {
    return (cbm_resolve_record_t){.file_path = file,
                                  .scope = tie_miss_qn(f, file, owner),
                                  .relation = "CALLS",
                                  .spelling = spelling,
                                  .start_byte = 1,
                                  .end_byte = 2,
                                  .source_origin = CBM_SOURCE_ORIGIN_RAW,
                                  .result = {.binding = CBM_RESOLVE_BINDING_UNRESOLVED,
                                             .analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                                             .reason = "consumer-fixture-withheld-call"}};
}

static bool tie_miss_seed(tie_miss_fixture_t *f, const char *path, int line) {
    static const char *const added[] = {"return changed_value;"};
    cbm_diff_hunk_t hunk = {.start = line, .count = 1, .added = added, .added_count = 1};
    cbm_ti_change_t change = {.path = path,
                              .whole_file = line == 0,
                              .hunks = line ? &hunk : NULL,
                              .hunk_count = line ? 1 : 0};
    cbm_ti_seed_input_t input = {.changes = &change,
                                 .change_count = 1,
                                 .model = f->c.model,
                                 .source = f->c.source,
                                 .graph = f->c.graph};
    return cbm_ti_seed(&input, &f->c.seeds) == CBM_TI_SEED_OK &&
           cbm_store_read_scope_open(f->c.store, NULL, NULL, &f->c.scope) == CBM_STORE_OK;
}

static bool tie_miss_selected(const tie_miss_fixture_t *f, const char *name) {
    for (int i = 0; i < f->c.reach_count; i++)
        if (strcmp(f->c.reach[i].test, name) == 0 && f->c.reach[i].reached)
            return true;
    return false;
}

static bool tie_miss_fallback(const tie_miss_fixture_t *f, cbm_test_result_fallback_t code) {
    for (int i = 0; i < f->c.fallback_count; i++)
        if (f->c.fallbacks[i] == code)
            return true;
    return false;
}

static void tie_miss_close(tie_miss_fixture_t *f) {
    if (f->c.scope)
        (void)cbm_store_read_scope_close(f->c.scope);
    cbm_ti_seeds_free(f->c.seeds);
    cbm_test_policy_free(f->c.policy);
    cbm_test_config_free(f->c.config);
    cbm_test_model_free(f->c.model);
    cbm_ti_source_free(f->c.source);
    cbm_ti_graph_free(f->c.graph);
    if (f->c.store)
        cbm_store_close(f->c.store);
    cbm_free(CBM_MEM_CLASS_EXTRACT, f->c.fallbacks);
    cbm_free(CBM_MEM_CLASS_EXTRACT, f->c.triggers);
    cbm_arena_destroy(&f->c.arena);
}

static const char tie_miss_cases[] =
    "TEST(owner) {\n    return target();\n}\n"
    "TEST(decoy) {\n    return 0;\n}\n"
    "SUITE(alpha) {\n    RUN_TEST(owner);\n    RUN_TEST(decoy);\n}\n";
static const char tie_miss_runner[] =
    "int main(void) {\n    RUN_SELECTED_SUITE(alpha);\n    return 0;\n}\n";

static bool tie_miss_basic(tie_miss_fixture_t *f) {
    const char *const files[] = {"src/lib.c",          "int target(void) { return 1; }\n",
                                 "tests/test_cases.c", tie_miss_cases,
                                 "tests/test_main.c",  tie_miss_runner};
    return tie_miss_open(f, files, 3) &&
           tie_miss_node(f, "src/lib.c", "target", "Function", 1, 1) > 0 &&
           tie_miss_node(f, "tests/test_cases.c", "owner", "Function", 1, 3) > 0 &&
           tie_miss_node(f, "tests/test_cases.c", "decoy", "Function", 4, 6) > 0 &&
           tie_miss_node(f, "tests/test_cases.c", "alpha", "Function", 7, 10) > 0 &&
           tie_miss_node(f, "tests/test_main.c", "main", "Function", 1, 4) > 0;
}

TEST(test_impact_engine_resolver_miss_selects_exact_owner_without_an_edge) {
    for (int origin = 0; origin < 2; origin++) {
        tie_miss_fixture_t f;
        ASSERT_TRUE(tie_miss_basic(&f));
        cbm_resolve_record_t row = tie_miss_row(&f, "tests/test_cases.c", "owner", "target");
        if (origin) {
            /* Expanded bytes are deliberately outside the raw file. Scope,
             * never a raw interpretation of expanded offsets, identifies owner. */
            row.source_origin = CBM_SOURCE_ORIGIN_PREPROCESSED;
            row.start_byte = 40000;
            row.end_byte = 40006;
        }
        ASSERT_EQ(cbm_store_resolver_replace(f.c.store, "test-impact", &row, 1), CBM_STORE_OK);
        ASSERT_TRUE(tie_miss_seed(&f, "src/lib.c", 0));
        ASSERT_TRUE(te_reach(&f.c));
        ASSERT_TRUE(tie_miss_selected(&f, "owner"));
        ASSERT_FALSE(tie_miss_selected(&f, "decoy"));
        ASSERT_FALSE(tie_miss_fallback(&f, CBM_TEST_RESULT_FALLBACK_RUNNER_REACHED));
        ASSERT_EQ(cbm_store_count_edges_by_type(f.c.store, "test-impact", "CALLS"), 0);
        tie_miss_close(&f);
    }
    PASS();
}

TEST(test_impact_engine_resolver_miss_unknown_owner_uses_its_whole_file) {
    tie_miss_fixture_t f;
    ASSERT_TRUE(tie_miss_basic(&f));
    cbm_resolve_record_t row = tie_miss_row(&f, "tests/test_cases.c", "missing_owner", "target");
    /* A real QN from another file must not masquerade as the owner. */
    row.scope = tie_miss_qn(&f, "src/lib.c", "target");
    ASSERT_EQ(cbm_store_resolver_replace(f.c.store, "test-impact", &row, 1), CBM_STORE_OK);
    ASSERT_TRUE(tie_miss_seed(&f, "src/lib.c", 0));
    ASSERT_TRUE(te_reach(&f.c));
    ASSERT_TRUE(tie_miss_selected(&f, "owner"));
    ASSERT_TRUE(tie_miss_selected(&f, "decoy"));
    ASSERT_EQ(cbm_store_count_edges_by_type(f.c.store, "test-impact", "CALLS"), 0);
    tie_miss_close(&f);
    PASS();
}

TEST(test_impact_engine_resolver_miss_ignores_unwalked_relations) {
    tie_miss_fixture_t f;
    ASSERT_TRUE(tie_miss_basic(&f));
    cbm_resolve_record_t row = tie_miss_row(&f, "tests/test_cases.c", "owner", "target");
    row.relation = "IMPORTS";
    ASSERT_EQ(cbm_store_resolver_replace(f.c.store, "test-impact", &row, 1), CBM_STORE_OK);
    ASSERT_TRUE(tie_miss_seed(&f, "src/lib.c", 0));
    ASSERT_TRUE(te_reach(&f.c));
    ASSERT_FALSE(tie_miss_selected(&f, "owner"));
    ASSERT_FALSE(tie_miss_selected(&f, "decoy"));
    tie_miss_close(&f);
    PASS();
}

TEST(test_impact_engine_resolver_miss_preserves_runner_sink_namesakes) {
    for (int include_function = 0; include_function < 2; include_function++) {
        const char *const files[] = {
            "src/lib.c",          "struct Shape { int alpha; };\nint alpha(void) { return 1; }\n",
            "tests/test_cases.c", tie_miss_cases,
            "tests/test_main.c",  tie_miss_runner};
        tie_miss_fixture_t f;
        ASSERT_TRUE(tie_miss_open(&f, files, 3));
        ASSERT_TRUE(tie_miss_node(&f, "src/lib.c", "alpha", "Field", 1, 1) > 0);
        /* Use a distinct exact QN for the namesake while retaining its short name. */
        cbm_node_t fn = {.project = "test-impact",
                         .name = "alpha",
                         .label = "Function",
                         .qualified_name = "test-impact.src.other.alpha",
                         .file_path = "src/lib.c",
                         .start_line = 2,
                         .end_line = 2};
        ASSERT_TRUE(cbm_store_upsert_node(f.c.store, &fn) > 0);
        ASSERT_TRUE(tie_miss_node(&f, "tests/test_cases.c", "owner", "Function", 1, 3) > 0);
        ASSERT_TRUE(tie_miss_node(&f, "tests/test_cases.c", "decoy", "Function", 4, 6) > 0);
        ASSERT_TRUE(tie_miss_node(&f, "tests/test_cases.c", "alpha", "Function", 7, 10) > 0);
        ASSERT_TRUE(tie_miss_node(&f, "tests/test_main.c", "main", "Function", 1, 4) > 0);
        cbm_resolve_record_t row = tie_miss_row(&f, "tests/test_main.c", "main", "alpha");
        ASSERT_EQ(cbm_store_resolver_replace(f.c.store, "test-impact", &row, 1), CBM_STORE_OK);
        ASSERT_TRUE(tie_miss_seed(&f, "src/lib.c", include_function ? 0 : 1));
        ASSERT_TRUE(te_reach(&f.c));
        ASSERT_EQ(tie_miss_fallback(&f, CBM_TEST_RESULT_FALLBACK_RUNNER_REACHED),
                  include_function != 0);
        if (include_function)
            ASSERT_NOT_NULL(strstr(f.diagnostic, "main -> other.alpha"));
        tie_miss_close(&f);
    }
    PASS();
}

TEST(test_impact_engine_resolver_misses_feed_role_and_spawn_fixpoints) {
    for (int spawn = 0; spawn < 2; spawn++) {
        static const char runner[] =
            "int role_probe(void) { return target(); }\n"
            "TF_ROLE(EARLY, \"--probe\", role_probe)\n"
            "int main(void) {\n    RUN_SELECTED_SUITE(alpha);\n    return role_probe();\n}\n";
        static const char cases[] =
            "int start_probe(void) { const char *role = \"--probe\"; return 0; }\n"
            "TEST(owner) {\n    return start_probe();\n}\n"
            "TEST(decoy) {\n    return 0;\n}\n"
            "SUITE(alpha) {\n    RUN_TEST(owner);\n    RUN_TEST(decoy);\n}\n";
        const char *const files[] = {"src/lib.c",          "int target(void) { return 1; }\n",
                                     "src/main.c",         "int main(void) { return target(); }\n",
                                     "tests/test_cases.c", cases,
                                     "tests/test_main.c",  runner};
        tie_miss_fixture_t f;
        ASSERT_TRUE(tie_miss_open(&f, files, 4));
        ASSERT_TRUE(tie_miss_node(&f, "src/lib.c", "target", "Function", 1, 1) > 0);
        ASSERT_TRUE(tie_miss_node(&f, "src/main.c", "main", "Function", 1, 1) > 0);
        int64_t handler = tie_miss_node(&f, "tests/test_main.c", "role_probe", "Function", 1, 1);
        ASSERT_TRUE(handler > 0);
        ASSERT_TRUE(tie_miss_node(&f, "tests/test_main.c", "main", "Function", 3, 6) > 0);
        ASSERT_TRUE(tie_miss_node(&f, "tests/test_cases.c", "start_probe", "Function", 1, 1) > 0);
        ASSERT_TRUE(tie_miss_node(&f, "tests/test_cases.c", "owner", "Function", 2, 4) > 0);
        ASSERT_TRUE(tie_miss_node(&f, "tests/test_cases.c", "decoy", "Function", 5, 7) > 0);
        ASSERT_TRUE(tie_miss_node(&f, "tests/test_cases.c", "alpha", "Function", 8, 11) > 0);
        if (spawn) {
            int64_t process = tie_miss_node(&f, "tests/test_main.c", "child", "Process", 1, 1);
            ASSERT_TRUE(process > 0);
            cbm_edge_t edge = {.project = "test-impact",
                               .source_id = handler,
                               .target_id = process,
                               .type = "SPAWNS"};
            ASSERT_TRUE(cbm_store_insert_edge(f.c.store, &edge) > 0);
        }
        cbm_resolve_record_t rows[] = {
            tie_miss_row(&f, spawn ? "src/main.c" : "tests/test_main.c",
                         spawn ? "main" : "role_probe", "target"),
            tie_miss_row(&f, "tests/test_cases.c", "owner", "start_probe"),
            tie_miss_row(&f, "tests/test_main.c", "main", "role_probe")};
        ASSERT_EQ(cbm_store_resolver_replace(f.c.store, "test-impact", rows, 3), CBM_STORE_OK);
        ASSERT_TRUE(tie_miss_seed(&f, "src/lib.c", 0));
        ASSERT_TRUE(te_reach(&f.c));
        ASSERT_TRUE(tie_miss_selected(&f, "owner"));
        ASSERT_FALSE(tie_miss_selected(&f, "decoy"));
        ASSERT_FALSE(tie_miss_fallback(&f, CBM_TEST_RESULT_FALLBACK_RUNNER_REACHED));
        ASSERT_EQ(cbm_store_count_edges_by_type(f.c.store, "test-impact", "CALLS"), 0);
        tie_miss_close(&f);
    }
    PASS();
}

static int tie_miss_deny_read(void *unused, int action, const char *table, const char *column,
                              const char *database, const char *trigger) {
    (void)unused;
    (void)column;
    (void)database;
    (void)trigger;
    return action == SQLITE_READ && table && strcmp(table, "resolver_occurrences") == 0
               ? SQLITE_DENY
               : SQLITE_OK;
}

TEST(test_impact_engine_resolver_metadata_failures_are_uncertainty) {
    for (int mode = 0; mode < 3; mode++) {
        tie_miss_fixture_t f;
        ASSERT_TRUE(tie_miss_basic(&f));
        if (mode) {
            cbm_resolve_record_t row = tie_miss_row(&f, "tests/test_cases.c", "owner", "target");
            ASSERT_EQ(cbm_store_resolver_replace(f.c.store, "test-impact", &row, 1), CBM_STORE_OK);
        }
        if (mode == 1) {
            /* SQLite accepts a nonintegral REAL in this INTEGER column.
             * Summary counts one abstention, but the exact emitted=0 visitor
             * returns none: this cannot be accepted as a complete empty load. */
            ASSERT_EQ(sqlite3_exec(cbm_store_get_db(f.c.store),
                                   "UPDATE resolver_occurrence_rows SET emitted=0.5", NULL, NULL,
                                   NULL),
                      SQLITE_OK);
        }
        ASSERT_TRUE(tie_miss_seed(&f, "src/lib.c", 0));
        if (mode == 2)
            sqlite3_set_authorizer(cbm_store_get_db(f.c.store), tie_miss_deny_read, NULL);
        ASSERT_FALSE(te_reach(&f.c));
        if (mode == 2)
            sqlite3_set_authorizer(cbm_store_get_db(f.c.store), NULL, NULL);
        ASSERT_FALSE(f.c.static_complete);
        ASSERT_TRUE(tie_miss_fallback(&f, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE));
        tie_miss_close(&f);
    }
    PASS();
}

typedef struct {
    int visits;
    int steps;
} tie_miss_sql_t;
static int tie_miss_sql_profile(unsigned event, void *context, void *statement, void *elapsed) {
    (void)elapsed;
    if (event == SQLITE_TRACE_PROFILE) {
        const char *sql = sqlite3_sql(statement);
        if (sql && strstr(sql, "WHERE project=?1 AND emitted=0 ORDER BY")) {
            tie_miss_sql_t *counter = context;
            counter->visits++;
            counter->steps += sqlite3_stmt_status(statement, SQLITE_STMTSTATUS_VM_STEP, 0);
        }
    }
    return 0;
}

TEST(test_impact_engine_resolver_miss_chain_load_is_linear_and_once) {
    int steps[2] = {0};
    for (int scale = 0; scale < 2; scale++) {
        int count = 16 << scale;
        char source[8192];
        size_t used = 0;
        for (int i = 0; i < count; i++) {
            int n = snprintf(source + used, sizeof(source) - used,
                             "int helper%d(void) { return %s%d(); }\n", i, i ? "helper" : "target",
                             i ? i - 1 : 0);
            ASSERT_TRUE(n > 0 && (size_t)n < sizeof(source) - used);
            used += (size_t)n;
        }
        int n = snprintf(source + used, sizeof(source) - used,
                         "TEST(owner) {\n    return helper%d();\n}\n"
                         "SUITE(alpha) {\n    RUN_TEST(owner);\n}\n",
                         count - 1);
        ASSERT_TRUE(n > 0 && (size_t)n < sizeof(source) - used);
        const char *const files[] = {"src/lib.c",          "int target0(void) { return 1; }\n",
                                     "tests/test_cases.c", source,
                                     "tests/test_main.c",  tie_miss_runner};
        tie_miss_fixture_t f;
        ASSERT_TRUE(tie_miss_open(&f, files, 3));
        ASSERT_TRUE(tie_miss_node(&f, "src/lib.c", "target0", "Function", 1, 1) > 0);
        cbm_resolve_record_t rows[33];
        for (int i = 0; i <= count; i++) {
            char name[40], target[40];
            if (i == count)
                snprintf(name, sizeof(name), "owner");
            else
                snprintf(name, sizeof(name), "helper%d", i);
            snprintf(target, sizeof(target), i ? "helper%d" : "target%d", i ? i - 1 : 0);
            ASSERT_TRUE(tie_miss_node(&f, "tests/test_cases.c", name, "Function", i + 1,
                                      i == count ? i + 3 : i + 1) > 0);
            rows[i] =
                tie_miss_row(&f, "tests/test_cases.c", name, cbm_arena_strdup(&f.c.arena, target));
        }
        ASSERT_TRUE(
            tie_miss_node(&f, "tests/test_cases.c", "alpha", "Function", count + 4, count + 6) > 0);
        ASSERT_TRUE(tie_miss_node(&f, "tests/test_main.c", "main", "Function", 1, 4) > 0);
        ASSERT_EQ(cbm_store_resolver_replace(f.c.store, "test-impact", rows, (size_t)count + 1),
                  CBM_STORE_OK);
        ASSERT_TRUE(tie_miss_seed(&f, "src/lib.c", 0));
        tie_miss_sql_t counter = {0};
        sqlite3_trace_v2(cbm_store_get_db(f.c.store), SQLITE_TRACE_PROFILE, tie_miss_sql_profile,
                         &counter);
        ASSERT_TRUE(te_reach(&f.c));
        sqlite3_trace_v2(cbm_store_get_db(f.c.store), 0, NULL, NULL);
        ASSERT_TRUE(tie_miss_selected(&f, "owner"));
        ASSERT_EQ(counter.visits, 1);
        ASSERT_TRUE(counter.steps > 0);
        steps[scale] = counter.steps;
        ASSERT_EQ(cbm_store_count_edges_by_type(f.c.store, "test-impact", "CALLS"), 0);
        tie_miss_close(&f);
    }
    ASSERT_TRUE(steps[1] > steps[0]);
    ASSERT_TRUE(steps[1] < 3 * steps[0]); /* deterministic VM work, never elapsed time */
    PASS();
}

SUITE(test_impact_engine) {
    RUN_TEST(test_impact_engine_resolver_miss_selects_exact_owner_without_an_edge);
    RUN_TEST(test_impact_engine_resolver_miss_unknown_owner_uses_its_whole_file);
    RUN_TEST(test_impact_engine_resolver_miss_ignores_unwalked_relations);
    RUN_TEST(test_impact_engine_resolver_miss_preserves_runner_sink_namesakes);
    RUN_TEST(test_impact_engine_resolver_misses_feed_role_and_spawn_fixpoints);
    RUN_TEST(test_impact_engine_resolver_metadata_failures_are_uncertainty);
    RUN_TEST(test_impact_engine_resolver_miss_chain_load_is_linear_and_once);

    RUN_TEST(test_impact_engine_variant_tests_are_one_test);
    RUN_TEST(test_impact_engine_platform_variants_reach_each_others_callers);
    RUN_TEST(test_impact_engine_spawning_tests_follow_main);
    RUN_TEST(test_impact_engine_reached_suites_carry_static);
    RUN_TEST(test_impact_engine_macro_registered_tests_select_their_suite);
    RUN_TEST(test_impact_engine_rules_unmapped_and_empty);
    RUN_TEST(test_impact_engine_path_key_selects_naming_tests_or_ignores);
    RUN_TEST(test_impact_engine_role_reach_selects_its_spawners);
    RUN_TEST(test_impact_engine_chained_roles_select_both_spawners);
    RUN_TEST(test_impact_engine_unspawned_role_and_main_still_run_all);
    RUN_TEST(test_impact_engine_role_with_two_literals_selects_both_spawners);
    RUN_TEST(test_impact_engine_role_literal_behind_a_define_selects_its_users);
    RUN_TEST(test_impact_engine_role_literal_behind_a_macro_chain);
    RUN_TEST(test_impact_engine_unusable_role_entry_runs_all);
    RUN_TEST(test_impact_engine_role_reached_by_the_spawn_lift_is_lifted);
    RUN_TEST(test_impact_engine_runner_helper_names_no_spawner);
    RUN_TEST(test_impact_engine_field_call_is_no_way_into_main);
    RUN_TEST(test_impact_engine_reached_role_handler_is_no_way_into_main);
    RUN_TEST(test_impact_engine_parse_gaps_reach_what_they_name);
    RUN_TEST(test_impact_engine_admitted_coverage_narrows_only_when_verified);
    RUN_TEST(test_impact_engine_coverage_reaches_unmapped_code_through_mapped_callers);
}
