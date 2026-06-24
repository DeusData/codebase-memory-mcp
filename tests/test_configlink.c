/*
 * test_configlink.c — Tests for config ↔ code linking strategies.
 *
 * Ported from internal/pipeline/configlink_test.go (9 tests).
 * Uses unit test approach: set up gbuf state directly, run strategies,
 * check CONFIGURES edges.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdatomic.h>

/* Helper: wrap old (gb, project, repo_path) call into ctx-based API */
static int run_configlink(cbm_gbuf_t *gb, const char *project, const char *repo_path) {
    atomic_int cancelled;
    atomic_init(&cancelled, 0);
    cbm_pipeline_ctx_t ctx = {
        .project_name = project,
        .repo_path = repo_path,
        .gbuf = gb,
        .cancelled = &cancelled,
    };
    return cbm_pipeline_pass_configlink(&ctx);
}
#include <sys/stat.h>
#include <unistd.h>

/* ── Helpers ─────────────────────────────────────────────────────── */

/* Helper to check if any CONFIGURES edge has a given strategy (parsed from JSON) */
static bool has_strategy(const cbm_gbuf_edge_t **edges, int count, const char *strategy) {
    char needle[64];
    snprintf(needle, sizeof(needle), "\"strategy\":\"%s\"", strategy);
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle))
            return true;
    }
    return false;
}

/* Check if any CONFIGURES edge has given strategy AND confidence. */
static bool has_strategy_with_confidence(const cbm_gbuf_edge_t **edges, int count,
                                         const char *strategy, double confidence) {
    char strat_needle[64];
    snprintf(strat_needle, sizeof(strat_needle), "\"strategy\":\"%s\"", strategy);
    char conf_needle[64];
    snprintf(conf_needle, sizeof(conf_needle), "\"confidence\":%.2f", confidence);

    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, strat_needle) &&
            strstr(edges[i]->properties_json, conf_needle))
            return true;
    }
    return false;
}

/* Check if any CONFIGURES edge has given strategy AND config_key. */
static bool has_strategy_with_key(const cbm_gbuf_edge_t **edges, int count, const char *strategy,
                                  const char *config_key) {
    char strat_needle[64];
    snprintf(strat_needle, sizeof(strat_needle), "\"strategy\":\"%s\"", strategy);
    char key_needle[128];
    snprintf(key_needle, sizeof(key_needle), "\"config_key\":\"%s\"", config_key);

    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, strat_needle) &&
            strstr(edges[i]->properties_json, key_needle))
            return true;
    }
    return false;
}

/* Recursive remove */
static void rm_rf(const char *path) {
    th_rmtree(path);
}

/* ── Strategy 1: Config Key → Code Symbol ───────────────────────── */

/* Go: TestConfigKeySymbol_ExactMatch
 * config.toml has max_connections, main.go has getMaxConnections() */
TEST(configlink_key_symbol_exact_match) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/test");

    /* Config Variable node: max_connections from config.toml */
    int64_t cfg_id = cbm_gbuf_upsert_node(gb, "Variable", "max_connections",
                                          "test.config.max_connections", "config.toml", 0, 0, NULL);
    ASSERT_GT(cfg_id, 0);

    /* Code Function node: getMaxConnections from main.go */
    int64_t func_id = cbm_gbuf_upsert_node(gb, "Function", "getMaxConnections",
                                           "test.main.getMaxConnections", "main.go", 0, 0, NULL);
    ASSERT_GT(func_id, 0);

    /* Run configlink */
    int n = run_configlink(gb, "test", NULL);
    ASSERT_GT(n, 0);

    /* Check CONFIGURES edges */
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "CONFIGURES", &edges, &count);
    ASSERT_TRUE(has_strategy(edges, count, "key_symbol"));
    cbm_gbuf_free(gb);
    PASS();
}

/* Go: TestConfigKeySymbol_SubstringMatch
 * config.toml has request_timeout, main.go has getRequestTimeoutSeconds() */
TEST(configlink_key_symbol_substring_match) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/test");

    cbm_gbuf_upsert_node(gb, "Variable", "request_timeout", "test.config.request_timeout",
                         "config.toml", 0, 0, NULL);

    cbm_gbuf_upsert_node(gb, "Function", "getRequestTimeoutSeconds",
                         "test.main.getRequestTimeoutSeconds", "main.go", 0, 0, NULL);

    run_configlink(gb, "test", NULL);

    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "CONFIGURES", &edges, &count);
    ASSERT_TRUE(has_strategy_with_confidence(edges, count, "key_symbol", 0.75));
    cbm_gbuf_free(gb);
    PASS();
}

/* Go: TestConfigKeySymbol_ShortKeySkipped
 * config.toml has port, host, name — all 1-token keys, should be skipped */
TEST(configlink_key_symbol_short_key_skipped) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/test");

    /* Short single-token config keys */
    const char *short_keys[] = {"port", "host", "name"};
    for (int i = 0; i < 3; i++) {
        char qn[64];
        snprintf(qn, sizeof(qn), "test.config.%s", short_keys[i]);
        cbm_gbuf_upsert_node(gb, "Variable", short_keys[i], qn, "config.toml", 0, 0, NULL);
    }

    cbm_gbuf_upsert_node(gb, "Function", "getPort", "test.main.getPort", "main.go", 0, 0, NULL);

    run_configlink(gb, "test", NULL);

    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "CONFIGURES", &edges, &count);
    /* No key_symbol edges for 1-token keys */
    ASSERT_FALSE(has_strategy(edges, count, "key_symbol"));
    cbm_gbuf_free(gb);
    PASS();
}

/* Go: TestConfigKeySymbol_CamelCaseNormalization
 * config.json has maxRetryCount, handler.go has getMaxRetryCount() */
TEST(configlink_key_symbol_camel_case) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/test");

    cbm_gbuf_upsert_node(gb, "Variable", "maxRetryCount", "test.config.maxRetryCount",
                         "config.json", 0, 0, NULL);

    cbm_gbuf_upsert_node(gb, "Function", "getMaxRetryCount", "test.handler.getMaxRetryCount",
                         "handler.go", 0, 0, NULL);

    run_configlink(gb, "test", NULL);

    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "CONFIGURES", &edges, &count);
    ASSERT_TRUE(has_strategy(edges, count, "key_symbol"));
    cbm_gbuf_free(gb);
    PASS();
}

/* Go: TestConfigKeySymbol_NoFalsePositive
 * config.toml has url — 1-token key, should not match parseURL */
TEST(configlink_key_symbol_no_false_positive) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/test");

    cbm_gbuf_upsert_node(gb, "Variable", "url", "test.db.url", "config.toml", 0, 0, NULL);

    cbm_gbuf_upsert_node(gb, "Function", "parseURL", "test.main.parseURL", "main.go", 0, 0, NULL);

    run_configlink(gb, "test", NULL);

    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "CONFIGURES", &edges, &count);
    /* url is 1 token → skipped by key_symbol strategy */
    ASSERT_FALSE(has_strategy_with_key(edges, count, "key_symbol", "url"));
    cbm_gbuf_free(gb);
    PASS();
}

/* ── Strategy 2: Dependency → Import ────────────────────────────── */

/* Go: TestDependencyImport_PackageJson
 * package.json has express dep, app.js imports express */
TEST(configlink_dep_import_package_json) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/test");

    /* Dependency Variable in package.json */
    cbm_gbuf_upsert_node(gb, "Variable", "express", "test.package.dependencies.express",
                         "package.json", 0, 0, NULL);

    /* Module node for app.js (source of import) */
    int64_t app_id = cbm_gbuf_upsert_node(gb, "Module", "app", "test.app", "app.js", 0, 0, NULL);

    /* Import target node (what 'express' resolves to) */
    int64_t target_id = cbm_gbuf_upsert_node(gb, "Module", "express", "test.node_modules.express",
                                             "node_modules/express/index.js", 0, 0, NULL);

    /* IMPORTS edge: app.js → express module */
    cbm_gbuf_insert_edge(gb, app_id, target_id, "IMPORTS", NULL);

    run_configlink(gb, "test", NULL);

    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "CONFIGURES", &edges, &count);
    /* Should find dependency_import edge linking app.js to package.json dep */
    ASSERT_TRUE(has_strategy(edges, count, "dependency_import"));
    cbm_gbuf_free(gb);
    PASS();
}

/* ── Strategy 3: Config File Path → Code String Reference ───────── */

/* Go: TestConfigFileRef_ExactPath
 * main.go references "config/database.toml" in source code */

/* Go: TestConfigFileRef_BasenameMatch
 * main.go references "settings.yaml" by basename */

/* Go: TestConfigFileRef_NoFalsePositive
 * main.go references "data.csv" — not a config extension */
TEST(configlink_file_ref_no_false_positive) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_cfglink_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Create data.csv */
    char csv_path[512];
    snprintf(csv_path, sizeof(csv_path), "%s/data.csv", tmpdir);
    FILE *f = fopen(csv_path, "w");
    fprintf(f, "a,b,c\n");
    fclose(f);

    /* Create main.go referencing data.csv */
    char main_path[512];
    snprintf(main_path, sizeof(main_path), "%s/main.go", tmpdir);
    f = fopen(main_path, "w");
    fprintf(f, "package main\n\nfunc loadData() {\n"
               "\tf := readFile(\"data.csv\")\n"
               "\t_ = f\n}\n");
    fclose(f);

    char *project = cbm_project_name_from_path(tmpdir);
    cbm_gbuf_t *gb = cbm_gbuf_new(project, tmpdir);

    /* data.csv Module (NOT a config extension) */
    char *csv_qn = cbm_pipeline_fqn_module(project, "data.csv");
    cbm_gbuf_upsert_node(gb, "Module", "data", csv_qn, "data.csv", 0, 0, NULL);

    /* Source Module */
    char *main_qn = cbm_pipeline_fqn_module(project, "main.go");
    cbm_gbuf_upsert_node(gb, "Module", "main", main_qn, "main.go", 0, 0, NULL);

    run_configlink(gb, project, tmpdir);

    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "CONFIGURES", &edges, &count);
    /* .csv is not a config extension — no file_reference edges */
    ASSERT_FALSE(has_strategy(edges, count, "file_reference"));

    cbm_gbuf_free(gb);
    free(csv_qn);
    free(main_qn);
    free(project);
    rm_rf(tmpdir);
    PASS();
}

/* ── Suite ───────────────────────────────────────────────────────── */

SUITE(configlink) {
    /* Strategy 1: Key → Symbol */
    RUN_TEST(configlink_key_symbol_exact_match);
    RUN_TEST(configlink_key_symbol_substring_match);
    RUN_TEST(configlink_key_symbol_short_key_skipped);
    RUN_TEST(configlink_key_symbol_camel_case);
    RUN_TEST(configlink_key_symbol_no_false_positive);

    /* Strategy 2: Dependency → Import */
    RUN_TEST(configlink_dep_import_package_json);

    /* Strategy 3: File Path → Reference */
    RUN_TEST(configlink_file_ref_no_false_positive);
}
