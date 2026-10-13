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
 * config.json has max_retry_count, handler.go has maxRetryCount: camelCase and
 * snake_case normalize to the same key. (A JSON key links by exact name only,
 * see configlink_key_symbol_infra_exact_only, so the symbol names it exactly.) */
TEST(configlink_key_symbol_camel_case) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/test");

    cbm_gbuf_upsert_node(gb, "Variable", "max_retry_count", "test.config.max_retry_count",
                         "config.json", 0, 0, NULL);

    cbm_gbuf_upsert_node(gb, "Function", "maxRetryCount", "test.handler.maxRetryCount",
                         "handler.go", 0, 0, NULL);

    run_configlink(gb, "test", NULL);

    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "CONFIGURES", &edges, &count);
    ASSERT_TRUE(has_strategy(edges, count, "key_symbol"));
    cbm_gbuf_free(gb);
    PASS();
}

static int count_key_symbol_edges(cbm_gbuf_t *gb) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "CONFIGURES", &edges, &count);
    int n = 0;
    for (int i = 0; i < count; i++) {
        if (edges[i]->properties_json &&
            strstr(edges[i]->properties_json, "\"strategy\":\"key_symbol\"")) {
            n++;
        }
    }
    return n;
}

/* Keys of infrastructure files (YAML, JSON, HCL, tfvars) link by exact name
 * only: the substring guess linked a chart's `node_selector` to every
 * function whose name contains it. In 17 infra repositories 4 of 40 sampled
 * key_symbol edges were right, all exact and in the same directory tree. */
TEST(configlink_key_symbol_infra_exact_only) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/test");
    cbm_gbuf_upsert_node(gb, "Variable", "node_selector", "test.charts.app.values.node_selector",
                         "charts/app/values.yaml", 0, 0, NULL);
    cbm_gbuf_upsert_node(gb, "Function", "GetPodNodeSelectorConfig",
                         "test.charts.app.gen.GetPodNodeSelectorConfig", "charts/app/gen.go", 0, 0,
                         NULL);
    run_configlink(gb, "test", NULL);
    ASSERT_EQ(count_key_symbol_edges(gb), 0);
    cbm_gbuf_free(gb);
    PASS();
}

/* ...and only within the config file's own component (its directory tree):
 * a stage's JSON schema does not configure an unrelated module's variable. */
TEST(configlink_key_symbol_infra_scoped_to_component) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/test");
    int64_t cfg = cbm_gbuf_upsert_node(gb, "Variable", "cluster_name",
                                       "test.svc_a.config.cluster_name", "svc-a/config.yaml", 0,
                                       0, NULL);
    int64_t inside = cbm_gbuf_upsert_node(gb, "Function", "clusterName",
                                          "test.svc_a.cmd.main.clusterName", "svc-a/cmd/main.go",
                                          0, 0, NULL);
    cbm_gbuf_upsert_node(gb, "Function", "clusterName", "test.svc_b.main.clusterName",
                         "svc-b/main.go", 0, 0, NULL);
    run_configlink(gb, "test", NULL);
    ASSERT_EQ(count_key_symbol_edges(gb), 1);
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "CONFIGURES", &edges, &count);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(edges[0]->source_id, inside);
    ASSERT_EQ(edges[0]->target_id, cfg);
    cbm_gbuf_free(gb);
    PASS();
}

/* A conventional config directory (config/, conf/, settings/, ...) belongs to
 * the component around it. */
TEST(configlink_key_symbol_config_dir_component) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/test");
    cbm_gbuf_upsert_node(gb, "Variable", "cluster_name", "test.config.app.cluster_name",
                         "config/app.yaml", 0, 0, NULL);
    cbm_gbuf_upsert_node(gb, "Function", "clusterName", "test.pkg.app.clusterName", "pkg/app.go",
                         0, 0, NULL);
    run_configlink(gb, "test", NULL);
    ASSERT_EQ(count_key_symbol_edges(gb), 1);
    cbm_gbuf_free(gb);
    PASS();
}

/* A Terraform/OpenTofu variable is what a config value sets: a tfvars.json key
 * links to `variable "<key>"` of its component (the block's Class is named
 * `variable.<key>`), not to outputs, modules or resources, and not to a
 * variable of another component (kubespray: contrib/terraform/gcp/tfvars.json
 * sets contrib/terraform/gcp/modules/.../variables.tf). */
TEST(configlink_key_symbol_terraform_variable) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/test");
    int64_t cfg = cbm_gbuf_upsert_node(gb, "Variable", "ssh_whitelist",
                                       "test.gcp.tfvars.ssh_whitelist",
                                       "contrib/terraform/gcp/tfvars.json", 0, 0, NULL);
    int64_t var = cbm_gbuf_upsert_node(
        gb, "Class", "variable.ssh_whitelist", "test.gcp.modules.cluster.variable.ssh_whitelist",
        "contrib/terraform/gcp/modules/cluster/variables.tf", 0, 0, NULL);
    cbm_gbuf_upsert_node(gb, "Class", "output.ssh_whitelist",
                         "test.gcp.modules.cluster.output.ssh_whitelist",
                         "contrib/terraform/gcp/modules/cluster/outputs.tf", 0, 0, NULL);
    cbm_gbuf_upsert_node(gb, "Class", "variable.ssh_whitelist",
                         "test.hetzner.variable.ssh_whitelist",
                         "contrib/terraform/hetzner/variables.tf", 0, 0, NULL);
    run_configlink(gb, "test", NULL);
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "CONFIGURES", &edges, &count);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(edges[0]->source_id, var);
    ASSERT_EQ(edges[0]->target_id, cfg);
    cbm_gbuf_free(gb);
    PASS();
}

/* No count of config keys decides what links: the collector stopped at 4,096. */
TEST(configlink_key_symbol_no_config_cap) {
    enum { KEYS = 4200 };
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/test");
    for (int i = 0; i < KEYS; i++) {
        char name[CBM_SZ_64];
        char qn[CBM_SZ_128];
        snprintf(name, sizeof(name), "limit_alpha_%04d", i);
        snprintf(qn, sizeof(qn), "test.config.%s", name);
        cbm_gbuf_upsert_node(gb, "Variable", name, qn, "config.toml", 0, 0, NULL);
        snprintf(qn, sizeof(qn), "test.main.%s", name);
        cbm_gbuf_upsert_node(gb, "Function", name, qn, "main.go", 0, 0, NULL);
    }
    run_configlink(gb, "test", NULL);
    ASSERT_EQ(count_key_symbol_edges(gb), KEYS);
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

/* ── Determinism: candidate truncation must not depend on insert order ── */

static int cl_cmp_row(const void *pa, const void *pb) {
    return strcmp(*(const char *const *)pa, *(const char *const *)pb);
}

/* Collect the CONFIGURES edge set as a sorted, newline-joined
 * "src_qn|tgt_qn" fingerprint. Caller frees. */
static char *configures_fingerprint(cbm_gbuf_t *gb) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "CONFIGURES", &edges, &count);

    char **rows = calloc((size_t)(count > 0 ? count : 1), sizeof(*rows));
    if (!rows) {
        return NULL;
    }
    int n = 0;
    for (int i = 0; i < count; i++) {
        const cbm_gbuf_node_t *s = cbm_gbuf_find_by_id(gb, edges[i]->source_id);
        const cbm_gbuf_node_t *t = cbm_gbuf_find_by_id(gb, edges[i]->target_id);
        if (!s || !t || !s->qualified_name || !t->qualified_name) {
            continue;
        }
        size_t len = strlen(s->qualified_name) + strlen(t->qualified_name) + 2;
        rows[n] = malloc(len);
        if (!rows[n]) {
            break;
        }
        snprintf(rows[n], len, "%s|%s", s->qualified_name, t->qualified_name);
        n++;
    }
    qsort(rows, (size_t)n, sizeof(*rows), cl_cmp_row);

    size_t total = 1;
    for (int i = 0; i < n; i++) {
        total += strlen(rows[i]) + 1;
    }
    char *out = calloc(total, 1);
    if (out) {
        for (int i = 0; i < n; i++) {
            strcat(out, rows[i]);
            strcat(out, "\n");
        }
    }
    for (int i = 0; i < n; i++) {
        free(rows[i]);
    }
    free(rows);
    return out;
}

/* Build a gbuf whose code-candidate count exceeds the collector's internal
 * cap, inserting the candidates either forwards or backwards. */
enum { CL_DET_CANDIDATES = 9000 }; /* > the 8192 code-entry cap */

static cbm_gbuf_t *build_oversized_corpus(bool reverse) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/test");

    /* One config key every candidate can match on: "max_connections". */
    cbm_gbuf_upsert_node(gb, "Variable", "max_connections", "test.config.max_connections",
                         "config.toml", 0, 0, NULL);

    for (int k = 0; k < CL_DET_CANDIDATES; k++) {
        int i = reverse ? (CL_DET_CANDIDATES - 1 - k) : k;
        char name[CBM_SZ_64];
        char qn[CBM_SZ_128];
        char path[CBM_SZ_64];
        snprintf(name, sizeof(name), "getMaxConnections%04d", i);
        snprintf(qn, sizeof(qn), "test.mod%04d.%s", i, name);
        snprintf(path, sizeof(path), "mod%04d.go", i);
        cbm_gbuf_upsert_node(gb, "Function", name, qn, path, 0, 0, NULL);
    }
    return gb;
}

/* REGRESSION: the collectors walk gbuf label indexes in insertion order --
 * under parallel extraction worker-merge order, which varies run to run.
 * They filled fixed-capacity arrays and stopped at a cap, so the surviving
 * set depended on that order until candidates were sorted first; now no cap
 * cuts the set at all. This test stands in for the scheduling variance by
 * feeding the same corpus in two insertion orders: the emitted edge set must
 * be identical -- and complete, all 9,000 candidates past the old 8,192 cap. */
TEST(configlink_candidate_truncation_is_order_independent) {
    cbm_gbuf_t *fwd = build_oversized_corpus(false);
    run_configlink(fwd, "test", NULL);
    char *fp_fwd = configures_fingerprint(fwd);
    int fwd_edges = count_key_symbol_edges(fwd);

    cbm_gbuf_t *rev = build_oversized_corpus(true);
    run_configlink(rev, "test", NULL);
    char *fp_rev = configures_fingerprint(rev);

    ASSERT_TRUE(fp_fwd != NULL);
    ASSERT_TRUE(fp_rev != NULL);
    ASSERT_EQ(fwd_edges, CL_DET_CANDIDATES);
    ASSERT_STR_EQ(fp_fwd, fp_rev);

    free(fp_fwd);
    free(fp_rev);
    cbm_gbuf_free(fwd);
    cbm_gbuf_free(rev);
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
    RUN_TEST(configlink_key_symbol_infra_exact_only);
    RUN_TEST(configlink_key_symbol_infra_scoped_to_component);
    RUN_TEST(configlink_key_symbol_config_dir_component);
    RUN_TEST(configlink_key_symbol_no_config_cap);
    RUN_TEST(configlink_key_symbol_terraform_variable);

    /* Strategy 2: Dependency → Import */
    RUN_TEST(configlink_dep_import_package_json);

    /* Strategy 3: File Path → Reference */
    RUN_TEST(configlink_file_ref_no_false_positive);

    /* Determinism */
    RUN_TEST(configlink_candidate_truncation_is_order_independent);
}
