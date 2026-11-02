/*
 * test_grammar_probe_f.c — IaC/build node/edge probe.
 *
 * SCOPE
 * ─────
 * Probes 19 IaC/config-as-code and build grammars:
 *   IaC/config:  hcl, k8s, kustomize, nix, nickel, pkl, ron, jsonnet, kdl,
 *                hyprlang, devicetree
 *   Build:       cmake, makefile, meson, gn, just, bitbake, kconfig, gomod
 *
 * DIMENSIONS PER GRAMMAR
 * ──────────────────────
 *   • NODE creation   — resource/target/recipe/config-entry blocks reach the graph
 *   • IMPORTS edges   — include/import/`use` where the pipeline models it
 *   • INFRA_MAPS      — HCL module.source → Route edge (parallel path only; probed
 *                       in the single-path here as an expected RED)
 *   • DEPENDS_ON      — gomod require → dependency node
 *
 * COLOUR LEGEND
 * ─────────────
 *   green = guard: the pipeline already produces the result; a failure is a regression.
 *   red   = bug reproduction: pipeline does NOT yet produce the expected node/edge;
 *           the test FAILS until fixed.  Brief inline comment names the root cause.
 *
 * Not registered in test_main.c — sibling agent owns that file.
 *
 * SUITE(grammar_probe_f)
 */

#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "resolver_quality.h"
#include "cbm.h"
#include <mcp/mcp.h>
#include <store/store.h>
#include <pipeline/pipeline.h>
#include <foundation/log.h>
#include <sqlite3.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

/* ══════════════════════════════════════════════════════════════════
 * Harness — mirrors test_grammar_probe_a.c (prefix "gpf_").
 * ══════════════════════════════════════════════════════════════════ */

typedef struct {
    char tmpdir[256];
    char dbpath[512];
    char *project;
    cbm_mcp_server_t *srv;
} GpfProj;

typedef struct {
    const char *name;
    const char *content;
} GpfFile;

static void gpf_to_fwd_slashes(char *p) {
    for (; *p; p++) {
        if (*p == '\\')
            *p = '/';
    }
}

static cbm_store_t *gpf_open_indexed(GpfProj *lp) {
    lp->project = cbm_project_name_from_path(lp->tmpdir);
    if (!lp->project)
        return NULL;
    const char *home = getenv("HOME");
    if (!home)
        home = "/tmp";
    char cache_dir[512];
    snprintf(cache_dir, sizeof(cache_dir), "%s/.cache/codebase-memory-mcp", home);
    cbm_mkdir(cache_dir);
    snprintf(lp->dbpath, sizeof(lp->dbpath), "%s/%s.db", cache_dir, lp->project);
    unlink(lp->dbpath);
    lp->srv = cbm_mcp_server_new(NULL);
    if (!lp->srv)
        return NULL;
    char args[700];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", lp->tmpdir);
    char *resp = cbm_mcp_handle_tool(lp->srv, "index_repository", args);
    if (resp)
        free(resp);
    return cbm_store_open_path(lp->dbpath);
}

static cbm_store_t *gpf_index_files(GpfProj *lp, const GpfFile *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_gpf_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir))
        return NULL;
    gpf_to_fwd_slashes(lp->tmpdir);
    for (int i = 0; i < nfiles; i++) {
        char path[700];
        snprintf(path, sizeof(path), "%s/%s", lp->tmpdir, files[i].name);
        char *slash = strrchr(path, '/');
        if (slash && slash > path + strlen(lp->tmpdir)) {
            *slash = '\0';
            cbm_mkdir_p(path, 0755);
            *slash = '/';
        }
        FILE *f = fopen(path, "wb");
        if (!f)
            return NULL;
        fputs(files[i].content, f);
        fclose(f);
    }
    return gpf_open_indexed(lp);
}

static void gpf_cleanup(GpfProj *lp, cbm_store_t *store) {
    if (store)
        cbm_store_close(store);
    if (lp->srv) {
        cbm_mcp_server_free(lp->srv);
        lp->srv = NULL;
    }
    free(lp->project);
    lp->project = NULL;
    th_rmtree(lp->tmpdir);
    unlink(lp->dbpath);
    char wal[600], shm[600];
    snprintf(wal, sizeof(wal), "%s-wal", lp->dbpath);
    unlink(wal);
    snprintf(shm, sizeof(shm), "%s-shm", lp->dbpath);
    unlink(shm);
}

/* ── Node-count helpers ──────────────────────────────────────────── */

static int gpf_count_label(cbm_store_t *store, const char *project, const char *label) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_label(store, project, label, &nodes, &count) != CBM_STORE_OK)
        return -1;
    cbm_store_free_nodes(nodes, count);
    return count;
}

/* Metrics bundled per index pass. */
typedef struct {
    int ok;
    int total_nodes;
    int functions;  /* Function label */
    int modules;    /* Module label */
    int classes;    /* Class label */
    int resources;  /* Resource label (k8s) */
    int charts;     /* Chart label (helm) */
    int variables;  /* Variable label */
    int imports;    /* IMPORTS edges */
    int depends_on; /* DEPENDS_ON edges */
    int infra_maps; /* INFRA_MAPS edges */
    int routes;     /* Route label */
} GpfMetrics;

static GpfMetrics gpf_metrics_files(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    GpfMetrics m = {0};
    if (store) {
        m.ok = 1;
        m.total_nodes = cbm_store_count_nodes(store, lp.project);
        m.functions = gpf_count_label(store, lp.project, "Function");
        m.modules = gpf_count_label(store, lp.project, "Module");
        m.classes = gpf_count_label(store, lp.project, "Class");
        m.resources = gpf_count_label(store, lp.project, "Resource");
        m.charts = gpf_count_label(store, lp.project, "Chart");
        m.variables = gpf_count_label(store, lp.project, "Variable");
        m.imports = cbm_store_count_edges_by_type(store, lp.project, "IMPORTS");
        m.depends_on = cbm_store_count_edges_by_type(store, lp.project, "DEPENDS_ON");
        m.infra_maps = cbm_store_count_edges_by_type(store, lp.project, "INFRA_MAPS");
        m.routes = gpf_count_label(store, lp.project, "Route");
    }
    gpf_cleanup(&lp, store);
    return m;
}

static GpfMetrics gpf_metrics(const char *filename, const char *content) {
    GpfFile f = {filename, content};
    return gpf_metrics_files(&f, 1);
}

/* These literal-import probes retain independently authored missing imports.
 * Every covered File caller closes its IMPORTS domain: a guessed target is a
 * bug, even if some other import happens to exist. The supported ESM call is a
 * required control through the same MCP indexing and persistence path.
 * Original source fixtures and their path spellings remain untouched. */
static const GpfFile GPF_IMPORT_ECMA_CONTROL = {"quality_control.js",
                                                "export function target() { return 1; }\n"
                                                "export function caller() { return target(); }\n"};
static const RQ_Expected GPF_IMPORT_ECMA_REQUIRED = {
    .case_id = "required-esm-local-call",
    .site = {.file = "quality_control.js",
             .caller = "quality_control.caller",
             .needle = "target()",
             .occurrence_index = 1},
    .relation = "CALLS",
    .target = {.file = "quality_control.js",
               .suffix = "quality_control.target",
               .label = "Function",
               .declaration_line = 1,
               .declaration_text = "export function target() { return 1; }"},
    .mode = RQ_REQUIRED};

static bool gpf_literal_import_quality(const GpfFile files[2], const RQ_Source inputs[2],
                                       const char *suite, const RQ_Expected *gap,
                                       const RQ_Forbidden *forbidden, RQ_Report *report) {
    GpfFile indexed[] = {files[0], files[1], GPF_IMPORT_ECMA_CONTROL};
    RQ_Source sources[] = {
        inputs[0],
        inputs[1],
        {.path = GPF_IMPORT_ECMA_CONTROL.name, .text = GPF_IMPORT_ECMA_CONTROL.content}};
    RQ_Expected expected[2];
    size_t count = 0;
    if (gap)
        expected[count++] = *gap;
    expected[count++] = GPF_IMPORT_ECMA_REQUIRED;
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, indexed, 3);
    if (!store) {
        gpf_cleanup(&lp, store);
        return false;
    }
    bool accepted = rq_check(store, lp.project, suite, sources, 3, expected, count, forbidden,
                             forbidden ? 1 : 0, report, stderr);
    gpf_cleanup(&lp, store);
    return accepted;
}

/* Infrastructure goldens are authored from source and explicit build membership.
 * Each covered caller/relation is closed; a guessed target is a bug. Padding
 * exercises the production parallel driver without adding semantic obligations. */
static bool gpf_infra_quality(const GpfFile *files, size_t nfiles, const char *suite,
                              const RQ_Expected *expected, size_t nexpected,
                              const RQ_Forbidden *forbidden, size_t nforbidden) {
    enum { MAX_INPUTS = 32, PARALLEL_PADDING = 56 };
    if (nfiles > MAX_INPUTS || nexpected >= RQ_MAX_EXPECTATIONS)
        return false;
    GpfFile indexed[MAX_INPUTS + 1 + PARALLEL_PADDING];
    RQ_Source sources[MAX_INPUTS + 1];
    RQ_Expected goldens[RQ_MAX_EXPECTATIONS];
    char pad_names[PARALLEL_PADDING][48];
    for (size_t i = 0; i < nfiles; i++) {
        indexed[i] = files[i];
        sources[i] = (RQ_Source){.path = files[i].name, .text = files[i].content};
    }
    indexed[nfiles] = GPF_IMPORT_ECMA_CONTROL;
    sources[nfiles] =
        (RQ_Source){.path = GPF_IMPORT_ECMA_CONTROL.name, .text = GPF_IMPORT_ECMA_CONTROL.content};
    size_t required = 1;
    for (size_t i = 0; i < nexpected; i++) {
        goldens[i] = expected[i];
        required += expected[i].mode == RQ_REQUIRED;
    }
    goldens[nexpected] = GPF_IMPORT_ECMA_REQUIRED;
    bool ok = true;
    for (int driver = 0; driver < 2; driver++) {
        size_t count = nfiles + 1;
        for (int i = 0; driver && i < PARALLEL_PADDING; i++) {
            snprintf(pad_names[i], sizeof(pad_names[i]), "quality_padding/file_%02d.js", i);
            indexed[count++] = (GpfFile){pad_names[i], "export const padding = 1;\n"};
        }
        GpfProj lp;
        cbm_store_t *store = gpf_index_files(&lp, indexed, (int)count);
        RQ_Report report = {0};
        char label[192];
        snprintf(label, sizeof(label), "%s/%s", suite, driver ? "parallel" : "sequential");
        bool checked = store && rq_check(store, lp.project, label, sources, nfiles + 1, goldens,
                                         nexpected + 1, forbidden, nforbidden, &report, stderr);
        ok &= checked && report.genuine == nexpected + 1 && report.found == required &&
              report.missing == nexpected + 1 - required;
        gpf_cleanup(&lp, store);
    }
    return ok;
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 1 — HCL / Terraform (.hcl, .tf)
 *
 * Labels histogram: Class:1, Module:1
 * The extractor models HCL as a single Module node plus a Class node for
 * the file-level scope.  Individual `resource` / `module` blocks are NOT
 * yet extracted as separate nodes (grammar-only path).
 * INFRA_MAPS is produced only by the parallel-path pipeline (>50 files);
 * single-file fixture tests it as RED.
 * ══════════════════════════════════════════════════════════════════ */

/* HCL: minimal resource block → at least 1 node (Module) and 1 Class node. */
TEST(probe_hcl_resource_nodes) {
    GpfMetrics m = gpf_metrics("main.tf", "resource \"aws_s3_bucket\" \"my_bucket\" {\n"
                                          "  bucket = \"my-tf-test-bucket\"\n"
                                          "  acl    = \"private\"\n"
                                          "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: at minimum a Module node is created for an HCL file. */
    ASSERT_TRUE(m.total_nodes >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* HCL: Class node present alongside Module (label histogram: Class:1,Module:1). */
TEST(probe_hcl_class_node) {
    GpfMetrics m = gpf_metrics("infra.hcl", "resource \"aws_instance\" \"web\" {\n"
                                            "  ami           = \"ami-0c55b159cbfafe1f0\"\n"
                                            "  instance_type = \"t2.micro\"\n"
                                            "}\n"
                                            "\n"
                                            "resource \"aws_instance\" \"db\" {\n"
                                            "  ami           = \"ami-0c55b159cbfafe1f0\"\n"
                                            "  instance_type = \"t2.small\"\n"
                                            "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: histogram confirms Class:1,Module:1 for any non-empty HCL file. */
    ASSERT_TRUE(m.classes >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* HCL: module block with `source` → separate resource-like node.
 * RED: grammar-only HCL extractor produces only Class+Module (one per file);
 *      individual resource/module blocks are not broken out as distinct nodes. */
TEST(probe_hcl_module_block_node) {
    GpfMetrics m =
        gpf_metrics("modules.tf", "module \"vpc\" {\n"
                                  "  source  = \"terraform-aws-modules/vpc/aws\"\n"
                                  "  version = \"3.14.0\"\n"
                                  "  cidr    = \"10.0.0.0/16\"\n"
                                  "}\n"
                                  "\n"
                                  "module \"eks\" {\n"
                                  "  source          = \"terraform-aws-modules/eks/aws\"\n"
                                  "  version         = \"18.26.6\"\n"
                                  "  cluster_name    = \"my-eks-cluster\"\n"
                                  "}\n");
    ASSERT_TRUE(m.ok);
    /* RED: individual module blocks not extracted as separate Resource/Module nodes;
     *      only the file-level Class+Module pair is produced. */
    ASSERT_TRUE(m.total_nodes >= 3); /* expected RED — no per-block node extraction */
    PASS();
}

/* HCL: INFRA_MAPS edge (module.source URL → Route node).
 * RED: INFRA_MAPS is produced only by the parallel-path pipeline (>50 files);
 *      single-file fixture never crosses the threshold. */
TEST(probe_hcl_infra_maps_edge) {
    GpfMetrics m = gpf_metrics("sched.tf", "resource \"google_cloud_scheduler_job\" \"cron\" {\n"
                                           "  name     = \"cron-job\"\n"
                                           "  schedule = \"0 * * * *\"\n"
                                           "  http_target {\n"
                                           "    uri = \"https://my-service.example.com/run\"\n"
                                           "  }\n"
                                           "}\n");
    ASSERT_TRUE(m.ok);
    /* RED: INFRA_MAPS requires parallel-path (>50 files) — not triggered here. */
    ASSERT_TRUE(m.infra_maps >= 1); /* expected RED — parallel path only */
    PASS();
}

/* HCL: two-file fixture (.tf + .hcl) → still no cross-file IMPORTS edge
 * because HCL/Terraform uses a directory-level evaluation model, not imports. */
TEST(probe_hcl_no_spurious_imports) {
    static const GpfFile files[] = {{"vars.hcl", "variable \"region\" {\n"
                                                 "  default = \"us-east-1\"\n"
                                                 "}\n"},
                                    {"main.tf", "resource \"aws_s3_bucket\" \"b\" {\n"
                                                "  bucket = \"test\"\n"
                                                "}\n"}};
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* GREEN: HCL has no import syntax — pipeline should emit 0 IMPORTS edges. */
    ASSERT_TRUE(m.imports == 0);
    PASS();
}

/* Only a top-level block (resource, data, module, variable, output, ...) is an
 * entity; blocks nested in it (lifecycle, dynamic, content, the resource's own
 * argument blocks) are part of it. cloud-foundation-fabric minted 1,342
 * `dynamic` and 205 `content` Classes next to 898 resources. */
TEST(probe_hcl_nested_blocks_are_not_classes) {
    GpfMetrics m = gpf_metrics("main.tf", "resource \"google_container_cluster\" \"main\" {\n"
                                          "  name = \"main\"\n"
                                          "  addons_config {\n"
                                          "    http_load_balancing { disabled = false }\n"
                                          "  }\n"
                                          "  dynamic \"node_pool\" {\n"
                                          "    for_each = var.pools\n"
                                          "    content {\n"
                                          "      name = node_pool.value.name\n"
                                          "    }\n"
                                          "  }\n"
                                          "  lifecycle {\n"
                                          "    ignore_changes = [node_pool]\n"
                                          "  }\n"
                                          "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.classes, 1);
    PASS();
}

/* OpenTofu configuration (.tofu) is indexed like .tf. */
TEST(probe_tofu_file_indexed) {
    GpfMetrics m = gpf_metrics("main.tofu", "resource \"aws_s3_bucket\" \"logs\" {\n"
                                            "  bucket = \"logs\"\n"
                                            "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.classes, 1);
    PASS();
}

/* A variable file's assignments are its content. */
TEST(probe_tfvars_file_indexed) {
    GpfMetrics m = gpf_metrics("prod.tfvars", "region       = \"europe-west1\"\n"
                                              "node_count   = 3\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.variables >= 2);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 2 — Kubernetes manifests (.yaml with apiVersion:)
 *
 * Labels histogram: Module:1 (generic YAML pass); Resource node is emitted
 * by pass_k8s.c (handle_k8s_manifest → cbm_extract_file CBM_LANG_K8S).
 * ══════════════════════════════════════════════════════════════════ */

/* K8s: Pod manifest → Resource node (kind=Pod, name=nginx-pod). */
TEST(probe_k8s_pod_resource_node) {
    GpfMetrics m = gpf_metrics("pod.yaml", "apiVersion: v1\n"
                                           "kind: Pod\n"
                                           "metadata:\n"
                                           "  name: nginx-pod\n"
                                           "  namespace: default\n"
                                           "spec:\n"
                                           "  containers:\n"
                                           "  - name: nginx\n"
                                           "    image: nginx:1.21\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: pass_k8s emits a Resource node for any k8s manifest with apiVersion. */
    ASSERT_TRUE(m.resources >= 1);
    PASS();
}

/* K8s: Deployment manifest → Resource node. */
TEST(probe_k8s_deployment_resource_node) {
    GpfMetrics m = gpf_metrics("deploy.yaml", "apiVersion: apps/v1\n"
                                              "kind: Deployment\n"
                                              "metadata:\n"
                                              "  name: my-app\n"
                                              "spec:\n"
                                              "  replicas: 3\n"
                                              "  selector:\n"
                                              "    matchLabels:\n"
                                              "      app: my-app\n"
                                              "  template:\n"
                                              "    metadata:\n"
                                              "      labels:\n"
                                              "        app: my-app\n"
                                              "    spec:\n"
                                              "      containers:\n"
                                              "      - name: app\n"
                                              "        image: my-app:latest\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: Deployment kind → Resource node via pass_k8s. */
    ASSERT_TRUE(m.resources >= 1);
    PASS();
}

/* K8s: two manifests in one directory → 2 Resource nodes (one per file). */
TEST(probe_k8s_two_manifests) {
    static const GpfFile files[] = {{"service.yaml", "apiVersion: v1\n"
                                                     "kind: Service\n"
                                                     "metadata:\n"
                                                     "  name: my-svc\n"
                                                     "spec:\n"
                                                     "  selector:\n"
                                                     "    app: my-app\n"
                                                     "  ports:\n"
                                                     "  - port: 80\n"},
                                    {"configmap.yaml", "apiVersion: v1\n"
                                                       "kind: ConfigMap\n"
                                                       "metadata:\n"
                                                       "  name: app-config\n"
                                                       "data:\n"
                                                       "  key: value\n"}};
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* GREEN: each manifest file produces its own Resource node. */
    ASSERT_TRUE(m.resources >= 2);
    PASS();
}

/* K8s: a Service routes to the workload whose pod template carries every
 * label of its selector (INFRA_MAPS Service -> workload). The fixture had no
 * pod labels and linked only through a "selector value == workload name"
 * guess that Kubernetes does not make. */
TEST(probe_k8s_infra_maps_edge) {
    static const GpfFile files[] = {{"deploy.yaml", "apiVersion: apps/v1\n"
                                                    "kind: Deployment\n"
                                                    "metadata:\n"
                                                    "  name: frontend\n"
                                                    "spec:\n"
                                                    "  template:\n"
                                                    "    metadata:\n"
                                                    "      labels:\n"
                                                    "        app: frontend\n"
                                                    "    spec:\n"
                                                    "      containers:\n"
                                                    "      - name: frontend\n"
                                                    "        image: frontend:latest\n"},
                                    {"service.yaml", "apiVersion: v1\n"
                                                     "kind: Service\n"
                                                     "metadata:\n"
                                                     "  name: frontend-svc\n"
                                                     "spec:\n"
                                                     "  selector:\n"
                                                     "    app: frontend\n"}};
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.infra_maps, 1);
    PASS();
}

/* A selector is the AND of its pairs: `tier: back` pods are not selected by a
 * `{app: web, tier: front}` Service. */
TEST(probe_k8s_selector_requires_all_pairs) {
    static const GpfFile files[] = {{"front.yaml", "apiVersion: apps/v1\n"
                                                   "kind: Deployment\n"
                                                   "metadata:\n"
                                                   "  name: web-front\n"
                                                   "spec:\n"
                                                   "  template:\n"
                                                   "    metadata:\n"
                                                   "      labels:\n"
                                                   "        app: web\n"
                                                   "        tier: front\n"},
                                    {"back.yaml", "apiVersion: apps/v1\n"
                                                  "kind: Deployment\n"
                                                  "metadata:\n"
                                                  "  name: web-back\n"
                                                  "spec:\n"
                                                  "  template:\n"
                                                  "    metadata:\n"
                                                  "      labels:\n"
                                                  "        app: web\n"
                                                  "        tier: back\n"},
                                    {"service.yaml", "apiVersion: v1\n"
                                                     "kind: Service\n"
                                                     "metadata:\n"
                                                     "  name: web\n"
                                                     "spec:\n"
                                                     "  selector:\n"
                                                     "    app: web\n"
                                                     "    tier: front\n"}};
    GpfMetrics m = gpf_metrics_files(files, 3);
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.infra_maps, 1);
    PASS();
}

/* Only the pod template's labels are selectable; the workload's own
 * metadata.labels are not. */
TEST(probe_k8s_selector_ignores_workload_metadata_labels) {
    static const GpfFile files[] = {{"deploy.yaml", "apiVersion: apps/v1\n"
                                                    "kind: Deployment\n"
                                                    "metadata:\n"
                                                    "  name: api\n"
                                                    "  labels:\n"
                                                    "    app: web\n"
                                                    "spec:\n"
                                                    "  template:\n"
                                                    "    metadata:\n"
                                                    "      labels:\n"
                                                    "        app: api\n"},
                                    {"service.yaml", "apiVersion: v1\n"
                                                     "kind: Service\n"
                                                     "metadata:\n"
                                                     "  name: web\n"
                                                     "spec:\n"
                                                     "  selector:\n"
                                                     "    app: web\n"}};
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.infra_maps, 0);
    PASS();
}

/* A workload whose NAME equals a selector value is not selected by it. */
TEST(probe_k8s_selector_no_name_guess) {
    static const GpfFile files[] = {{"deploy.yaml", "apiVersion: apps/v1\n"
                                                    "kind: Deployment\n"
                                                    "metadata:\n"
                                                    "  name: frontend\n"
                                                    "spec:\n"
                                                    "  template:\n"
                                                    "    metadata:\n"
                                                    "      labels:\n"
                                                    "        app: fe\n"},
                                    {"service.yaml", "apiVersion: v1\n"
                                                     "kind: Service\n"
                                                     "metadata:\n"
                                                     "  name: frontend\n"
                                                     "spec:\n"
                                                     "  selector:\n"
                                                     "    app: frontend\n"}};
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.infra_maps, 0);
    PASS();
}

/* `component: web` does not select pods labelled `app: web`. */
TEST(probe_k8s_selector_key_matters) {
    static const GpfFile files[] = {{"deploy.yaml", "apiVersion: apps/v1\n"
                                                    "kind: Deployment\n"
                                                    "metadata:\n"
                                                    "  name: api\n"
                                                    "spec:\n"
                                                    "  template:\n"
                                                    "    metadata:\n"
                                                    "      labels:\n"
                                                    "        app: web\n"},
                                    {"service.yaml", "apiVersion: v1\n"
                                                     "kind: Service\n"
                                                     "metadata:\n"
                                                     "  name: web\n"
                                                     "spec:\n"
                                                     "  selector:\n"
                                                     "    component: web\n"}};
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.infra_maps, 0);
    PASS();
}

/* A Service selects pods of its own namespace only. */
TEST(probe_k8s_selector_namespace_scoped) {
    static const GpfFile files[] = {{"deploy.yaml", "apiVersion: apps/v1\n"
                                                    "kind: Deployment\n"
                                                    "metadata:\n"
                                                    "  name: web\n"
                                                    "  namespace: team-b\n"
                                                    "spec:\n"
                                                    "  template:\n"
                                                    "    metadata:\n"
                                                    "      labels:\n"
                                                    "        app: web\n"},
                                    {"service.yaml", "apiVersion: v1\n"
                                                     "kind: Service\n"
                                                     "metadata:\n"
                                                     "  name: web\n"
                                                     "  namespace: team-a\n"
                                                     "spec:\n"
                                                     "  selector:\n"
                                                     "    app: web\n"}};
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.infra_maps, 0);
    PASS();
}

/* Selector linking recorded at most 512 manifests; a Service and workload
 * indexed after that limit (deckhouse has 2,003 manifest files) were never
 * linked. No count of manifests may decide graph content. */
TEST(probe_k8s_selector_beyond_512_manifests) {
    enum { FILLER = 600 };
    GpfFile *files = calloc(FILLER + 2, sizeof(*files));
    char (*names)[32] = calloc(FILLER, sizeof(*names));
    ASSERT_NOT_NULL(files);
    ASSERT_NOT_NULL(names);
    for (int i = 0; i < FILLER; i++) {
        snprintf(names[i], sizeof(names[i]), "a/cm-%03d.yaml", i);
        files[i].name = names[i];
        files[i].content = "apiVersion: v1\n"
                           "kind: ConfigMap\n"
                           "metadata:\n"
                           "  name: filler\n"
                           "data:\n"
                           "  k: v\n";
    }
    files[FILLER].name = "z/deploy.yaml";
    files[FILLER].content = "apiVersion: apps/v1\n"
                            "kind: Deployment\n"
                            "metadata:\n"
                            "  name: late\n"
                            "spec:\n"
                            "  template:\n"
                            "    metadata:\n"
                            "      labels:\n"
                            "        app: late\n";
    files[FILLER + 1].name = "z/service.yaml";
    files[FILLER + 1].content = "apiVersion: v1\n"
                                "kind: Service\n"
                                "metadata:\n"
                                "  name: late\n"
                                "spec:\n"
                                "  selector:\n"
                                "    app: late\n";
    GpfMetrics m = gpf_metrics_files(files, FILLER + 2);
    free(names);
    free(files);
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.infra_maps, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 3 — Kustomize (kustomization.yaml)
 *
 * Labels histogram: Module:1
 * pass_k8s.c handle_kustomize: emits a Module node + IMPORTS edges for each
 * resources/bases/patches entry that resolves to an existing graph node.
 * ══════════════════════════════════════════════════════════════════ */

/* Kustomize: overlay file → Module node. */
TEST(probe_kustomize_module_node) {
    GpfMetrics m = gpf_metrics("kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1beta1\n"
                                                     "kind: Kustomization\n"
                                                     "resources:\n"
                                                     "  - deploy.yaml\n"
                                                     "  - service.yaml\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: handle_kustomize always emits a Module node for the overlay file. */
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Kustomize: overlay + two resource manifests → IMPORTS edges.
 * The resolver looks for nodes whose QN matches the resource paths.
 * Each resolved entry produces an IMPORTS edge (Module → Resource node).
 * RED: resources are YAML files that pass_k8s processes as k8s manifests only
 *      when apiVersion is present; without it they are plain YAML with no node
 *      in the graph, so the IMPORTS edge can't be created. */
TEST(probe_kustomize_imports_edges) {
    static const GpfFile files[] = {{"kustomization.yaml",
                                     "apiVersion: kustomize.config.k8s.io/v1beta1\n"
                                     "kind: Kustomization\n"
                                     "resources:\n"
                                     "  - deploy.yaml\n"
                                     "  - service.yaml\n"},
                                    {"deploy.yaml", "apiVersion: apps/v1\n"
                                                    "kind: Deployment\n"
                                                    "metadata:\n"
                                                    "  name: myapp\n"
                                                    "spec:\n"
                                                    "  replicas: 1\n"
                                                    "  template:\n"
                                                    "    spec:\n"
                                                    "      containers:\n"
                                                    "      - name: app\n"
                                                    "        image: myapp:v1\n"},
                                    {"service.yaml", "apiVersion: v1\n"
                                                     "kind: Service\n"
                                                     "metadata:\n"
                                                     "  name: myapp-svc\n"
                                                     "spec:\n"
                                                     "  selector:\n"
                                                     "    app: myapp\n"
                                                     "  ports:\n"
                                                     "  - port: 80\n"}};
    GpfMetrics m = gpf_metrics_files(files, 3);
    ASSERT_TRUE(m.ok);
    /* Kustomize Module node is always emitted. */
    ASSERT_TRUE(m.modules >= 1);
    /* RED: IMPORTS edge from kustomize Module → Resource nodes requires the
     *      resource file to already be in the graph as a QN-matched node;
     *      the ordering of pipeline passes may leave resource nodes absent
     *      when the kustomize pass runs, preventing edge creation. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — resource QN resolution fails */
    PASS();
}

/* Kustomize: base overlay with patches entry → Module node (no crash). */
TEST(probe_kustomize_with_patches) {
    GpfMetrics m = gpf_metrics("kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1beta1\n"
                                                     "kind: Kustomization\n"
                                                     "bases:\n"
                                                     "  - ../base\n"
                                                     "patches:\n"
                                                     "  - path: replica-patch.yaml\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: handle_kustomize emits Module node regardless of patches content. */
    ASSERT_TRUE(m.modules >= 1);
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* A comment between `---` and the mapping (the `# yaml-language-server:
 * $schema=...` line editors add) is the document's first named child; the
 * manifest behind it is still a manifest. onedr0p/home-ops opens 428 of its
 * manifests this way and got a Resource for 26 of 390 objects. */
TEST(probe_k8s_manifest_after_leading_comment) {
    GpfMetrics m = gpf_metrics(
        "helmrelease.yaml", "---\n"
                            "# yaml-language-server: $schema=https://example.com/helmrelease.json\n"
                            "apiVersion: helm.toolkit.fluxcd.io/v2\n"
                            "kind: HelmRelease\n"
                            "metadata:\n"
                            "  name: echo\n"
                            "spec:\n"
                            "  interval: 1h\n");
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.resources, 1);
    PASS();
}

/* A comment between a key and its block (`metadata: # owned by ...`) sits
 * ahead of the value; the object still has its name. */
TEST(probe_k8s_manifest_metadata_after_key_line_comment) {
    GpfMetrics m = gpf_metrics("deploy.yaml", "apiVersion: apps/v1\n"
                                              "kind: Deployment\n"
                                              "metadata: # owned by the platform team\n"
                                              "  name: web\n"
                                              "spec:\n"
                                              "  replicas: 1\n");
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.resources, 1);
    PASS();
}

/* ── Identity: every document is an object (IDENTITY.md) ──────────── */

/* The QN and properties of the Resource named `name` after indexing `files`;
 * false when there is no such node or it is not unique. */
static bool gpf_resource(const GpfFile *files, int nfiles, const char *name, char *qn, size_t qn_sz,
                         char *props, size_t props_sz) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    bool found = false;
    int hits = 0;
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (store && cbm_store_find_nodes_by_label(store, lp.project, "Resource", &nodes, &count) ==
                     CBM_STORE_OK) {
        for (int i = 0; i < count; i++) {
            if (nodes[i].name && strcmp(nodes[i].name, name) == 0) {
                hits++;
                const char *q = nodes[i].qualified_name ? nodes[i].qualified_name : "";
                const char *sep = strstr(q, "::");
                snprintf(qn, qn_sz, "%s", sep ? sep : q); /* the part after the file base */
                snprintf(props, props_sz, "%s",
                         nodes[i].properties_json ? nodes[i].properties_json : "{}");
            }
        }
        cbm_store_free_nodes(nodes, count);
        found = hits == 1;
    }
    gpf_cleanup(&lp, store);
    return found;
}

/* A multi-document manifest is a list of objects: kubernetes-sigs and
 * microservices-demo ship a Deployment and its Service in one file, and
 * 3,139 objects of the 17 infra corpora sat behind a file's first `---`. */
TEST(probe_k8s_every_document_is_an_object) {
    GpfMetrics m = gpf_metrics("app.yaml", "apiVersion: apps/v1\n"
                                           "kind: Deployment\n"
                                           "metadata:\n"
                                           "  name: web\n"
                                           "---\n"
                                           "apiVersion: v1\n"
                                           "kind: Service\n"
                                           "metadata:\n"
                                           "  name: web\n"
                                           "---\n"
                                           "apiVersion: v1\n"
                                           "kind: ConfigMap\n"
                                           "metadata:\n"
                                           "  name: web-config\n");
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.resources, 3);
    PASS();
}

/* ...and the Service in the second document selects the Deployment in the
 * first. */
TEST(probe_k8s_selector_across_documents) {
    GpfMetrics m = gpf_metrics("app.yaml", "apiVersion: apps/v1\n"
                                           "kind: Deployment\n"
                                           "metadata:\n"
                                           "  name: web\n"
                                           "spec:\n"
                                           "  template:\n"
                                           "    metadata:\n"
                                           "      labels:\n"
                                           "        app: web\n"
                                           "---\n"
                                           "apiVersion: v1\n"
                                           "kind: Service\n"
                                           "metadata:\n"
                                           "  name: web\n"
                                           "spec:\n"
                                           "  selector:\n"
                                           "    app: web\n");
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.resources, 2);
    ASSERT_EQ(m.infra_maps, 1);
    PASS();
}

/* ── Secret hygiene: config values that are secret material stay out ── */

/* Does any node name / QN / properties or edge properties contain `needle`? */
static int gpf_graph_mentions(const GpfFile *files, int nfiles, const char *needle) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    int hits = -1;
    if (store) {
        hits = 0;
        static const char *const sql[] = {
            "SELECT COUNT(*) FROM nodes WHERE project=?1 AND (instr(name,?2) OR "
            "instr(qualified_name,?2) OR instr(properties,?2))",
            "SELECT COUNT(*) FROM edges WHERE project=?1 AND instr(properties,?2)"};
        for (size_t q = 0; q < sizeof(sql) / sizeof(sql[0]); q++) {
            sqlite3_stmt *st = NULL;
            if (sqlite3_prepare_v2(cbm_store_get_db(store), sql[q], -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
                sqlite3_bind_text(st, 2, needle, -1, SQLITE_STATIC);
                if (sqlite3_step(st) == SQLITE_ROW) {
                    hits += sqlite3_column_int(st, 0);
                }
            }
            sqlite3_finalize(st);
        }
    }
    gpf_cleanup(&lp, store);
    return hits;
}

/* A Secret's stringData is secret material, whatever its keys are called. */
TEST(probe_secret_stringdata_mints_no_route) {
    static const GpfFile files[] = {
        {"alerting-secret.yaml", "apiVersion: v1\n"
                                 "kind: Secret\n"
                                 "metadata:\n"
                                 "  name: alerting\n"
                                 "stringData:\n"
                                 "  webhook_url: https://hooks.example.com/T0/B0/s3cr3tpath\n"}};
    ASSERT_EQ(gpf_graph_mentions(files, 1, "s3cr3tpath"), 0);
    PASS();
}

/* An endpoint keeps its path; the credentials and query riding on it stay
 * out of the graph. */
TEST(probe_endpoint_url_loses_credentials) {
    static const GpfFile files[] = {{"subscription.yaml",
                                     "subscription:\n"
                                     "  push_endpoint: https://svc:hunter2@svc.example.com/push"
                                     "?token=abc123\n"}};
    GpfMetrics m = gpf_metrics_files(files, 1);
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.routes, 1);
    ASSERT_EQ(gpf_graph_mentions(files, 1, "hunter2"), 0);
    ASSERT_EQ(gpf_graph_mentions(files, 1, "abc123"), 0);
    ASSERT_TRUE(gpf_graph_mentions(files, 1, "https://svc.example.com/push") > 0);
    PASS();
}

/* .tfvars assigns values -- a variable file is where secrets live; the
 * declaration in the .tf is what the graph keeps. */
TEST(probe_tfvars_values_mint_no_route) {
    static const GpfFile files[] = {
        {"prod.tfvars", "push_endpoint = \"https://svc.example.com/tfvars-only-path\"\n"}};
    ASSERT_EQ(gpf_graph_mentions(files, 1, "tfvars-only-path"), 0);
    PASS();
}

/* ── Object references (Phase 1, PHASE1.md) ────────────────────────── */

/* Edges of `type` between two Resources whose properties carry `prop`
 * (e.g. "\"ref\":\"volume\""); -1 when indexing failed. */
static int gpf_resource_edges(const GpfFile *files, int nfiles, const char *type,
                              const char *prop) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    int n = -1;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
            "JOIN nodes t ON t.id=e.target_id WHERE e.project=?1 AND e.type=?2 "
            "AND s.label='Resource' AND t.label='Resource' AND instr(e.properties,?3)";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, type, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 3, prop, -1, SQLITE_STATIC);
            n = sqlite3_step(st) == SQLITE_ROW ? sqlite3_column_int(st, 0) : -1;
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return n;
}

static const char k_pod_refs_app[] =
    "apiVersion: apps/v1\n"
    "kind: Deployment\n"
    "metadata:\n"
    "  name: web\n"
    "spec:\n"
    "  template:\n"
    "    spec:\n"
    "      serviceAccountName: web-sa\n"
    "      imagePullSecrets:\n"
    "        - name: registry-creds\n"
    "      volumes:\n"
    "        - name: cfg\n"
    "          configMap:\n"
    "            name: web-config\n"
    "        - name: data\n"
    "          persistentVolumeClaim:\n"
    "            claimName: web-data\n"
    "      containers:\n"
    "        - name: web\n"
    "          env:\n"
    "            - name: DB_PASSWORD\n"
    "              valueFrom:\n"
    "                secretKeyRef:\n"
    "                  name: web-db\n"
    "                  key: password\n"
    "          envFrom:\n"
    "            - configMapRef:\n"
    "                name: web-env\n"
    "---\n"
    "apiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: web-sa\n"
    "---\n"
    "apiVersion: v1\nkind: Secret\nmetadata:\n  name: registry-creds\n"
    "---\n"
    "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: web-config\n"
    "---\n"
    "apiVersion: v1\nkind: PersistentVolumeClaim\nmetadata:\n  name: web-data\n"
    "---\n"
    "apiVersion: v1\nkind: Secret\nmetadata:\n  name: web-db\n"
    "---\n"
    "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: web-env\n";

/* A workload's pod spec names the objects it runs with: its service
 * account, pull secret, mounted ConfigMap and claim, env sources. */
TEST(probe_k8s_pod_spec_references) {
    static const GpfFile files[] = {{"app.yaml", k_pod_refs_app}};
    /* WHY: a directory or shared YAML document does not select an apply domain for
     * an occurrence proof. Tried the original six references unchanged; explicit flat
     * membership is the supported counterpart and must retain all six exact
     * declarations. These rows speak of occurrences only, here and in every Kubernetes
     * and Helm probe below: pass_k8s still draws an edge by its own rule, which
     * projection keeps by its marker. resolver_infra pins those edges. */
    const RQ_Forbidden unconfigured[] = {{.case_id = "unconfigured-pod-web-sa",
                                          .site = {.file = "app.yaml",
                                                   .caller = "app.yaml::apps/Deployment/-/web",
                                                   .start_byte = 113,
                                                   .end_byte = 119},
                                          .relation = "USAGE",
                                          .any_target = true},
                                         {.case_id = "unconfigured-pod-registry-creds",
                                          .site = {.file = "app.yaml",
                                                   .caller = "app.yaml::apps/Deployment/-/web",
                                                   .start_byte = 160,
                                                   .end_byte = 174},
                                          .relation = "USAGE",
                                          .any_target = true},
                                         {.case_id = "unconfigured-pod-web-config",
                                          .site = {.file = "app.yaml",
                                                   .caller = "app.yaml::apps/Deployment/-/web",
                                                   .start_byte = 249,
                                                   .end_byte = 259},
                                          .relation = "USAGE",
                                          .any_target = true},
                                         {.case_id = "unconfigured-pod-web-data",
                                          .site = {.file = "app.yaml",
                                                   .caller = "app.yaml::apps/Deployment/-/web",
                                                   .start_byte = 337,
                                                   .end_byte = 345},
                                          .relation = "USAGE",
                                          .any_target = true},
                                         {.case_id = "unconfigured-pod-web-db",
                                          .site = {.file = "app.yaml",
                                                   .caller = "app.yaml::apps/Deployment/-/web",
                                                   .start_byte = 510,
                                                   .end_byte = 516},
                                          .relation = "USAGE",
                                          .any_target = true},
                                         {.case_id = "unconfigured-pod-web-env",
                                          .site = {.file = "app.yaml",
                                                   .caller = "app.yaml::apps/Deployment/-/web",
                                                   .start_byte = 618,
                                                   .end_byte = 625},
                                          .relation = "USAGE",
                                          .any_target = true}};
    ASSERT_TRUE(gpf_infra_quality(files, 1, "probe_k8s_pod_spec_references/unconfigured", NULL, 0,
                                  unconfigured, 6));
    const GpfFile configured[] = {files[0], {"kustomization.yaml", "resources:\n  - app.yaml\n"}};
    const RQ_Expected expected[] = {
        {.case_id = "configured-pod-web-sa",
         .site = {.file = "app.yaml",
                  .caller = "app.yaml::apps/Deployment/-/web",
                  .start_byte = 113,
                  .end_byte = 119},
         .relation = "USAGE",
         .target = {.file = "app.yaml",
                    .suffix = "app.yaml::core/ServiceAccount/-/web-sa",
                    .label = "Resource",
                    .declaration_line = 33,
                    .declaration_text = "  name: web-sa"},
         .mode = RQ_REQUIRED},
        {.case_id = "configured-pod-registry-creds",
         .site = {.file = "app.yaml",
                  .caller = "app.yaml::apps/Deployment/-/web",
                  .start_byte = 160,
                  .end_byte = 174},
         .relation = "USAGE",
         .target = {.file = "app.yaml",
                    .suffix = "app.yaml::core/Secret/-/registry-creds",
                    .label = "Resource",
                    .declaration_line = 38,
                    .declaration_text = "  name: registry-creds"},
         .mode = RQ_REQUIRED},
        {.case_id = "configured-pod-web-config",
         .site = {.file = "app.yaml",
                  .caller = "app.yaml::apps/Deployment/-/web",
                  .start_byte = 249,
                  .end_byte = 259},
         .relation = "USAGE",
         .target = {.file = "app.yaml",
                    .suffix = "app.yaml::core/ConfigMap/-/web-config",
                    .label = "Resource",
                    .declaration_line = 43,
                    .declaration_text = "  name: web-config"},
         .mode = RQ_REQUIRED},
        {.case_id = "configured-pod-web-data",
         .site = {.file = "app.yaml",
                  .caller = "app.yaml::apps/Deployment/-/web",
                  .start_byte = 337,
                  .end_byte = 345},
         .relation = "USAGE",
         .target = {.file = "app.yaml",
                    .suffix = "app.yaml::core/PersistentVolumeClaim/-/web-data",
                    .label = "Resource",
                    .declaration_line = 48,
                    .declaration_text = "  name: web-data"},
         .mode = RQ_REQUIRED},
        {.case_id = "configured-pod-web-db",
         .site = {.file = "app.yaml",
                  .caller = "app.yaml::apps/Deployment/-/web",
                  .start_byte = 510,
                  .end_byte = 516},
         .relation = "USAGE",
         .target = {.file = "app.yaml",
                    .suffix = "app.yaml::core/Secret/-/web-db",
                    .label = "Resource",
                    .declaration_line = 53,
                    .declaration_text = "  name: web-db"},
         .mode = RQ_REQUIRED},
        {.case_id = "configured-pod-web-env",
         .site = {.file = "app.yaml",
                  .caller = "app.yaml::apps/Deployment/-/web",
                  .start_byte = 618,
                  .end_byte = 625},
         .relation = "USAGE",
         .target = {.file = "app.yaml",
                    .suffix = "app.yaml::core/ConfigMap/-/web-env",
                    .label = "Resource",
                    .declaration_line = 58,
                    .declaration_text = "  name: web-env"},
         .mode = RQ_REQUIRED}};
    ASSERT_TRUE(gpf_infra_quality(configured, 2, "probe_k8s_pod_spec_references/configured",
                                  expected, 6, NULL, 0));
    PASS();
}

/* RBAC: a binding uses the role it names and the service accounts it
 * grants; an Ingress routes to its backend Service; an HPA scales its
 * target; a webhook configuration calls its Service. */
TEST(probe_k8s_rbac_ingress_hpa_webhook_references) {
    static const GpfFile files[] = {
        {"rbac.yaml",
         "apiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRole\nmetadata:\n  name: reader\n"
         "---\n"
         "apiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: web-sa\n  namespace: prod\n"
         "---\n"
         "apiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRoleBinding\n"
         "metadata:\n  name: web-reader\n"
         "roleRef:\n  apiGroup: rbac.authorization.k8s.io\n  kind: ClusterRole\n  name: reader\n"
         "subjects:\n  - kind: ServiceAccount\n    name: web-sa\n    namespace: prod\n"},
        {"web.yaml",
         "apiVersion: v1\nkind: Service\nmetadata:\n  name: web\n  namespace: prod\n"
         "---\n"
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  namespace: prod\n"
         "---\n"
         "apiVersion: networking.k8s.io/v1\nkind: Ingress\nmetadata:\n  name: web\n"
         "  namespace: prod\nspec:\n  rules:\n    - host: web.example.com\n      http:\n"
         "        paths:\n          - path: /\n            pathType: Prefix\n"
         "            backend:\n              service:\n                name: web\n"
         "                port:\n                  number: 80\n"
         "---\n"
         "apiVersion: autoscaling/v2\nkind: HorizontalPodAutoscaler\nmetadata:\n  name: web\n"
         "  namespace: prod\nspec:\n  scaleTargetRef:\n    apiVersion: apps/v1\n"
         "    kind: Deployment\n    name: web\n"
         "---\n"
         "apiVersion: admissionregistration.k8s.io/v1\nkind: ValidatingWebhookConfiguration\n"
         "metadata:\n  name: web-hook\nwebhooks:\n  - name: validate.web.example.com\n"
         "    clientConfig:\n      service:\n        name: web\n        namespace: prod\n"}};
    /* WHY: these three schema-defined references are genuine quality debt.
     * Tried exact declarations and an explicit flat build; RBAC/HPA are outside
     * the audited source/target kind subset. Routing contracts remain below. */
    const GpfFile configured[] = {
        files[0], files[1], {"kustomization.yaml", "resources:\n  - rbac.yaml\n  - web.yaml\n"}};
    const RQ_Expected expected[] = {
        {.case_id = "rbac-role",
         .site = {.file = "rbac.yaml",
                  .caller = "rbac.yaml::rbac.authorization.k8s.io/ClusterRoleBinding/-/web-reader",
                  .start_byte = 341,
                  .end_byte = 347},
         .relation = "USAGE",
         .target = {.file = "rbac.yaml",
                    .suffix = "rbac.yaml::rbac.authorization.k8s.io/ClusterRole/-/reader",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: reader"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "k8s_rbac_reference_proof_missing",
         .why = "The roleRef fixes the API group, cluster-scoped kind and name; the "
                "source-certified proof adapter does not cover RBAC.",
         .tried = "Preserved original source bytes; explicit build membership and exact "
                  "declaration/site goldens run with sequential and parallel drivers. Wrong "
                  "targets and roles remain failures."},
        {.case_id = "rbac-subject",
         .site = {.file = "rbac.yaml",
                  .caller = "rbac.yaml::rbac.authorization.k8s.io/ClusterRoleBinding/-/web-reader",
                  .start_byte = 393,
                  .end_byte = 399},
         .relation = "USAGE",
         .target = {.file = "rbac.yaml",
                    .suffix = "rbac.yaml::core/ServiceAccount/prod/web-sa",
                    .label = "Resource",
                    .declaration_line = 9,
                    .declaration_text = "  name: web-sa"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "k8s_rbac_subject_proof_missing",
         .why = "The ServiceAccount subject fixes name and namespace; the RBAC binding is outside "
                "the audited reference adapter.",
         .tried = "Preserved original source bytes; explicit build membership and exact "
                  "declaration/site goldens run with sequential and parallel drivers. Wrong "
                  "targets and roles remain failures."},
        {.case_id = "hpa-scale-target",
         .site = {.file = "web.yaml",
                  .caller = "web.yaml::autoscaling/HorizontalPodAutoscaler/prod/web",
                  .start_byte = 661,
                  .end_byte = 664},
         .relation = "USAGE",
         .target = {.file = "web.yaml",
                    .suffix = "web.yaml::apps/Deployment/prod/web",
                    .label = "Resource",
                    .declaration_line = 10,
                    .declaration_text = "  name: web"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "k8s_hpa_scale_target_proof_missing",
         .why = "The scaleTargetRef fixes apps/v1 Deployment web in the HPA namespace; the source "
                "adapter does not yet certify HPA references.",
         .tried = "Preserved original source bytes; explicit build membership and exact "
                  "declaration/site goldens run with sequential and parallel drivers. Wrong "
                  "targets and roles remain failures."}};
    const RQ_Forbidden forbidden[] = {
        {.case_id = "hpa-service-is-not-scale-target",
         .site = {.file = "web.yaml",
                  .caller = "web.yaml::autoscaling/HorizontalPodAutoscaler/prod/web",
                  .start_byte = 661,
                  .end_byte = 664},
         .relation = "USAGE",
         .target = {.file = "web.yaml",
                    .suffix = "web.yaml::core/Service/prod/web",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: web"}},
        {.case_id = "role-is-not-subject",
         .site = {.file = "rbac.yaml",
                  .caller = "rbac.yaml::rbac.authorization.k8s.io/ClusterRoleBinding/-/web-reader",
                  .start_byte = 393,
                  .end_byte = 399},
         .relation = "USAGE",
         .target = {.file = "rbac.yaml",
                    .suffix = "rbac.yaml::rbac.authorization.k8s.io/ClusterRole/-/reader",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: reader"}},
        {.case_id = "subject-is-not-role",
         .site = {.file = "rbac.yaml",
                  .caller = "rbac.yaml::rbac.authorization.k8s.io/ClusterRoleBinding/-/web-reader",
                  .start_byte = 341,
                  .end_byte = 347},
         .relation = "USAGE",
         .target = {.file = "rbac.yaml",
                    .suffix = "rbac.yaml::core/ServiceAccount/prod/web-sa",
                    .label = "Resource",
                    .declaration_line = 9,
                    .declaration_text = "  name: web-sa"}}};
    ASSERT_TRUE(gpf_infra_quality(configured, 3, "probe_k8s_rbac_ingress_hpa_webhook_references",
                                  expected, 3, forbidden, 3));
    ASSERT_EQ(gpf_resource_edges(files, 2, "INFRA_MAPS", "\"kind\":\"ingress\""), 1);
    ASSERT_EQ(gpf_resource_edges(files, 2, "INFRA_MAPS", "\"kind\":\"webhook\""), 1);
    PASS();
}

/* A custom resource uses its selected CRD declaration. Without an explicit
 * build, repository co-location alone does not select the deployed CRD. */
TEST(probe_k8s_cr_uses_its_crd_and_unresolved_draws_nothing) {
    static const GpfFile files[] = {
        {"crd.yaml", "apiVersion: apiextensions.k8s.io/v1\nkind: CustomResourceDefinition\n"
                     "metadata:\n  name: widgets.example.com\nspec:\n  group: example.com\n"
                     "  names:\n    kind: Widget\n    plural: widgets\n  scope: Namespaced\n"},
        {"widget.yaml", "apiVersion: example.com/v1\nkind: Widget\nmetadata:\n  name: w1\n"
                        "---\n"
                        "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: lonely\nspec:\n"
                        "  template:\n    spec:\n      serviceAccountName: not-in-repo\n"}};
    const RQ_Forbidden unconfigured = {.case_id = "crd-unconfigured-no-build",
                                       .site = {.file = "widget.yaml",
                                                .caller = "widget.yaml::example.com/Widget/-/w1",
                                                .needle = "Widget"},
                                       .relation = "USAGE",
                                       .any_target = true};
    ASSERT_TRUE(gpf_infra_quality(files, 2, "probe_k8s_cr_uses_its_crd/unconfigured", NULL, 0,
                                  &unconfigured, 1));
    const GpfFile configured[] = {
        files[0], files[1], {"kustomization.yaml", "resources:\n  - crd.yaml\n  - widget.yaml\n"}};
    const RQ_Expected expected = {
        .case_id = "crd-selected-declaration",
        .site = {.file = "widget.yaml",
                 .caller = "widget.yaml::example.com/Widget/-/w1",
                 .needle = "Widget"},
        .relation = "USAGE",
        .target = {.file = "crd.yaml",
                   .suffix = "crd.yaml::apiextensions.k8s.io/CustomResourceDefinition/-/"
                             "widgets.example.com",
                   .label = "Resource",
                   .declaration_line = 4,
                   .declaration_text = "  name: widgets.example.com"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "k8s_crd_declaration_source_scope_missing",
        .why = "The explicit build selects the unique CRD for the resource group/kind; the "
               "immutable CRD binding adapter is still missing.",
        .tried = "Preserved the original resource and declaration; added explicit build "
                 "membership and run exact endpoints with both pipeline drivers."};
    const RQ_Forbidden unresolved = {.case_id = "service-account-absent",
                                     .site = {.file = "widget.yaml",
                                              .caller = "widget.yaml::apps/Deployment/-/lonely",
                                              .needle = "not-in-repo"},
                                     .relation = "USAGE",
                                     .any_target = true};
    ASSERT_TRUE(gpf_infra_quality(configured, 3, "probe_k8s_cr_uses_its_crd/configured", &expected,
                                  1, &unresolved, 1));
    PASS();
}

/* Explicit build membership selects the mounted ConfigMap; directory
 * proximity by itself supplies no authority. */
TEST(probe_k8s_reference_stays_in_bundle) {
    static const GpfFile files[] = {
        {"a/app.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\nspec:\n  template:\n"
         "    spec:\n      volumes:\n        - name: cfg\n          configMap:\n"
         "            name: web-config\n"
         "---\n"
         "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: web-config\n"},
        {"b/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: web-config\n"}};
    /* WHY: directory proximity cannot select between these same-named objects.
     * Tried both original files unchanged; the explicit build selects only a/. */
    const RQ_Forbidden unconfigured[] = {{.case_id = "unconfigured-bundle-candidate",
                                          .site = {.file = "a/app.yaml",
                                                   .caller = "a.app.yaml::apps/Deployment/-/web",
                                                   .start_byte = 161,
                                                   .end_byte = 171},
                                          .relation = "USAGE",
                                          .any_target = true}};
    ASSERT_TRUE(gpf_infra_quality(files, 2, "probe_k8s_reference_stays_in_bundle/unconfigured",
                                  NULL, 0, unconfigured, 1));
    const GpfFile configured[] = {
        files[0], files[1], {"a/kustomization.yaml", "resources:\n  - app.yaml\n"}};
    const RQ_Expected expected[] = {{.case_id = "configured-bundle-exact-configmap",
                                     .site = {.file = "a/app.yaml",
                                              .caller = "a.app.yaml::apps/Deployment/-/web",
                                              .start_byte = 161,
                                              .end_byte = 171},
                                     .relation = "USAGE",
                                     .target = {.file = "a/app.yaml",
                                                .suffix = "a.app.yaml::core/ConfigMap/-/web-config",
                                                .label = "Resource",
                                                .declaration_line = 16,
                                                .declaration_text = "  name: web-config"},
                                     .mode = RQ_REQUIRED}};
    const RQ_Forbidden decoy[] = {{.case_id = "other-build-configmap",
                                   .site = {.file = "a/app.yaml",
                                            .caller = "a.app.yaml::apps/Deployment/-/web",
                                            .start_byte = 161,
                                            .end_byte = 171},
                                   .relation = "USAGE",
                                   .target = {.file = "b/cm.yaml",
                                              .suffix = "b.cm.yaml::core/ConfigMap/-/web-config",
                                              .label = "Resource",
                                              .declaration_line = 4,
                                              .declaration_text = "  name: web-config"}}};
    ASSERT_TRUE(gpf_infra_quality(configured, 3, "probe_k8s_reference_stays_in_bundle/configured",
                                  expected, 1, decoy, 1));
    PASS();
}

/* Version alternatives can coexist in the source tree. Each explicit build
 * selects one declaration; sharing a file does not establish that selection. */
TEST(probe_k8s_reference_prefers_its_own_file) {
    static const char v1[] =
        "apiVersion: apps/v1\nkind: DaemonSet\nmetadata:\n  name: canal\nspec:\n  template:\n"
        "    spec:\n      volumes:\n        - name: cfg\n          configMap:\n"
        "            name: canal-config\n"
        "---\n"
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: canal-config\n";
    static const GpfFile files[] = {{"canal/canal_v1.yaml", v1}, {"canal/canal_v2.yaml", v1}};
    /* WHY: colocated version alternatives require an explicit selected build.
     * Tried each original file in turn, with the other version still indexed.
     * Separate runs model the two valid configurations without root escapes. */
    const RQ_Forbidden unconfigured[] = {
        {.case_id = "unconfigured-version-1",
         .site = {.file = "canal/canal_v1.yaml",
                  .caller = "canal.canal_v1.yaml::apps/DaemonSet/-/canal",
                  .start_byte = 162,
                  .end_byte = 174},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "unconfigured-version-2",
         .site = {.file = "canal/canal_v2.yaml",
                  .caller = "canal.canal_v2.yaml::apps/DaemonSet/-/canal",
                  .start_byte = 162,
                  .end_byte = 174},
         .relation = "USAGE",
         .any_target = true}};
    ASSERT_TRUE(gpf_infra_quality(files, 2, "probe_k8s_reference_prefers_its_own_file/unconfigured",
                                  NULL, 0, unconfigured, 2));
    const GpfFile configured_1[] = {
        files[0], files[1], {"canal/kustomization.yaml", "resources:\n  - canal_v1.yaml\n"}};
    const RQ_Expected expected_1[] = {
        {.case_id = "selected-version-1",
         .site = {.file = "canal/canal_v1.yaml",
                  .caller = "canal.canal_v1.yaml::apps/DaemonSet/-/canal",
                  .start_byte = 162,
                  .end_byte = 174},
         .relation = "USAGE",
         .target = {.file = "canal/canal_v1.yaml",
                    .suffix = "canal.canal_v1.yaml::core/ConfigMap/-/canal-config",
                    .label = "Resource",
                    .declaration_line = 16,
                    .declaration_text = "  name: canal-config"},
         .mode = RQ_REQUIRED}};
    const RQ_Forbidden forbidden_1[] = {
        {.case_id = "wrong-version-2",
         .site = {.file = "canal/canal_v1.yaml",
                  .caller = "canal.canal_v1.yaml::apps/DaemonSet/-/canal",
                  .start_byte = 162,
                  .end_byte = 174},
         .relation = "USAGE",
         .target = {.file = "canal/canal_v2.yaml",
                    .suffix = "canal.canal_v2.yaml::core/ConfigMap/-/canal-config",
                    .label = "Resource",
                    .declaration_line = 16,
                    .declaration_text = "  name: canal-config"}},
        {.case_id = "unselected-version-2",
         .site = {.file = "canal/canal_v2.yaml",
                  .caller = "canal.canal_v2.yaml::apps/DaemonSet/-/canal",
                  .start_byte = 162,
                  .end_byte = 174},
         .relation = "USAGE",
         .any_target = true}};
    ASSERT_TRUE(gpf_infra_quality(configured_1, 3,
                                  "probe_k8s_reference_prefers_its_own_file/selected-1", expected_1,
                                  1, forbidden_1, 2));
    const GpfFile configured_2[] = {
        files[0], files[1], {"canal/kustomization.yaml", "resources:\n  - canal_v2.yaml\n"}};
    const RQ_Expected expected_2[] = {
        {.case_id = "selected-version-2",
         .site = {.file = "canal/canal_v2.yaml",
                  .caller = "canal.canal_v2.yaml::apps/DaemonSet/-/canal",
                  .start_byte = 162,
                  .end_byte = 174},
         .relation = "USAGE",
         .target = {.file = "canal/canal_v2.yaml",
                    .suffix = "canal.canal_v2.yaml::core/ConfigMap/-/canal-config",
                    .label = "Resource",
                    .declaration_line = 16,
                    .declaration_text = "  name: canal-config"},
         .mode = RQ_REQUIRED}};
    const RQ_Forbidden forbidden_2[] = {
        {.case_id = "wrong-version-1",
         .site = {.file = "canal/canal_v2.yaml",
                  .caller = "canal.canal_v2.yaml::apps/DaemonSet/-/canal",
                  .start_byte = 162,
                  .end_byte = 174},
         .relation = "USAGE",
         .target = {.file = "canal/canal_v1.yaml",
                    .suffix = "canal.canal_v1.yaml::core/ConfigMap/-/canal-config",
                    .label = "Resource",
                    .declaration_line = 16,
                    .declaration_text = "  name: canal-config"}},
        {.case_id = "unselected-version-1",
         .site = {.file = "canal/canal_v1.yaml",
                  .caller = "canal.canal_v1.yaml::apps/DaemonSet/-/canal",
                  .start_byte = 162,
                  .end_byte = 174},
         .relation = "USAGE",
         .any_target = true}};
    ASSERT_TRUE(gpf_infra_quality(configured_2, 3,
                                  "probe_k8s_reference_prefers_its_own_file/selected-2", expected_2,
                                  1, forbidden_2, 2));
    PASS();
}

/* ── Go operator layer (P1.2) ───────────────────────────────────────── */

/* Edges of `type` whose properties carry `prop`, between nodes of
 * `src_label` and `tgt_label`; -1 when indexing failed. */
static int gpf_typed_edges(const GpfFile *files, int nfiles, const char *type,
                           const char *src_label, const char *tgt_label, const char *prop) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    int n = -1;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql = "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
                          "JOIN nodes t ON t.id=e.target_id WHERE e.project=?1 AND e.type=?2 "
                          "AND s.label=?3 AND t.label=?4 AND instr(e.properties,?5)";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, type, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 3, src_label, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 4, tgt_label, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 5, prop, -1, SQLITE_STATIC);
            n = sqlite3_step(st) == SQLITE_ROW ? sqlite3_column_int(st, 0) : -1;
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return n;
}

static const GpfFile k_operator_repo[] = {
    {"go.mod", "module example.com/proj\n\ngo 1.22\n"},
    {"api/v1/doc.go", "// +groupName=example.com\npackage v1\n"},
    {"api/v1/widget_types.go", "package v1\n\ntype Widget struct {\n\tName string\n}\n\n"
                               "type Gadget struct {\n\tName string\n}\n"},
    {"config/crd/widgets.yaml",
     "apiVersion: apiextensions.k8s.io/v1\nkind: CustomResourceDefinition\n"
     "metadata:\n  name: widgets.example.com\nspec:\n  group: example.com\n"
     "  names:\n    kind: Widget\n    plural: widgets\n  scope: Namespaced\n"},
    {"internal/controller/widget.go", "package controller\n\n"
                                      "import (\n\texamplev1 \"example.com/proj/api/v1\"\n"
                                      "\tctrl \"sigs.k8s.io/controller-runtime\"\n)\n\n"
                                      "func SetupWithManager(mgr ctrl.Manager) error {\n"
                                      "\treturn ctrl.NewControllerManagedBy(mgr).\n"
                                      "\t\tFor(&examplev1.Widget{}).\n"
                                      "\t\tOwns(&examplev1.Gadget{}).\n"
                                      "\t\tComplete(nil)\n"
                                      "}\n"},
    {"config/embed.go",
     "package config\n\nimport \"embed\"\n\n//go:embed crd/*\nvar CRDs embed.FS\n"}};

/* A controller reconciles the kind it is built For, and the kinds it Owns. */
TEST(probe_go_controller_handles_its_kinds) {
    ASSERT_EQ(
        gpf_typed_edges(k_operator_repo, 6, "HANDLES", "Function", "Struct", "\"role\":\"for\""),
        1);
    ASSERT_EQ(
        gpf_typed_edges(k_operator_repo, 6, "HANDLES", "Function", "Struct", "\"role\":\"owns\""),
        1);
    PASS();
}

/* The CRD of group example.com / kind Widget is backed by `type Widget`
 * in the package marked +groupName=example.com; a Go file embeds the CRD
 * files its //go:embed names. */
TEST(probe_go_crd_type_and_embed) {
    /* WHY: this is a real schema/model association, not arbitrary same-name
     * usage. Tried the exact groupName package and source-declared Struct identities;
     * keep go:embed's independent file relation required below. */
    const RQ_Expected expected[] = {
        {.case_id = "crd-go-widget-type",
         .site = {.file = "config/crd/widgets.yaml",
                  .caller = "config.crd.widgets.yaml::apiextensions.k8s.io/"
                            "CustomResourceDefinition/-/widgets.example.com",
                  .needle = "Widget"},
         .relation = "USAGE",
         .target = {.file = "api/v1/widget_types.go",
                    .suffix = "api.v1.Widget",
                    .label = "Struct",
                    .declaration_line = 3,
                    .declaration_text = "type Widget struct {"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "crd_go_schema_type_proof_missing",
         .why = "The +groupName package annotation and exact Widget type identify the CRD model; "
                "the operator pass supplies no immutable cross-domain source proof.",
         .tried =
             "Preserved the original source declarations; exact source/target goldens and required "
             "ESM control run in both drivers. No directory or name-only target authority."}};
    const RQ_Forbidden forbidden[] = {
        {.case_id = "crd-kind-is-not-gadget",
         .site = {.file = "config/crd/widgets.yaml",
                  .caller = "config.crd.widgets.yaml::apiextensions.k8s.io/"
                            "CustomResourceDefinition/-/widgets.example.com",
                  .needle = "Widget"},
         .relation = "USAGE",
         .target = {.file = "api/v1/widget_types.go",
                    .suffix = "api.v1.Gadget",
                    .label = "Struct",
                    .declaration_line = 7,
                    .declaration_text = "type Gadget struct {"}}};
    ASSERT_TRUE(gpf_infra_quality(k_operator_repo, 6, "probe_go_crd_type_and_embed", expected, 1,
                                  forbidden, 1));
    ASSERT_EQ(gpf_typed_edges(k_operator_repo, 6, "REFERENCES_FILE", "File", "File",
                              "\"via\":\"go:embed\""),
              1);
    PASS();
}

/* The kind a controller reconciles is the type of the package it imports:
 * kubermatic has `Cluster` structs and a method `Cluster` elsewhere, and a
 * name-only lookup handed `For(&kubermaticv1.Cluster{})` to the method. */
TEST(probe_go_controller_kind_is_the_imported_type) {
    static const GpfFile files[] = {
        {"go.mod", "module example.com/proj\n\ngo 1.22\n"},
        {"api/v1/types.go", "package v1\n\ntype Cluster struct {\n\tName string\n}\n"},
        {"internal/legacy/types.go", "package legacy\n\ntype Cluster struct {\n\tID int\n}\n"},
        {"pkg/resources/creds.go",
         "package resources\n\ntype Creds struct{}\n\nfunc (c Creds) Cluster() string {\n"
         "\treturn \"\"\n}\n"},
        {"internal/controller/cluster.go",
         "package controller\n\n"
         "import (\n\tkubermaticv1 \"example.com/proj/api/v1\"\n"
         "\tctrl \"sigs.k8s.io/controller-runtime\"\n)\n\n"
         "func Add(mgr ctrl.Manager) error {\n"
         "\treturn ctrl.NewControllerManagedBy(mgr).For(&kubermaticv1.Cluster{}).Complete(nil)\n"
         "}\n"}};
    ASSERT_EQ(gpf_typed_edges(files, 5, "HANDLES", "Function", "Struct", "\"role\":\"for\""), 1);
    ASSERT_EQ(gpf_typed_edges(files, 5, "HANDLES", "Function", "Method", "\"role\":\"for\""), 0);
    PASS();
}

/* Edges of `type` into nodes defined in `tgt_file`; -1 when indexing failed. */
static int gpf_edges_into_file(const GpfFile *files, int nfiles, const char *type,
                               const char *tgt_file) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    int n = -1;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql = "SELECT COUNT(*) FROM edges e JOIN nodes t ON t.id=e.target_id "
                          "WHERE e.project=?1 AND e.type=?2 AND t.file_path=?3";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, type, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 3, tgt_file, -1, SQLITE_STATIC);
            n = sqlite3_step(st) == SQLITE_ROW ? sqlite3_column_int(st, 0) : -1;
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return n;
}

/* An import path names the directory its go.mod roots: kubermatic's
 * `k8c.io/kubermatic/sdk/v2/apis/...` lives in `sdk/apis/...`, which the
 * path does not end with, while an unrelated `apis/v1` does. */
TEST(probe_go_controller_import_follows_go_mod) {
    static const GpfFile files[] = {
        {"go.mod", "module example.com/proj\n\ngo 1.22\n"},
        {"sdk/go.mod", "module example.com/proj/sdk/v2\n\ngo 1.22\n"},
        {"sdk/apis/v1/types.go", "package v1\n\ntype Cluster struct {\n\tName string\n}\n"},
        {"apis/v1/types.go", "package v1\n\ntype Cluster struct {\n\tID int\n}\n"},
        {"pkg/controller/cluster.go",
         "package controller\n\n"
         "import (\n\tkubermaticv1 \"example.com/proj/sdk/v2/apis/v1\"\n"
         "\tctrl \"sigs.k8s.io/controller-runtime\"\n)\n\n"
         "func Add(mgr ctrl.Manager) error {\n"
         "\treturn ctrl.NewControllerManagedBy(mgr).For(&kubermaticv1.Cluster{}).Complete(nil)\n"
         "}\n"}};
    ASSERT_EQ(gpf_edges_into_file(files, 5, "HANDLES", "sdk/apis/v1/types.go"), 1);
    ASSERT_EQ(gpf_edges_into_file(files, 5, "HANDLES", "apis/v1/types.go"), 0);
    PASS();
}

/* A comment line may sit between a chain's '.' and the call (kubermatic's
 * seed-sync: `}).` / `// watch all seeds` / `For(&kubermaticv1.Seed{}, ...`),
 * and a call written inside a comment builds nothing. */
TEST(probe_go_controller_chain_with_comment_lines) {
    static const GpfFile files[] = {
        {"go.mod", "module example.com/proj\n\ngo 1.22\n"},
        {"api/v1/widget.go", "package v1\n\ntype Widget struct {\n\tName string\n}\n"},
        {"api/v1/gadget.go", "package v1\n\ntype Gadget struct {\n\tName string\n}\n"},
        {"internal/controller/widget.go", "package controller\n\n"
                                          "import (\n\tv1 \"example.com/proj/api/v1\"\n"
                                          "\tctrl \"sigs.k8s.io/controller-runtime\"\n)\n\n"
                                          "func Add(mgr ctrl.Manager) error {\n"
                                          "\t// gadgets would be b.Owns(&v1.Gadget{}) (not yet)\n"
                                          "\treturn ctrl.NewControllerManagedBy(mgr).\n"
                                          "\t\tNamed(\"widget\").\n"
                                          "\t\t// watch all widgets\n"
                                          "\t\tFor(&v1.Widget{}).\n"
                                          "\t\tComplete(nil)\n"
                                          "}\n"}};
    ASSERT_EQ(gpf_edges_into_file(files, 4, "HANDLES", "api/v1/widget.go"), 1);
    ASSERT_EQ(gpf_edges_into_file(files, 4, "HANDLES", "api/v1/gadget.go"), 0);
    PASS();
}

/* Go method QNs carry no receiver, so every reconciler's SetupWithManager in
 * one package is one node: each definition still handles its own kind. */
TEST(probe_go_controller_methods_sharing_a_node) {
    static const GpfFile files[] = {
        {"go.mod", "module example.com/proj\n\ngo 1.22\n"},
        {"api/v1/types.go", "package v1\n\ntype Widget struct {\n\tName string\n}\n\n"
                            "type Gadget struct {\n\tName string\n}\n"},
        {"internal/controller/widget.go",
         "package controller\n\n"
         "import (\n\tv1 \"example.com/proj/api/v1\"\n"
         "\tctrl \"sigs.k8s.io/controller-runtime\"\n)\n\n"
         "type WidgetReconciler struct{}\n\n"
         "func (r *WidgetReconciler) SetupWithManager(mgr ctrl.Manager) error {\n"
         "\treturn ctrl.NewControllerManagedBy(mgr).For(&v1.Widget{}).Complete(r)\n"
         "}\n"},
        {"internal/controller/gadget.go",
         "package controller\n\n"
         "import (\n\tv1 \"example.com/proj/api/v1\"\n"
         "\tctrl \"sigs.k8s.io/controller-runtime\"\n)\n\n"
         "type GadgetReconciler struct{}\n\n"
         "func (r *GadgetReconciler) SetupWithManager(mgr ctrl.Manager) error {\n"
         "\treturn ctrl.NewControllerManagedBy(mgr).For(&v1.Gadget{}).Complete(r)\n"
         "}\n"}};
    ASSERT_EQ(gpf_edges_into_file(files, 4, "HANDLES", "api/v1/types.go"), 2);
    PASS();
}

/* ── Curated CRD references + deploy scopes (P1.3) ──────────────────── */

/* A Flux bootstrap: the flux-system Kustomization deploys ./clusters/prod,
 * whose Kustomizations deploy ./infra and ./apps/prod. Each is its own
 * kustomize root, so the references between them -- a Kustomization's
 * sourceRef to the flux-system GitRepository, a HelmRelease's dependsOn in
 * another namespace, a Certificate's ClusterIssuer -- meet only in the
 * deploy scope the root reaches. */
TEST(probe_flux_refs_meet_in_the_deploy_scope) {
    static const GpfFile files[] = {
        {"clusters/prod/flux-system/kustomization.yaml",
         "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n"
         "resources:\n  - gotk-sync.yaml\n"},
        {"clusters/prod/flux-system/gotk-sync.yaml",
         "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\n"
         "metadata:\n  name: flux-system\n  namespace: flux-system\n"
         "spec:\n  url: ssh://git@example.com/fleet\n---\n"
         "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\n"
         "metadata:\n  name: flux-system\n  namespace: flux-system\n"
         "spec:\n  path: ./clusters/prod\n  sourceRef:\n    kind: GitRepository\n"
         "    name: flux-system\n"},
        {"clusters/prod/apps.yaml",
         "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\n"
         "metadata:\n  name: infra\n  namespace: flux-system\n"
         "spec:\n  path: ./infra\n  sourceRef:\n    kind: GitRepository\n"
         "    name: flux-system\n---\n"
         "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\n"
         "metadata:\n  name: apps\n  namespace: flux-system\n"
         "spec:\n  dependsOn:\n    - name: infra\n  path: ./apps/prod\n"
         "  sourceRef:\n    kind: GitRepository\n    name: flux-system\n"},
        {"infra/kustomization.yaml",
         "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n"
         "resources:\n  - cert-manager.yaml\n"},
        {"infra/cert-manager.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\n"
                                    "metadata:\n  name: cert-manager\n  namespace: cert-manager\n"
                                    "spec:\n  interval: 1h\n---\n"
                                    "apiVersion: cert-manager.io/v1\nkind: ClusterIssuer\n"
                                    "metadata:\n  name: letsencrypt\nspec:\n  selfSigned: {}\n"},
        {"apps/prod/kustomization.yaml",
         "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n"
         "namespace: podinfo\nresources:\n  - ../base/podinfo\n"},
        {"apps/base/podinfo/kustomization.yaml",
         "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n"
         "resources:\n  - release.yaml\n  - repo.yaml\n"},
        {"apps/base/podinfo/release.yaml",
         "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\n"
         "metadata:\n  name: podinfo\nspec:\n  chart:\n    spec:\n      chart: podinfo\n"
         "      sourceRef:\n        kind: HelmRepository\n        name: podinfo\n"
         "  dependsOn:\n    - name: cert-manager\n      namespace: cert-manager\n"
         "  valuesFrom:\n    - kind: ConfigMap\n      name: podinfo-values\n---\n"
         "apiVersion: cert-manager.io/v1\nkind: Certificate\n"
         "metadata:\n  name: podinfo-tls\nspec:\n  secretName: podinfo-tls\n"
         "  issuerRef:\n    kind: ClusterIssuer\n    name: letsencrypt\n"},
        {"apps/base/podinfo/repo.yaml",
         "apiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmRepository\n"
         "metadata:\n  name: podinfo\nspec:\n  url: https://example.com/charts\n---\n"
         "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: podinfo-values\n"
         "data:\n  replicaCount: \"2\"\n"}};
    /* WHY: these six original named dependencies remain genuine. Tried the
     * unchanged resource documents in one separately authored flat build; this
     * selects membership without treating a Git URL or Flux path as local-artifact
     * authority. The valuesFrom ConfigMap is a dependency even when its default
     * values.yaml key is absent (a reconciliation error, not a different target).
     * No namespace transform is inferred. Custom-controller proof remains missing. */
    const GpfFile configured[] = {
        files[1],
        files[2],
        files[4],
        files[7],
        files[8],
        {"quality/decoys.yaml",
         "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata:\n  name: "
         "flux-system\n  namespace: unrelated\n---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: "
         "HelmRepository\nmetadata:\n  name: podinfo\n  namespace: unrelated\n---\napiVersion: "
         "v1\nkind: ConfigMap\nmetadata:\n  name: podinfo-values\n  namespace: "
         "unrelated\n---\napiVersion: cert-manager.io/v1\nkind: Issuer\nmetadata:\n  name: "
         "letsencrypt\n"},
        {"kustomization.yaml",
         "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - "
         "clusters/prod/flux-system/gotk-sync.yaml\n  - clusters/prod/apps.yaml\n  - "
         "infra/cert-manager.yaml\n  - apps/base/podinfo/release.yaml\n  - "
         "apps/base/podinfo/repo.yaml\n  - quality/decoys.yaml\n"}};
    const RQ_Expected expected[] = {
        {.case_id = "flux-sync-source",
         .site = {.file = "clusters/prod/flux-system/gotk-sync.yaml",
                  .caller = "clusters.prod.flux-system.gotk-sync.yaml::kustomize.toolkit.fluxcd.io/"
                            "Kustomization/flux-system/flux-system",
                  .start_byte = 325,
                  .end_byte = 366},
         .relation = "USAGE",
         .target = {.file = "clusters/prod/flux-system/gotk-sync.yaml",
                    .suffix = "clusters.prod.flux-system.gotk-sync.yaml::source.toolkit.fluxcd.io/"
                              "GitRepository/flux-system/flux-system",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: flux-system"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "flux_controller_reference_proof_missing",
         .why = "The explicit flat resource list supplies one cluster domain. The source field "
                "fixes the kind, name and same or cluster namespace; no repository URL or "
                "directory heuristic establishes target identity. Custom-controller immutable "
                "proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "flux-infra-source",
         .site = {.file = "clusters/prod/apps.yaml",
                  .caller = "clusters.prod.apps.yaml::kustomize.toolkit.fluxcd.io/Kustomization/"
                            "flux-system/infra",
                  .start_byte = 151,
                  .end_byte = 192},
         .relation = "USAGE",
         .target = {.file = "clusters/prod/flux-system/gotk-sync.yaml",
                    .suffix = "clusters.prod.flux-system.gotk-sync.yaml::source.toolkit.fluxcd.io/"
                              "GitRepository/flux-system/flux-system",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: flux-system"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "flux_controller_reference_proof_missing",
         .why = "The explicit flat resource list supplies one cluster domain. The source field "
                "fixes the kind, name and same or cluster namespace; no repository URL or "
                "directory heuristic establishes target identity. Custom-controller immutable "
                "proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "flux-apps-source",
         .site = {.file = "clusters/prod/apps.yaml",
                  .caller = "clusters.prod.apps.yaml::kustomize.toolkit.fluxcd.io/Kustomization/"
                            "flux-system/apps",
                  .start_byte = 382,
                  .end_byte = 423},
         .relation = "USAGE",
         .target = {.file = "clusters/prod/flux-system/gotk-sync.yaml",
                    .suffix = "clusters.prod.flux-system.gotk-sync.yaml::source.toolkit.fluxcd.io/"
                              "GitRepository/flux-system/flux-system",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: flux-system"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "flux_controller_reference_proof_missing",
         .why = "The explicit flat resource list supplies one cluster domain. The source field "
                "fixes the kind, name and same or cluster namespace; no repository URL or "
                "directory heuristic establishes target identity. Custom-controller immutable "
                "proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "flux-release-source",
         .site =
             {.file = "apps/base/podinfo/release.yaml",
              .caller =
                  "apps.base.podinfo.release.yaml::helm.toolkit.fluxcd.io/HelmRelease/-/podinfo",
              .start_byte = 153,
              .end_byte = 195},
         .relation = "USAGE",
         .target =
             {.file = "apps/base/podinfo/repo.yaml",
              .suffix =
                  "apps.base.podinfo.repo.yaml::source.toolkit.fluxcd.io/HelmRepository/-/podinfo",
              .label = "Resource",
              .declaration_line = 4,
              .declaration_text = "  name: podinfo"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "flux_controller_reference_proof_missing",
         .why = "The explicit flat resource list supplies one cluster domain. The source field "
                "fixes the kind, name and same or cluster namespace; no repository URL or "
                "directory heuristic establishes target identity. Custom-controller immutable "
                "proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "flux-values-configmap",
         .site =
             {.file = "apps/base/podinfo/release.yaml",
              .caller =
                  "apps.base.podinfo.release.yaml::helm.toolkit.fluxcd.io/HelmRelease/-/podinfo",
              .start_byte = 284,
              .end_byte = 326},
         .relation = "USAGE",
         .target = {.file = "apps/base/podinfo/repo.yaml",
                    .suffix = "apps.base.podinfo.repo.yaml::core/ConfigMap/-/podinfo-values",
                    .label = "Resource",
                    .declaration_line = 11,
                    .declaration_text = "  name: podinfo-values"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "flux_controller_reference_proof_missing",
         .why = "The explicit flat resource list supplies one cluster domain. The source field "
                "fixes the kind, name and same or cluster namespace; no repository URL or "
                "directory heuristic establishes target identity. Custom-controller immutable "
                "proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "flux-certificate-cluster-issuer",
         .site = {.file = "apps/base/podinfo/release.yaml",
                  .caller =
                      "apps.base.podinfo.release.yaml::cert-manager.io/Certificate/-/podinfo-tls",
                  .start_byte = 459,
                  .end_byte = 500},
         .relation = "USAGE",
         .target = {.file = "infra/cert-manager.yaml",
                    .suffix =
                        "infra.cert-manager.yaml::cert-manager.io/ClusterIssuer/-/letsencrypt",
                    .label = "Resource",
                    .declaration_line = 12,
                    .declaration_text = "  name: letsencrypt"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "flux_controller_reference_proof_missing",
         .why = "The explicit flat resource list supplies one cluster domain. The source field "
                "fixes the kind, name and same or cluster namespace; no repository URL or "
                "directory heuristic establishes target identity. Custom-controller immutable "
                "proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."}};
    const RQ_Forbidden forbidden[] = {
        {.case_id = "flux-sync-source-not-other-namespace-or-kind",
         .site = {.file = "clusters/prod/flux-system/gotk-sync.yaml",
                  .caller = "clusters.prod.flux-system.gotk-sync.yaml::kustomize.toolkit.fluxcd.io/"
                            "Kustomization/flux-system/flux-system",
                  .start_byte = 325,
                  .end_byte = 366},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::source.toolkit.fluxcd.io/GitRepository/"
                              "unrelated/flux-system",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: flux-system"}},
        {.case_id = "flux-infra-source-not-other-namespace-or-kind",
         .site = {.file = "clusters/prod/apps.yaml",
                  .caller = "clusters.prod.apps.yaml::kustomize.toolkit.fluxcd.io/Kustomization/"
                            "flux-system/infra",
                  .start_byte = 151,
                  .end_byte = 192},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::source.toolkit.fluxcd.io/GitRepository/"
                              "unrelated/flux-system",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: flux-system"}},
        {.case_id = "flux-apps-source-not-other-namespace-or-kind",
         .site = {.file = "clusters/prod/apps.yaml",
                  .caller = "clusters.prod.apps.yaml::kustomize.toolkit.fluxcd.io/Kustomization/"
                            "flux-system/apps",
                  .start_byte = 382,
                  .end_byte = 423},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::source.toolkit.fluxcd.io/GitRepository/"
                              "unrelated/flux-system",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: flux-system"}},
        {.case_id = "flux-release-source-not-other-namespace-or-kind",
         .site =
             {.file = "apps/base/podinfo/release.yaml",
              .caller =
                  "apps.base.podinfo.release.yaml::helm.toolkit.fluxcd.io/HelmRelease/-/podinfo",
              .start_byte = 153,
              .end_byte = 195},
         .relation = "USAGE",
         .target =
             {.file = "quality/decoys.yaml",
              .suffix =
                  "quality.decoys.yaml::source.toolkit.fluxcd.io/HelmRepository/unrelated/podinfo",
              .label = "Resource",
              .declaration_line = 10,
              .declaration_text = "  name: podinfo"}},
        {.case_id = "flux-values-configmap-not-other-namespace-or-kind",
         .site =
             {.file = "apps/base/podinfo/release.yaml",
              .caller =
                  "apps.base.podinfo.release.yaml::helm.toolkit.fluxcd.io/HelmRelease/-/podinfo",
              .start_byte = 284,
              .end_byte = 326},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::core/ConfigMap/unrelated/podinfo-values",
                    .label = "Resource",
                    .declaration_line = 16,
                    .declaration_text = "  name: podinfo-values"}},
        {.case_id = "flux-certificate-cluster-issuer-not-other-namespace-or-kind",
         .site = {.file = "apps/base/podinfo/release.yaml",
                  .caller =
                      "apps.base.podinfo.release.yaml::cert-manager.io/Certificate/-/podinfo-tls",
                  .start_byte = 459,
                  .end_byte = 500},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::cert-manager.io/Issuer/-/letsencrypt",
                    .label = "Resource",
                    .declaration_line = 22,
                    .declaration_text = "  name: letsencrypt"}}};
    ASSERT_TRUE(gpf_infra_quality(configured, 7,
                                  "probe_flux_refs_meet_in_the_deploy_scope/configured", expected,
                                  6, forbidden, 6));
    /* apps -> infra; podinfo -> cert-manager in another namespace and root */
    ASSERT_EQ(
        gpf_typed_edges(files, 9, "DEPENDS_ON", "Resource", "Resource", "\"via\":\"dependsOn\""),
        2);
    PASS();
}

/* Cluster API: a Cluster names its infrastructure, control plane and
 * class; machines name their cluster, bootstrap config and templates. The
 * source kind counts in its API group: a CloudNativePG `Cluster` with the
 * same field is not a Cluster API one. */
TEST(probe_capi_refs_follow_the_api_group) {
    static const char tmpl[] =
        "apiVersion: cluster.x-k8s.io/v1beta2\nkind: Cluster\nmetadata:\n  name: c1\n"
        "spec:\n  infrastructureRef:\n    kind: DockerCluster\n    name: c1\n"
        "  controlPlaneRef:\n    kind: KubeadmControlPlane\n    name: c1-cp\n"
        "  topology:\n    classRef:\n      name: quick\n---\n"
        "apiVersion: infrastructure.cluster.x-k8s.io/v1beta2\nkind: DockerCluster\n"
        "metadata:\n  name: c1\n---\n"
        "apiVersion: controlplane.cluster.x-k8s.io/v1beta2\nkind: KubeadmControlPlane\n"
        "metadata:\n  name: c1-cp\nspec:\n  machineTemplate:\n    spec:\n"
        "      infrastructureRef:\n        kind: DockerMachineTemplate\n        name: "
        "cp-infra\n---\n"
        "apiVersion: infrastructure.cluster.x-k8s.io/v1beta2\nkind: DockerMachineTemplate\n"
        "metadata:\n  name: cp-infra\n---\n"
        "apiVersion: cluster.x-k8s.io/v1beta2\nkind: MachineDeployment\nmetadata:\n  name: md\n"
        "spec:\n  clusterName: c1\n  template:\n    spec:\n      bootstrap:\n"
        "        configRef:\n          kind: KubeadmConfigTemplate\n          name: md-boot\n"
        "      infrastructureRef:\n        kind: DockerMachineTemplate\n        name: "
        "md-infra\n---\n"
        "apiVersion: bootstrap.cluster.x-k8s.io/v1beta2\nkind: KubeadmConfigTemplate\n"
        "metadata:\n  name: md-boot\n---\n"
        "apiVersion: infrastructure.cluster.x-k8s.io/v1beta2\nkind: DockerMachineTemplate\n"
        "metadata:\n  name: md-infra\n---\n"
        "apiVersion: cluster.x-k8s.io/v1beta2\nkind: ClusterClass\nmetadata:\n  name: quick\n"
        "spec:\n  infrastructure:\n    templateRef:\n      kind: DockerClusterTemplate\n"
        "      name: quick-cluster\n  workers:\n    machineDeployments:\n      - class: w\n"
        "        bootstrap:\n          templateRef:\n            kind: KubeadmConfigTemplate\n"
        "            name: md-boot\n---\n"
        "apiVersion: infrastructure.cluster.x-k8s.io/v1beta2\nkind: DockerClusterTemplate\n"
        "metadata:\n  name: quick-cluster\n---\n"
        "apiVersion: postgresql.cnpg.io/v1\nkind: Cluster\nmetadata:\n  name: pg\n"
        "spec:\n  infrastructureRef:\n    kind: DockerCluster\n    name: c1\n";
    static const GpfFile files[] = {{"templates/cluster.yaml", tmpl}};
    /* WHY: the original classRef and clusterName dependencies are genuine.
     * Five v1beta2 object references omit required apiGroup, and two templateRefs
     * omit required apiVersion; kind/name proximity cannot repair those identities.
     * Tried the unchanged source under explicit membership, then a separate copy
     * with complete reference fields and literal namespaces. All nine complete
     * reference obligations are counted; CNPG never gains CAPI field semantics.
     * These are source-reference checks, not claims of successful reconciliation. */
    const GpfFile original_build[] = {files[0],
                                      {"kustomization.yaml",
                                       "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: "
                                       "Kustomization\nresources:\n  - templates/cluster.yaml\n"}};
    const RQ_Expected original_expected[] = {
        {.case_id = "original-cluster-class",
         .site = {.file = "templates/cluster.yaml",
                  .caller = "templates.cluster.yaml::cluster.x-k8s.io/Cluster/-/c1",
                  .start_byte = 233,
                  .end_byte = 244},
         .relation = "USAGE",
         .target = {.file = "templates/cluster.yaml",
                    .suffix = "templates.cluster.yaml::cluster.x-k8s.io/ClusterClass/-/quick",
                    .label = "Resource",
                    .declaration_line = 66,
                    .declaration_text = "  name: quick"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "capi_controller_reference_proof_missing",
         .why = "The source names an exact same-namespace declaration in one explicit flat build. "
                "Its required reference identity fields are complete. Immutable Cluster API schema "
                "and membership proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "original-machine-cluster",
         .site = {.file = "templates/cluster.yaml",
                  .caller = "templates.cluster.yaml::cluster.x-k8s.io/MachineDeployment/-/md",
                  .start_byte = 783,
                  .end_byte = 785},
         .relation = "USAGE",
         .target = {.file = "templates/cluster.yaml",
                    .suffix = "templates.cluster.yaml::cluster.x-k8s.io/Cluster/-/c1",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: c1"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "capi_controller_reference_proof_missing",
         .why = "The source names an exact same-namespace declaration in one explicit flat build. "
                "Its required reference identity fields are complete. Immutable Cluster API schema "
                "and membership proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."}};
    const RQ_Forbidden original_forbidden[] = {
        {.case_id = "original-missing-api-group-cluster-infrastructure",
         .site = {.file = "templates/cluster.yaml",
                  .caller = "templates.cluster.yaml::cluster.x-k8s.io/Cluster/-/c1",
                  .start_byte = 103,
                  .end_byte = 135},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "original-missing-api-group-cluster-control-plane",
         .site = {.file = "templates/cluster.yaml",
                  .caller = "templates.cluster.yaml::cluster.x-k8s.io/Cluster/-/c1",
                  .start_byte = 159,
                  .end_byte = 200},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "original-missing-api-group-control-plane-infrastructure",
         .site = {.file = "templates/cluster.yaml",
                  .caller = "templates.cluster.yaml::controlplane.cluster.x-k8s.io/"
                            "KubeadmControlPlane/-/c1-cp",
                  .start_byte = 514,
                  .end_byte = 564},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "original-missing-api-group-machine-bootstrap",
         .site = {.file = "templates/cluster.yaml",
                  .caller = "templates.cluster.yaml::cluster.x-k8s.io/MachineDeployment/-/md",
                  .start_byte = 854,
                  .end_byte = 905},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "original-missing-api-group-machine-infrastructure",
         .site = {.file = "templates/cluster.yaml",
                  .caller = "templates.cluster.yaml::cluster.x-k8s.io/MachineDeployment/-/md",
                  .start_byte = 939,
                  .end_byte = 989},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "original-missing-api-version-class-infrastructure-template",
         .site = {.file = "templates/cluster.yaml",
                  .caller = "templates.cluster.yaml::cluster.x-k8s.io/ClusterClass/-/quick",
                  .start_byte = 1337,
                  .end_byte = 1390},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "original-missing-api-version-class-bootstrap-template",
         .site = {.file = "templates/cluster.yaml",
                  .caller = "templates.cluster.yaml::cluster.x-k8s.io/ClusterClass/-/quick",
                  .start_byte = 1497,
                  .end_byte = 1550},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "original-cnpg-is-not-capi",
         .site = {.file = "templates/cluster.yaml",
                  .caller = "templates.cluster.yaml::postgresql.cnpg.io/Cluster/-/pg",
                  .start_byte = 1771,
                  .end_byte = 1803},
         .relation = "USAGE",
         .any_target = true}};
    ASSERT_TRUE(gpf_infra_quality(original_build, 2,
                                  "probe_capi_refs_follow_the_api_group/original",
                                  original_expected, 2, original_forbidden, 8));
    const GpfFile configured[] = {
        {"quality/cluster.yaml",
         "apiVersion: cluster.x-k8s.io/v1beta2\nkind: Cluster\nmetadata:\n  name: c1\n  namespace: "
         "quality\nspec:\n  infrastructureRef:\n    apiGroup: infrastructure.cluster.x-k8s.io\n    "
         "kind: DockerCluster\n    name: c1\n  controlPlaneRef:\n    apiGroup: "
         "controlplane.cluster.x-k8s.io\n    kind: KubeadmControlPlane\n    name: c1-cp\n  "
         "topology:\n    classRef:\n      name: quick\n---\napiVersion: "
         "infrastructure.cluster.x-k8s.io/v1beta2\nkind: DockerCluster\nmetadata:\n  name: c1\n  "
         "namespace: quality\n---\napiVersion: controlplane.cluster.x-k8s.io/v1beta2\nkind: "
         "KubeadmControlPlane\nmetadata:\n  name: c1-cp\n  namespace: quality\nspec:\n  "
         "machineTemplate:\n    spec:\n      infrastructureRef:\n        apiGroup: "
         "infrastructure.cluster.x-k8s.io\n        kind: DockerMachineTemplate\n        name: "
         "cp-infra\n---\napiVersion: infrastructure.cluster.x-k8s.io/v1beta2\nkind: "
         "DockerMachineTemplate\nmetadata:\n  name: cp-infra\n  namespace: "
         "quality\n---\napiVersion: cluster.x-k8s.io/v1beta2\nkind: MachineDeployment\nmetadata:\n "
         " name: md\n  namespace: quality\nspec:\n  clusterName: c1\n  template:\n    spec:\n      "
         "bootstrap:\n        configRef:\n          apiGroup: bootstrap.cluster.x-k8s.io\n         "
         " kind: KubeadmConfigTemplate\n          name: md-boot\n      infrastructureRef:\n        "
         "apiGroup: infrastructure.cluster.x-k8s.io\n        kind: DockerMachineTemplate\n        "
         "name: md-infra\n---\napiVersion: bootstrap.cluster.x-k8s.io/v1beta2\nkind: "
         "KubeadmConfigTemplate\nmetadata:\n  name: md-boot\n  namespace: "
         "quality\n---\napiVersion: infrastructure.cluster.x-k8s.io/v1beta2\nkind: "
         "DockerMachineTemplate\nmetadata:\n  name: md-infra\n  namespace: "
         "quality\n---\napiVersion: cluster.x-k8s.io/v1beta2\nkind: ClusterClass\nmetadata:\n  "
         "name: quick\n  namespace: quality\nspec:\n  infrastructure:\n    templateRef:\n      "
         "apiVersion: infrastructure.cluster.x-k8s.io/v1beta2\n      kind: DockerClusterTemplate\n "
         "     name: quick-cluster\n  workers:\n    machineDeployments:\n      - class: w\n        "
         "bootstrap:\n          templateRef:\n            apiVersion: "
         "bootstrap.cluster.x-k8s.io/v1beta2\n            kind: KubeadmConfigTemplate\n            "
         "name: md-boot\n---\napiVersion: infrastructure.cluster.x-k8s.io/v1beta2\nkind: "
         "DockerClusterTemplate\nmetadata:\n  name: quick-cluster\n  namespace: "
         "quality\n---\napiVersion: postgresql.cnpg.io/v1\nkind: Cluster\nmetadata:\n  name: pg\n  "
         "namespace: quality\nspec:\n  infrastructureRef:\n    kind: DockerCluster\n    name: "
         "c1\n"},
        {"quality/decoys.yaml",
         "apiVersion: infrastructure.cluster.x-k8s.io/v1beta2\nkind: DockerCluster\nmetadata:\n  "
         "name: c1\n  namespace: unrelated\n---\napiVersion: "
         "controlplane.cluster.x-k8s.io/v1beta2\nkind: KubeadmControlPlane\nmetadata:\n  name: "
         "c1-cp\n  namespace: unrelated\n---\napiVersion: cluster.x-k8s.io/v1beta2\nkind: "
         "ClusterClass\nmetadata:\n  name: quick\n  namespace: unrelated\n---\napiVersion: "
         "infrastructure.cluster.x-k8s.io/v1beta2\nkind: DockerMachineTemplate\nmetadata:\n  name: "
         "cp-infra\n  namespace: unrelated\n---\napiVersion: cluster.x-k8s.io/v1beta2\nkind: "
         "Cluster\nmetadata:\n  name: c1\n  namespace: unrelated\n---\napiVersion: "
         "bootstrap.cluster.x-k8s.io/v1beta2\nkind: KubeadmConfigTemplate\nmetadata:\n  name: "
         "md-boot\n  namespace: unrelated\n---\napiVersion: "
         "infrastructure.cluster.x-k8s.io/v1beta2\nkind: DockerMachineTemplate\nmetadata:\n  name: "
         "md-infra\n  namespace: unrelated\n---\napiVersion: "
         "infrastructure.cluster.x-k8s.io/v1beta2\nkind: DockerClusterTemplate\nmetadata:\n  name: "
         "quick-cluster\n  namespace: unrelated\n---\napiVersion: wrong.example/v1\nkind: "
         "DockerCluster\nmetadata:\n  name: c1\n  namespace: quality\n"},
        {"quality/kustomization.yaml",
         "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - "
         "cluster.yaml\n  - decoys.yaml\n"}};
    const RQ_Expected expected[] = {
        {.case_id = "configured-cluster-infrastructure",
         .site = {.file = "quality/cluster.yaml",
                  .caller = "quality.cluster.yaml::cluster.x-k8s.io/Cluster/quality/c1",
                  .start_byte = 124,
                  .end_byte = 202},
         .relation = "USAGE",
         .target =
             {.file = "quality/cluster.yaml",
              .suffix =
                  "quality.cluster.yaml::infrastructure.cluster.x-k8s.io/DockerCluster/quality/c1",
              .label = "Resource",
              .declaration_line = 22,
              .declaration_text = "  name: c1"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "capi_controller_reference_proof_missing",
         .why = "The source names an exact same-namespace declaration in one explicit flat build. "
                "Its required reference identity fields are complete. Immutable Cluster API schema "
                "and membership proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "configured-cluster-control-plane",
         .site = {.file = "quality/cluster.yaml",
                  .caller = "quality.cluster.yaml::cluster.x-k8s.io/Cluster/quality/c1",
                  .start_byte = 226,
                  .end_byte = 311},
         .relation = "USAGE",
         .target = {.file = "quality/cluster.yaml",
                    .suffix = "quality.cluster.yaml::controlplane.cluster.x-k8s.io/"
                              "KubeadmControlPlane/quality/c1-cp",
                    .label = "Resource",
                    .declaration_line = 28,
                    .declaration_text = "  name: c1-cp"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "capi_controller_reference_proof_missing",
         .why = "The source names an exact same-namespace declaration in one explicit flat build. "
                "Its required reference identity fields are complete. Immutable Cluster API schema "
                "and membership proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "configured-cluster-class",
         .site = {.file = "quality/cluster.yaml",
                  .caller = "quality.cluster.yaml::cluster.x-k8s.io/Cluster/quality/c1",
                  .start_byte = 344,
                  .end_byte = 355},
         .relation = "USAGE",
         .target = {.file = "quality/cluster.yaml",
                    .suffix = "quality.cluster.yaml::cluster.x-k8s.io/ClusterClass/quality/quick",
                    .label = "Resource",
                    .declaration_line = 78,
                    .declaration_text = "  name: quick"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "capi_controller_reference_proof_missing",
         .why = "The source names an exact same-namespace declaration in one explicit flat build. "
                "Its required reference identity fields are complete. Immutable Cluster API schema "
                "and membership proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "configured-control-plane-infrastructure",
         .site = {.file = "quality/cluster.yaml",
                  .caller = "quality.cluster.yaml::controlplane.cluster.x-k8s.io/"
                            "KubeadmControlPlane/quality/c1-cp",
                  .start_byte = 667,
                  .end_byte = 767},
         .relation = "USAGE",
         .target = {.file = "quality/cluster.yaml",
                    .suffix = "quality.cluster.yaml::infrastructure.cluster.x-k8s.io/"
                              "DockerMachineTemplate/quality/cp-infra",
                    .label = "Resource",
                    .declaration_line = 41,
                    .declaration_text = "  name: cp-infra"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "capi_controller_reference_proof_missing",
         .why = "The source names an exact same-namespace declaration in one explicit flat build. "
                "Its required reference identity fields are complete. Immutable Cluster API schema "
                "and membership proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "configured-machine-cluster",
         .site = {.file = "quality/cluster.yaml",
                  .caller = "quality.cluster.yaml::cluster.x-k8s.io/MachineDeployment/quality/md",
                  .start_byte = 1028,
                  .end_byte = 1030},
         .relation = "USAGE",
         .target = {.file = "quality/cluster.yaml",
                    .suffix = "quality.cluster.yaml::cluster.x-k8s.io/Cluster/quality/c1",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: c1"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "capi_controller_reference_proof_missing",
         .why = "The source names an exact same-namespace declaration in one explicit flat build. "
                "Its required reference identity fields are complete. Immutable Cluster API schema "
                "and membership proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "configured-machine-bootstrap",
         .site = {.file = "quality/cluster.yaml",
                  .caller = "quality.cluster.yaml::cluster.x-k8s.io/MachineDeployment/quality/md",
                  .start_byte = 1099,
                  .end_byte = 1197},
         .relation = "USAGE",
         .target = {.file = "quality/cluster.yaml",
                    .suffix = "quality.cluster.yaml::bootstrap.cluster.x-k8s.io/"
                              "KubeadmConfigTemplate/quality/md-boot",
                    .label = "Resource",
                    .declaration_line = 66,
                    .declaration_text = "  name: md-boot"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "capi_controller_reference_proof_missing",
         .why = "The source names an exact same-namespace declaration in one explicit flat build. "
                "Its required reference identity fields are complete. Immutable Cluster API schema "
                "and membership proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "configured-machine-infrastructure",
         .site = {.file = "quality/cluster.yaml",
                  .caller = "quality.cluster.yaml::cluster.x-k8s.io/MachineDeployment/quality/md",
                  .start_byte = 1231,
                  .end_byte = 1331},
         .relation = "USAGE",
         .target = {.file = "quality/cluster.yaml",
                    .suffix = "quality.cluster.yaml::infrastructure.cluster.x-k8s.io/"
                              "DockerMachineTemplate/quality/md-infra",
                    .label = "Resource",
                    .declaration_line = 72,
                    .declaration_text = "  name: md-infra"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "capi_controller_reference_proof_missing",
         .why = "The source names an exact same-namespace declaration in one explicit flat build. "
                "Its required reference identity fields are complete. Immutable Cluster API schema "
                "and membership proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "configured-class-infrastructure-template",
         .site = {.file = "quality/cluster.yaml",
                  .caller = "quality.cluster.yaml::cluster.x-k8s.io/ClusterClass/quality/quick",
                  .start_byte = 1742,
                  .end_byte = 1853},
         .relation = "USAGE",
         .target = {.file = "quality/cluster.yaml",
                    .suffix = "quality.cluster.yaml::infrastructure.cluster.x-k8s.io/"
                              "DockerClusterTemplate/quality/quick-cluster",
                    .label = "Resource",
                    .declaration_line = 98,
                    .declaration_text = "  name: quick-cluster"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "capi_controller_reference_proof_missing",
         .why = "The source names an exact same-namespace declaration in one explicit flat build. "
                "Its required reference identity fields are complete. Immutable Cluster API schema "
                "and membership proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "configured-class-bootstrap-template",
         .site = {.file = "quality/cluster.yaml",
                  .caller = "quality.cluster.yaml::cluster.x-k8s.io/ClusterClass/quality/quick",
                  .start_byte = 1960,
                  .end_byte = 2072},
         .relation = "USAGE",
         .target = {.file = "quality/cluster.yaml",
                    .suffix = "quality.cluster.yaml::bootstrap.cluster.x-k8s.io/"
                              "KubeadmConfigTemplate/quality/md-boot",
                    .label = "Resource",
                    .declaration_line = 66,
                    .declaration_text = "  name: md-boot"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "capi_controller_reference_proof_missing",
         .why = "The source names an exact same-namespace declaration in one explicit flat build. "
                "Its required reference identity fields are complete. Immutable Cluster API schema "
                "and membership proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."}};
    const RQ_Forbidden forbidden[] = {
        {.case_id = "configured-cluster-infrastructure-not-other-namespace",
         .site = {.file = "quality/cluster.yaml",
                  .caller = "quality.cluster.yaml::cluster.x-k8s.io/Cluster/quality/c1",
                  .start_byte = 124,
                  .end_byte = 202},
         .relation = "USAGE",
         .target =
             {.file = "quality/decoys.yaml",
              .suffix =
                  "quality.decoys.yaml::infrastructure.cluster.x-k8s.io/DockerCluster/unrelated/c1",
              .label = "Resource",
              .declaration_line = 4,
              .declaration_text = "  name: c1"}},
        {.case_id = "configured-cluster-control-plane-not-other-namespace",
         .site = {.file = "quality/cluster.yaml",
                  .caller = "quality.cluster.yaml::cluster.x-k8s.io/Cluster/quality/c1",
                  .start_byte = 226,
                  .end_byte = 311},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::controlplane.cluster.x-k8s.io/"
                              "KubeadmControlPlane/unrelated/c1-cp",
                    .label = "Resource",
                    .declaration_line = 10,
                    .declaration_text = "  name: c1-cp"}},
        {.case_id = "configured-cluster-class-not-other-namespace",
         .site = {.file = "quality/cluster.yaml",
                  .caller = "quality.cluster.yaml::cluster.x-k8s.io/Cluster/quality/c1",
                  .start_byte = 344,
                  .end_byte = 355},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::cluster.x-k8s.io/ClusterClass/unrelated/quick",
                    .label = "Resource",
                    .declaration_line = 16,
                    .declaration_text = "  name: quick"}},
        {.case_id = "configured-control-plane-infrastructure-not-other-namespace",
         .site = {.file = "quality/cluster.yaml",
                  .caller = "quality.cluster.yaml::controlplane.cluster.x-k8s.io/"
                            "KubeadmControlPlane/quality/c1-cp",
                  .start_byte = 667,
                  .end_byte = 767},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::infrastructure.cluster.x-k8s.io/"
                              "DockerMachineTemplate/unrelated/cp-infra",
                    .label = "Resource",
                    .declaration_line = 22,
                    .declaration_text = "  name: cp-infra"}},
        {.case_id = "configured-machine-cluster-not-other-namespace",
         .site = {.file = "quality/cluster.yaml",
                  .caller = "quality.cluster.yaml::cluster.x-k8s.io/MachineDeployment/quality/md",
                  .start_byte = 1028,
                  .end_byte = 1030},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::cluster.x-k8s.io/Cluster/unrelated/c1",
                    .label = "Resource",
                    .declaration_line = 28,
                    .declaration_text = "  name: c1"}},
        {.case_id = "configured-machine-bootstrap-not-other-namespace",
         .site = {.file = "quality/cluster.yaml",
                  .caller = "quality.cluster.yaml::cluster.x-k8s.io/MachineDeployment/quality/md",
                  .start_byte = 1099,
                  .end_byte = 1197},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::bootstrap.cluster.x-k8s.io/"
                              "KubeadmConfigTemplate/unrelated/md-boot",
                    .label = "Resource",
                    .declaration_line = 34,
                    .declaration_text = "  name: md-boot"}},
        {.case_id = "configured-machine-infrastructure-not-other-namespace",
         .site = {.file = "quality/cluster.yaml",
                  .caller = "quality.cluster.yaml::cluster.x-k8s.io/MachineDeployment/quality/md",
                  .start_byte = 1231,
                  .end_byte = 1331},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::infrastructure.cluster.x-k8s.io/"
                              "DockerMachineTemplate/unrelated/md-infra",
                    .label = "Resource",
                    .declaration_line = 40,
                    .declaration_text = "  name: md-infra"}},
        {.case_id = "configured-class-infrastructure-template-not-other-namespace",
         .site = {.file = "quality/cluster.yaml",
                  .caller = "quality.cluster.yaml::cluster.x-k8s.io/ClusterClass/quality/quick",
                  .start_byte = 1742,
                  .end_byte = 1853},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::infrastructure.cluster.x-k8s.io/"
                              "DockerClusterTemplate/unrelated/quick-cluster",
                    .label = "Resource",
                    .declaration_line = 46,
                    .declaration_text = "  name: quick-cluster"}},
        {.case_id = "configured-class-bootstrap-template-not-other-namespace",
         .site = {.file = "quality/cluster.yaml",
                  .caller = "quality.cluster.yaml::cluster.x-k8s.io/ClusterClass/quality/quick",
                  .start_byte = 1960,
                  .end_byte = 2072},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::bootstrap.cluster.x-k8s.io/"
                              "KubeadmConfigTemplate/unrelated/md-boot",
                    .label = "Resource",
                    .declaration_line = 34,
                    .declaration_text = "  name: md-boot"}},
        {.case_id = "configured-infrastructure-not-other-api-group",
         .site = {.file = "quality/cluster.yaml",
                  .caller = "quality.cluster.yaml::cluster.x-k8s.io/Cluster/quality/c1",
                  .start_byte = 124,
                  .end_byte = 202},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::wrong.example/DockerCluster/quality/c1",
                    .label = "Resource",
                    .declaration_line = 52,
                    .declaration_text = "  name: c1"}},
        {.case_id = "configured-cnpg-is-not-capi",
         .site = {.file = "quality/cluster.yaml",
                  .caller = "quality.cluster.yaml::postgresql.cnpg.io/Cluster/quality/pg",
                  .start_byte = 2335,
                  .end_byte = 2367},
         .relation = "USAGE",
         .any_target = true}};
    ASSERT_TRUE(gpf_infra_quality(configured, 3, "probe_capi_refs_follow_the_api_group/configured",
                                  expected, 9, forbidden, 11));
    PASS();
}

/* cert-manager and Kubermatic: a Certificate's issuer (Issuer when the
 * kind is omitted) and Secret, an Ingress's cluster-issuer annotation, an
 * issuer's CA and DNS-solver Secrets (`*SecretRef` of any provider), a KKP
 * Seed's kubeconfig and a user cluster's provider credentials. */
TEST(probe_cert_manager_and_kkp_refs) {
    static const char doc[] =
        "apiVersion: cert-manager.io/v1\nkind: Issuer\nmetadata:\n  name: ca\n"
        "spec:\n  ca:\n    secretName: ca-key\n---\n"
        "apiVersion: cert-manager.io/v1\nkind: ClusterIssuer\nmetadata:\n  name: letsencrypt\n"
        "spec:\n  acme:\n    solvers:\n      - dns01:\n          cloudflare:\n"
        "            apiTokenSecretRef:\n              name: cf-token\n              key: token\n"
        "---\n"
        "apiVersion: cert-manager.io/v1\nkind: Certificate\nmetadata:\n  name: web\n"
        "spec:\n  secretName: web-tls\n  issuerRef:\n    name: ca\n---\n"
        "apiVersion: networking.k8s.io/v1\nkind: Ingress\nmetadata:\n  name: web\n"
        "  annotations:\n    cert-manager.io/cluster-issuer: letsencrypt\n---\n"
        "apiVersion: v1\nkind: Secret\nmetadata:\n  name: ca-key\n---\n"
        "apiVersion: v1\nkind: Secret\nmetadata:\n  name: cf-token\n---\n"
        "apiVersion: v1\nkind: Secret\nmetadata:\n  name: web-tls\n---\n"
        "apiVersion: kubermatic.k8c.io/v1\nkind: Seed\nmetadata:\n  name: europe\n"
        "  namespace: kubermatic\nspec:\n  kubeconfig:\n    name: seed-kubeconfig\n"
        "    namespace: kubermatic\n---\n"
        "apiVersion: kubermatic.k8c.io/v1\nkind: Cluster\nmetadata:\n  name: user1\n"
        "spec:\n  cloud:\n    aws:\n      credentialsReference:\n        name: aws-creds\n"
        "        namespace: kubermatic\n---\n"
        "apiVersion: v1\nkind: Secret\nmetadata:\n  name: seed-kubeconfig\n"
        "  namespace: kubermatic\n---\n"
        "apiVersion: v1\nkind: Secret\nmetadata:\n  name: aws-creds\n  namespace: kubermatic\n";
    static const GpfFile files[] = {{"pki/certs.yaml", doc}};
    /* WHY: six original named references are genuine under selected membership.
     * A ClusterIssuer's Secret uses the controller's cluster-resource namespace,
     * not source-file proximity or a Certificate namespace. Tried the unchanged
     * source with no such context, then a separate copy with literal namespaces,
     * explicit Secret kinds, and an authored controller flag. The latter measures
     * all seven dependencies; wrong namespaces and kinds remain forbidden.
     * Secret contents and successful reconciliation are outside this identity test. */
    const GpfFile original_build[] = {files[0],
                                      {"pki/kustomization.yaml",
                                       "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: "
                                       "Kustomization\nresources:\n  - certs.yaml\n"}};
    const RQ_Expected original_expected[] = {
        {.case_id = "original-issuer-ca-secret",
         .site = {.file = "pki/certs.yaml",
                  .caller = "pki.certs.yaml::cert-manager.io/Issuer/-/ca",
                  .start_byte = 93,
                  .end_byte = 99},
         .relation = "USAGE",
         .target = {.file = "pki/certs.yaml",
                    .suffix = "pki.certs.yaml::core/Secret/-/ca-key",
                    .label = "Resource",
                    .declaration_line = 41,
                    .declaration_text = "  name: ca-key"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "cert_manager_kkp_reference_proof_missing",
         .why = "The controller field fixes the referenced kind and namespace in one selected "
                "build. Cert-manager defaults issuerRef to Issuer; KKP kubeconfig and "
                "GlobalSecretKeySelector select Secrets. Custom-controller immutable reference "
                "proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "original-certificate-issuer",
         .site = {.file = "pki/certs.yaml",
                  .caller = "pki.certs.yaml::cert-manager.io/Certificate/-/web",
                  .start_byte = 454,
                  .end_byte = 462},
         .relation = "USAGE",
         .target = {.file = "pki/certs.yaml",
                    .suffix = "pki.certs.yaml::cert-manager.io/Issuer/-/ca",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: ca"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "cert_manager_kkp_reference_proof_missing",
         .why = "The controller field fixes the referenced kind and namespace in one selected "
                "build. Cert-manager defaults issuerRef to Issuer; KKP kubeconfig and "
                "GlobalSecretKeySelector select Secrets. Custom-controller immutable reference "
                "proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "original-certificate-secret",
         .site = {.file = "pki/certs.yaml",
                  .caller = "pki.certs.yaml::cert-manager.io/Certificate/-/web",
                  .start_byte = 429,
                  .end_byte = 436},
         .relation = "USAGE",
         .target = {.file = "pki/certs.yaml",
                    .suffix = "pki.certs.yaml::core/Secret/-/web-tls",
                    .label = "Resource",
                    .declaration_line = 51,
                    .declaration_text = "  name: web-tls"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "cert_manager_kkp_reference_proof_missing",
         .why = "The controller field fixes the referenced kind and namespace in one selected "
                "build. Cert-manager defaults issuerRef to Issuer; KKP kubeconfig and "
                "GlobalSecretKeySelector select Secrets. Custom-controller immutable reference "
                "proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "original-ingress-cluster-issuer",
         .site = {.file = "pki/certs.yaml",
                  .caller = "pki.certs.yaml::networking.k8s.io/Ingress/-/web",
                  .start_byte = 587,
                  .end_byte = 598},
         .relation = "USAGE",
         .target = {.file = "pki/certs.yaml",
                    .suffix = "pki.certs.yaml::cert-manager.io/ClusterIssuer/-/letsencrypt",
                    .label = "Resource",
                    .declaration_line = 12,
                    .declaration_text = "  name: letsencrypt"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "cert_manager_kkp_reference_proof_missing",
         .why = "The controller field fixes the referenced kind and namespace in one selected "
                "build. Cert-manager defaults issuerRef to Issuer; KKP kubeconfig and "
                "GlobalSecretKeySelector select Secrets. Custom-controller immutable reference "
                "proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "original-seed-kubeconfig",
         .site = {.file = "pki/certs.yaml",
                  .caller = "pki.certs.yaml::kubermatic.k8c.io/Seed/kubermatic/europe",
                  .start_byte = 894,
                  .end_byte = 941},
         .relation = "USAGE",
         .target = {.file = "pki/certs.yaml",
                    .suffix = "pki.certs.yaml::core/Secret/kubermatic/seed-kubeconfig",
                    .label = "Resource",
                    .declaration_line = 77,
                    .declaration_text = "  name: seed-kubeconfig"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "cert_manager_kkp_reference_proof_missing",
         .why = "The controller field fixes the referenced kind and namespace in one selected "
                "build. Cert-manager defaults issuerRef to Issuer; KKP kubeconfig and "
                "GlobalSecretKeySelector select Secrets. Custom-controller immutable reference "
                "proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "original-cluster-credentials",
         .site = {.file = "pki/certs.yaml",
                  .caller = "pki.certs.yaml::kubermatic.k8c.io/Cluster/-/user1",
                  .start_byte = 1077,
                  .end_byte = 1122},
         .relation = "USAGE",
         .target = {.file = "pki/certs.yaml",
                    .suffix = "pki.certs.yaml::core/Secret/kubermatic/aws-creds",
                    .label = "Resource",
                    .declaration_line = 83,
                    .declaration_text = "  name: aws-creds"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "cert_manager_kkp_reference_proof_missing",
         .why = "The controller field fixes the referenced kind and namespace in one selected "
                "build. Cert-manager defaults issuerRef to Issuer; KKP kubeconfig and "
                "GlobalSecretKeySelector select Secrets. Custom-controller immutable reference "
                "proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."}};
    const RQ_Forbidden original_forbidden[] = {
        {.case_id = "original-cluster-issuer-secret-namespace-unproved",
         .site = {.file = "pki/certs.yaml",
                  .caller = "pki.certs.yaml::cert-manager.io/ClusterIssuer/-/letsencrypt",
                  .start_byte = 300,
                  .end_byte = 308},
         .relation = "USAGE",
         .any_target = true}};
    ASSERT_TRUE(gpf_infra_quality(original_build, 2, "probe_cert_manager_and_kkp_refs/original",
                                  original_expected, 6, original_forbidden, 1));
    const GpfFile configured[] = {
        {"quality/certs.yaml",
         "apiVersion: cert-manager.io/v1\nkind: Issuer\nmetadata:\n  name: ca\n  namespace: "
         "quality\nspec:\n  ca:\n    secretName: ca-key\n---\napiVersion: "
         "cert-manager.io/v1\nkind: ClusterIssuer\nmetadata:\n  name: letsencrypt\nspec:\n  "
         "acme:\n    solvers:\n      - dns01:\n          cloudflare:\n            "
         "apiTokenSecretRef:\n              name: cf-token\n              key: "
         "token\n---\napiVersion: cert-manager.io/v1\nkind: Certificate\nmetadata:\n  name: web\n  "
         "namespace: quality\nspec:\n  secretName: web-tls\n  issuerRef:\n    name: "
         "ca\n---\napiVersion: networking.k8s.io/v1\nkind: Ingress\nmetadata:\n  name: web\n  "
         "namespace: quality\n  annotations:\n    cert-manager.io/cluster-issuer: "
         "letsencrypt\n---\napiVersion: v1\nkind: Secret\nmetadata:\n  name: ca-key\n  namespace: "
         "quality\n---\napiVersion: v1\nkind: Secret\nmetadata:\n  name: cf-token\n  namespace: "
         "cert-manager\n---\napiVersion: v1\nkind: Secret\nmetadata:\n  name: web-tls\n  "
         "namespace: quality\n---\napiVersion: kubermatic.k8c.io/v1\nkind: Seed\nmetadata:\n  "
         "name: europe\n  namespace: kubermatic\nspec:\n  kubeconfig:\n    apiVersion: v1\n    "
         "kind: Secret\n    name: seed-kubeconfig\n    namespace: kubermatic\n---\napiVersion: "
         "kubermatic.k8c.io/v1\nkind: Cluster\nmetadata:\n  name: user1\nspec:\n  cloud:\n    "
         "aws:\n      credentialsReference:\n        apiVersion: v1\n        kind: Secret\n        "
         "name: aws-creds\n        namespace: kubermatic\n---\napiVersion: v1\nkind: "
         "Secret\nmetadata:\n  name: seed-kubeconfig\n  namespace: kubermatic\n---\napiVersion: "
         "v1\nkind: Secret\nmetadata:\n  name: aws-creds\n  namespace: kubermatic\n"},
        {"quality/controller.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: cert-manager\n  namespace: "
         "cert-manager\nspec:\n  selector:\n    matchLabels:\n      app: cert-manager\n  "
         "template:\n    metadata:\n      labels:\n        app: cert-manager\n    spec:\n      "
         "containers:\n        - name: controller\n          image: "
         "quay.io/jetstack/cert-manager-controller:v1.16.0\n          args:\n            - "
         "--cluster-resource-namespace=cert-manager\n"},
        {"quality/decoys.yaml",
         "apiVersion: v1\nkind: Secret\nmetadata:\n  name: ca-key\n  namespace: "
         "unrelated\n---\napiVersion: v1\nkind: Secret\nmetadata:\n  name: cf-token\n  namespace: "
         "quality\n---\napiVersion: cert-manager.io/v1\nkind: Issuer\nmetadata:\n  name: ca\n  "
         "namespace: unrelated\n---\napiVersion: v1\nkind: Secret\nmetadata:\n  name: web-tls\n  "
         "namespace: unrelated\n---\napiVersion: cert-manager.io/v1\nkind: Issuer\nmetadata:\n  "
         "name: letsencrypt\n  namespace: quality\n---\napiVersion: v1\nkind: "
         "ConfigMap\nmetadata:\n  name: seed-kubeconfig\n  namespace: kubermatic\n---\napiVersion: "
         "v1\nkind: ConfigMap\nmetadata:\n  name: aws-creds\n  namespace: kubermatic\n"},
        {"quality/kustomization.yaml",
         "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - "
         "certs.yaml\n  - controller.yaml\n  - decoys.yaml\n"}};
    const RQ_Expected expected[] = {
        {.case_id = "configured-issuer-ca-secret",
         .site = {.file = "quality/certs.yaml",
                  .caller = "quality.certs.yaml::cert-manager.io/Issuer/quality/ca",
                  .start_byte = 114,
                  .end_byte = 120},
         .relation = "USAGE",
         .target = {.file = "quality/certs.yaml",
                    .suffix = "quality.certs.yaml::core/Secret/quality/ca-key",
                    .label = "Resource",
                    .declaration_line = 44,
                    .declaration_text = "  name: ca-key"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "cert_manager_kkp_reference_proof_missing",
         .why = "The controller field fixes the referenced kind and namespace in one selected "
                "build. Cert-manager defaults issuerRef to Issuer; KKP kubeconfig and "
                "GlobalSecretKeySelector select Secrets. Custom-controller immutable reference "
                "proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "configured-cluster-issuer-dns-secret",
         .site = {.file = "quality/certs.yaml",
                  .caller = "quality.certs.yaml::cert-manager.io/ClusterIssuer/-/letsencrypt",
                  .start_byte = 321,
                  .end_byte = 329},
         .relation = "USAGE",
         .target = {.file = "quality/certs.yaml",
                    .suffix = "quality.certs.yaml::core/Secret/cert-manager/cf-token",
                    .label = "Resource",
                    .declaration_line = 50,
                    .declaration_text = "  name: cf-token"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "cluster_issuer_controller_namespace_proof_missing",
         .why = "The explicitly configured cert-manager controller uses "
                "cluster-resource-namespace=cert-manager, selecting this Secret; same-file or "
                "Certificate namespace cannot substitute for that configuration. This "
                "controller-context proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "configured-certificate-issuer",
         .site = {.file = "quality/certs.yaml",
                  .caller = "quality.certs.yaml::cert-manager.io/Certificate/quality/web",
                  .start_byte = 496,
                  .end_byte = 504},
         .relation = "USAGE",
         .target = {.file = "quality/certs.yaml",
                    .suffix = "quality.certs.yaml::cert-manager.io/Issuer/quality/ca",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: ca"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "cert_manager_kkp_reference_proof_missing",
         .why = "The controller field fixes the referenced kind and namespace in one selected "
                "build. Cert-manager defaults issuerRef to Issuer; KKP kubeconfig and "
                "GlobalSecretKeySelector select Secrets. Custom-controller immutable reference "
                "proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "configured-certificate-secret",
         .site = {.file = "quality/certs.yaml",
                  .caller = "quality.certs.yaml::cert-manager.io/Certificate/quality/web",
                  .start_byte = 471,
                  .end_byte = 478},
         .relation = "USAGE",
         .target = {.file = "quality/certs.yaml",
                    .suffix = "quality.certs.yaml::core/Secret/quality/web-tls",
                    .label = "Resource",
                    .declaration_line = 56,
                    .declaration_text = "  name: web-tls"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "cert_manager_kkp_reference_proof_missing",
         .why = "The controller field fixes the referenced kind and namespace in one selected "
                "build. Cert-manager defaults issuerRef to Issuer; KKP kubeconfig and "
                "GlobalSecretKeySelector select Secrets. Custom-controller immutable reference "
                "proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "configured-ingress-cluster-issuer",
         .site = {.file = "quality/certs.yaml",
                  .caller = "quality.certs.yaml::networking.k8s.io/Ingress/quality/web",
                  .start_byte = 650,
                  .end_byte = 661},
         .relation = "USAGE",
         .target = {.file = "quality/certs.yaml",
                    .suffix = "quality.certs.yaml::cert-manager.io/ClusterIssuer/-/letsencrypt",
                    .label = "Resource",
                    .declaration_line = 13,
                    .declaration_text = "  name: letsencrypt"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "cert_manager_kkp_reference_proof_missing",
         .why = "The controller field fixes the referenced kind and namespace in one selected "
                "build. Cert-manager defaults issuerRef to Issuer; KKP kubeconfig and "
                "GlobalSecretKeySelector select Secrets. Custom-controller immutable reference "
                "proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "configured-seed-kubeconfig",
         .site = {.file = "quality/certs.yaml",
                  .caller = "quality.certs.yaml::kubermatic.k8c.io/Seed/kubermatic/europe",
                  .start_byte = 1025,
                  .end_byte = 1108},
         .relation = "USAGE",
         .target = {.file = "quality/certs.yaml",
                    .suffix = "quality.certs.yaml::core/Secret/kubermatic/seed-kubeconfig",
                    .label = "Resource",
                    .declaration_line = 87,
                    .declaration_text = "  name: seed-kubeconfig"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "cert_manager_kkp_reference_proof_missing",
         .why = "The controller field fixes the referenced kind and namespace in one selected "
                "build. Cert-manager defaults issuerRef to Issuer; KKP kubeconfig and "
                "GlobalSecretKeySelector select Secrets. Custom-controller immutable reference "
                "proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."},
        {.case_id = "configured-cluster-credentials",
         .site = {.file = "quality/certs.yaml",
                  .caller = "quality.certs.yaml::kubermatic.k8c.io/Cluster/-/user1",
                  .start_byte = 1244,
                  .end_byte = 1333},
         .relation = "USAGE",
         .target = {.file = "quality/certs.yaml",
                    .suffix = "quality.certs.yaml::core/Secret/kubermatic/aws-creds",
                    .label = "Resource",
                    .declaration_line = 93,
                    .declaration_text = "  name: aws-creds"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "cert_manager_kkp_reference_proof_missing",
         .why = "The controller field fixes the referenced kind and namespace in one selected "
                "build. Cert-manager defaults issuerRef to Issuer; KKP kubeconfig and "
                "GlobalSecretKeySelector select Secrets. Custom-controller immutable reference "
                "proof is not implemented.",
         .tried = "Preserved original source bytes; selected explicit flat build membership and "
                  "independently anchored source occurrences and declarations. Both drivers "
                  "require an exact ESM call; wrong targets and roles remain failures."}};
    const RQ_Forbidden forbidden[] = {
        {.case_id = "configured-issuer-ca-secret-not-other-namespace-or-kind",
         .site = {.file = "quality/certs.yaml",
                  .caller = "quality.certs.yaml::cert-manager.io/Issuer/quality/ca",
                  .start_byte = 114,
                  .end_byte = 120},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::core/Secret/unrelated/ca-key",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: ca-key"}},
        {.case_id = "configured-cluster-issuer-dns-secret-not-other-namespace-or-kind",
         .site = {.file = "quality/certs.yaml",
                  .caller = "quality.certs.yaml::cert-manager.io/ClusterIssuer/-/letsencrypt",
                  .start_byte = 321,
                  .end_byte = 329},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::core/Secret/quality/cf-token",
                    .label = "Resource",
                    .declaration_line = 10,
                    .declaration_text = "  name: cf-token"}},
        {.case_id = "configured-certificate-issuer-not-other-namespace-or-kind",
         .site = {.file = "quality/certs.yaml",
                  .caller = "quality.certs.yaml::cert-manager.io/Certificate/quality/web",
                  .start_byte = 496,
                  .end_byte = 504},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::cert-manager.io/Issuer/unrelated/ca",
                    .label = "Resource",
                    .declaration_line = 16,
                    .declaration_text = "  name: ca"}},
        {.case_id = "configured-certificate-secret-not-other-namespace-or-kind",
         .site = {.file = "quality/certs.yaml",
                  .caller = "quality.certs.yaml::cert-manager.io/Certificate/quality/web",
                  .start_byte = 471,
                  .end_byte = 478},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::core/Secret/unrelated/web-tls",
                    .label = "Resource",
                    .declaration_line = 22,
                    .declaration_text = "  name: web-tls"}},
        {.case_id = "configured-ingress-cluster-issuer-not-other-namespace-or-kind",
         .site = {.file = "quality/certs.yaml",
                  .caller = "quality.certs.yaml::networking.k8s.io/Ingress/quality/web",
                  .start_byte = 650,
                  .end_byte = 661},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::cert-manager.io/Issuer/quality/letsencrypt",
                    .label = "Resource",
                    .declaration_line = 28,
                    .declaration_text = "  name: letsencrypt"}},
        {.case_id = "configured-seed-kubeconfig-not-other-namespace-or-kind",
         .site = {.file = "quality/certs.yaml",
                  .caller = "quality.certs.yaml::kubermatic.k8c.io/Seed/kubermatic/europe",
                  .start_byte = 1025,
                  .end_byte = 1108},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::core/ConfigMap/kubermatic/seed-kubeconfig",
                    .label = "Resource",
                    .declaration_line = 34,
                    .declaration_text = "  name: seed-kubeconfig"}},
        {.case_id = "configured-cluster-credentials-not-other-namespace-or-kind",
         .site = {.file = "quality/certs.yaml",
                  .caller = "quality.certs.yaml::kubermatic.k8c.io/Cluster/-/user1",
                  .start_byte = 1244,
                  .end_byte = 1333},
         .relation = "USAGE",
         .target = {.file = "quality/decoys.yaml",
                    .suffix = "quality.decoys.yaml::core/ConfigMap/kubermatic/aws-creds",
                    .label = "Resource",
                    .declaration_line = 40,
                    .declaration_text = "  name: aws-creds"}}};
    ASSERT_TRUE(gpf_infra_quality(configured, 4, "probe_cert_manager_and_kkp_refs/configured",
                                  expected, 7, forbidden, 7));
    PASS();
}

/* Argo: an Application's project, and a Rollout -- a workload with a pod
 * template -- names its Services, analysis template and service account. */
TEST(probe_argo_application_and_rollout_refs) {
    static const char doc[] =
        "apiVersion: argoproj.io/v1alpha1\nkind: AppProject\nmetadata:\n  name: team-a\n"
        "  namespace: argocd\n---\n"
        "apiVersion: argoproj.io/v1alpha1\nkind: Application\nmetadata:\n  name: web\n"
        "  namespace: argocd\nspec:\n  project: team-a\n  source:\n    path: apps/web\n"
        "  destination:\n    namespace: web\n---\n"
        "apiVersion: argoproj.io/v1alpha1\nkind: Rollout\nmetadata:\n  name: web\n"
        "spec:\n  strategy:\n    canary:\n      canaryService: web-canary\n"
        "      stableService: web-stable\n      analysis:\n        templates:\n"
        "          - templateName: success-rate\n  template:\n    spec:\n"
        "      serviceAccountName: web\n---\n"
        "apiVersion: v1\nkind: Service\nmetadata:\n  name: web-canary\n---\n"
        "apiVersion: v1\nkind: Service\nmetadata:\n  name: web-stable\n---\n"
        "apiVersion: argoproj.io/v1alpha1\nkind: AnalysisTemplate\nmetadata:\n"
        "  name: success-rate\n---\n"
        "apiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: web\n";
    static const GpfFile files[] = {{"apps/web/rollout.yaml", doc}};
    /* WHY: these controller-defined references have exact source meanings but
     * no immutable custom-schema proof. Tried the original fields and declarations
     * in one explicit flat build; Application destination does not rewrite the
     * authored Rollout namespace. All five obligations remain counted. */
    const GpfFile configured[] = {
        files[0], {"apps/web/kustomization.yaml", "resources:\n  - rollout.yaml\n"}};
    const RQ_Expected expected[] = {
        {.case_id = "argo-project",
         .site = {.file = "apps/web/rollout.yaml",
                  .caller = "apps.web.rollout.yaml::argoproj.io/Application/argocd/web",
                  .start_byte = 209,
                  .end_byte = 215},
         .relation = "USAGE",
         .target = {.file = "apps/web/rollout.yaml",
                    .suffix = "apps.web.rollout.yaml::argoproj.io/AppProject/argocd/team-a",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: team-a"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "argo_schema_reference_proof_missing",
         .why = "The explicit Argo field names this exact declaration in its namespace. "
                "Custom-source controller schemas are outside the builtin proof adapter.",
         .tried = "Preserved original source bytes; explicit build membership and exact "
                  "declaration/site goldens run with sequential and parallel drivers. Wrong "
                  "targets and roles remain failures."},
        {.case_id = "argo-canary-service",
         .site = {.file = "apps/web/rollout.yaml",
                  .caller = "apps.web.rollout.yaml::argoproj.io/Rollout/-/web",
                  .start_byte = 403,
                  .end_byte = 413},
         .relation = "USAGE",
         .target = {.file = "apps/web/rollout.yaml",
                    .suffix = "apps.web.rollout.yaml::core/Service/-/web-canary",
                    .label = "Resource",
                    .declaration_line = 38,
                    .declaration_text = "  name: web-canary"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "argo_schema_reference_proof_missing",
         .why = "The explicit Argo field names this exact declaration in its namespace. "
                "Custom-source controller schemas are outside the builtin proof adapter.",
         .tried = "Preserved original source bytes; explicit build membership and exact "
                  "declaration/site goldens run with sequential and parallel drivers. Wrong "
                  "targets and roles remain failures."},
        {.case_id = "argo-stable-service",
         .site = {.file = "apps/web/rollout.yaml",
                  .caller = "apps.web.rollout.yaml::argoproj.io/Rollout/-/web",
                  .start_byte = 435,
                  .end_byte = 445},
         .relation = "USAGE",
         .target = {.file = "apps/web/rollout.yaml",
                    .suffix = "apps.web.rollout.yaml::core/Service/-/web-stable",
                    .label = "Resource",
                    .declaration_line = 43,
                    .declaration_text = "  name: web-stable"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "argo_schema_reference_proof_missing",
         .why = "The explicit Argo field names this exact declaration in its namespace. "
                "Custom-source controller schemas are outside the builtin proof adapter.",
         .tried = "Preserved original source bytes; explicit build membership and exact "
                  "declaration/site goldens run with sequential and parallel drivers. Wrong "
                  "targets and roles remain failures."},
        {.case_id = "argo-analysis",
         .site = {.file = "apps/web/rollout.yaml",
                  .caller = "apps.web.rollout.yaml::argoproj.io/Rollout/-/web",
                  .start_byte = 507,
                  .end_byte = 519},
         .relation = "USAGE",
         .target = {.file = "apps/web/rollout.yaml",
                    .suffix = "apps.web.rollout.yaml::argoproj.io/AnalysisTemplate/-/success-rate",
                    .label = "Resource",
                    .declaration_line = 48,
                    .declaration_text = "  name: success-rate"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "argo_schema_reference_proof_missing",
         .why = "The explicit Argo field names this exact declaration in its namespace. "
                "Custom-source controller schemas are outside the builtin proof adapter.",
         .tried = "Preserved original source bytes; explicit build membership and exact "
                  "declaration/site goldens run with sequential and parallel drivers. Wrong "
                  "targets and roles remain failures."},
        {.case_id = "argo-service-account",
         .site = {.file = "apps/web/rollout.yaml",
                  .caller = "apps.web.rollout.yaml::argoproj.io/Rollout/-/web",
                  .start_byte = 568,
                  .end_byte = 571},
         .relation = "USAGE",
         .target = {.file = "apps/web/rollout.yaml",
                    .suffix = "apps.web.rollout.yaml::core/ServiceAccount/-/web",
                    .label = "Resource",
                    .declaration_line = 53,
                    .declaration_text = "  name: web"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "argo_schema_reference_proof_missing",
         .why = "The explicit Argo field names this exact declaration in its namespace. "
                "Custom-source controller schemas are outside the builtin proof adapter.",
         .tried = "Preserved original source bytes; explicit build membership and exact "
                  "declaration/site goldens run with sequential and parallel drivers. Wrong "
                  "targets and roles remain failures."}};
    const RQ_Forbidden forbidden[] = {
        {.case_id = "canary-not-stable",
         .site = {.file = "apps/web/rollout.yaml",
                  .caller = "apps.web.rollout.yaml::argoproj.io/Rollout/-/web",
                  .start_byte = 403,
                  .end_byte = 413},
         .relation = "USAGE",
         .target = {.file = "apps/web/rollout.yaml",
                    .suffix = "apps.web.rollout.yaml::core/Service/-/web-stable",
                    .label = "Resource",
                    .declaration_line = 43,
                    .declaration_text = "  name: web-stable"}},
        {.case_id = "stable-not-canary",
         .site = {.file = "apps/web/rollout.yaml",
                  .caller = "apps.web.rollout.yaml::argoproj.io/Rollout/-/web",
                  .start_byte = 435,
                  .end_byte = 445},
         .relation = "USAGE",
         .target = {.file = "apps/web/rollout.yaml",
                    .suffix = "apps.web.rollout.yaml::core/Service/-/web-canary",
                    .label = "Resource",
                    .declaration_line = 38,
                    .declaration_text = "  name: web-canary"}},
        {.case_id = "service-account-not-service",
         .site = {.file = "apps/web/rollout.yaml",
                  .caller = "apps.web.rollout.yaml::argoproj.io/Rollout/-/web",
                  .start_byte = 568,
                  .end_byte = 571},
         .relation = "USAGE",
         .target = {.file = "apps/web/rollout.yaml",
                    .suffix = "apps.web.rollout.yaml::core/Service/-/web-canary",
                    .label = "Resource",
                    .declaration_line = 38,
                    .declaration_text = "  name: web-canary"}}};
    ASSERT_TRUE(gpf_infra_quality(configured, 2, "probe_argo_application_and_rollout_refs",
                                  expected, 5, forbidden, 3));
    PASS();
}

/* ── Exposed routes + policy selectors (P1.4) ───────────────────────── */

/* Policy selectors refer to workloads by pod labels. Empty podSelector covers
 * every workload in the namespace; additional expressions still constrain it. */
TEST(probe_k8s_pdb_and_networkpolicy_select_workloads) {
    static const char doc[] =
        "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\nspec:\n  template:\n"
        "    metadata:\n      labels:\n        app: web\n---\n"
        "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: api\nspec:\n  template:\n"
        "    metadata:\n      labels:\n        app: api\n---\n"
        "apiVersion: policy/v1\nkind: PodDisruptionBudget\nmetadata:\n  name: web\nspec:\n"
        "  minAvailable: 1\n  selector:\n    matchLabels:\n      app: web\n---\n"
        "apiVersion: policy/v1\nkind: PodDisruptionBudget\nmetadata:\n  name: narrowed\nspec:\n"
        "  selector:\n    matchLabels:\n      app: web\n    matchExpressions:\n"
        "      - key: track\n        operator: In\n        values: [canary]\n---\n"
        "apiVersion: networking.k8s.io/v1\nkind: NetworkPolicy\nmetadata:\n  name: api\nspec:\n"
        "  podSelector:\n    matchLabels:\n      app: api\n---\n"
        "apiVersion: networking.k8s.io/v1\nkind: NetworkPolicy\nmetadata:\n  name: deny-all\n"
        "spec:\n  podSelector: {}\n";
    static const GpfFile files[] = {{"k8s/app.yaml", doc}};
    /* WHY: selector-to-workload proof is missing. Tried explicit flat build
     * membership and all original labels. Empty podSelector selects both workloads;
     * the additional track requirement selects neither. These are separate facts,
     * not a name/proximity match or an all-zero expectation. */
    const GpfFile configured[] = {files[0],
                                  {"k8s/kustomization.yaml", "resources:\n  - app.yaml\n"}};
    const RQ_Expected expected[] = {
        {.case_id = "pdb-web",
         .site = {.file = "k8s/app.yaml",
                  .caller = "k8s.app.yaml::policy/PodDisruptionBudget/-/web",
                  .start_byte = 362,
                  .end_byte = 389},
         .relation = "USAGE",
         .target = {.file = "k8s/app.yaml",
                    .suffix = "k8s.app.yaml::apps/Deployment/-/web",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: web"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "k8s_selector_proof_missing",
         .why = "Workload pod labels satisfy this source selector in the explicit build, but "
                "selector-to-workload immutable evidence is not implemented.",
         .tried = "Preserved original source bytes; explicit build membership and exact "
                  "declaration/site goldens run with sequential and parallel drivers. Wrong "
                  "targets and roles remain failures."},
        {.case_id = "network-policy-api",
         .site = {.file = "k8s/app.yaml",
                  .caller = "k8s.app.yaml::networking.k8s.io/NetworkPolicy/-/api",
                  .start_byte = 710,
                  .end_byte = 737},
         .relation = "USAGE",
         .target = {.file = "k8s/app.yaml",
                    .suffix = "k8s.app.yaml::apps/Deployment/-/api",
                    .label = "Resource",
                    .declaration_line = 14,
                    .declaration_text = "  name: api"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "k8s_selector_proof_missing",
         .why = "Workload pod labels satisfy this source selector in the explicit build, but "
                "selector-to-workload immutable evidence is not implemented.",
         .tried = "Preserved original source bytes; explicit build membership and exact "
                  "declaration/site goldens run with sequential and parallel drivers. Wrong "
                  "targets and roles remain failures."},
        {.case_id = "empty-network-policy-web",
         .site = {.file = "k8s/app.yaml",
                  .caller = "k8s.app.yaml::networking.k8s.io/NetworkPolicy/-/deny-all",
                  .start_byte = 843,
                  .end_byte = 845},
         .relation = "USAGE",
         .target = {.file = "k8s/app.yaml",
                    .suffix = "k8s.app.yaml::apps/Deployment/-/web",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: web"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "k8s_empty_selector_proof_missing",
         .why = "An empty podSelector selects every pod in its namespace, including this exact "
                "workload; absence is quality debt.",
         .tried = "Preserved original source bytes; explicit build membership and exact "
                  "declaration/site goldens run with sequential and parallel drivers. Wrong "
                  "targets and roles remain failures.",
         .dispatch_group = "empty-network-policy"},
        {.case_id = "empty-network-policy-api",
         .site = {.file = "k8s/app.yaml",
                  .caller = "k8s.app.yaml::networking.k8s.io/NetworkPolicy/-/deny-all",
                  .start_byte = 843,
                  .end_byte = 845},
         .relation = "USAGE",
         .target = {.file = "k8s/app.yaml",
                    .suffix = "k8s.app.yaml::apps/Deployment/-/api",
                    .label = "Resource",
                    .declaration_line = 14,
                    .declaration_text = "  name: api"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "k8s_empty_selector_proof_missing",
         .why = "An empty podSelector selects every pod in its namespace, including this exact "
                "workload; absence is quality debt.",
         .tried = "Preserved original source bytes; explicit build membership and exact "
                  "declaration/site goldens run with sequential and parallel drivers. Wrong "
                  "targets and roles remain failures.",
         .dispatch_group = "empty-network-policy"}};
    const RQ_Forbidden forbidden[] = {
        {.case_id = "pdb-not-api",
         .site = {.file = "k8s/app.yaml",
                  .caller = "k8s.app.yaml::policy/PodDisruptionBudget/-/web",
                  .start_byte = 362,
                  .end_byte = 389},
         .relation = "USAGE",
         .target = {.file = "k8s/app.yaml",
                    .suffix = "k8s.app.yaml::apps/Deployment/-/api",
                    .label = "Resource",
                    .declaration_line = 14,
                    .declaration_text = "  name: api"}},
        {.case_id = "network-policy-not-web",
         .site = {.file = "k8s/app.yaml",
                  .caller = "k8s.app.yaml::networking.k8s.io/NetworkPolicy/-/api",
                  .start_byte = 710,
                  .end_byte = 737},
         .relation = "USAGE",
         .target = {.file = "k8s/app.yaml",
                    .suffix = "k8s.app.yaml::apps/Deployment/-/web",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: web"}},
        {.case_id = "narrowed-selector-has-no-matching-workload",
         .site = {.file = "k8s/app.yaml",
                  .caller = "k8s.app.yaml::policy/PodDisruptionBudget/-/narrowed",
                  .start_byte = 475,
                  .end_byte = 605},
         .relation = "USAGE",
         .any_target = true}};
    ASSERT_TRUE(gpf_infra_quality(configured, 2, "probe_k8s_pdb_and_networkpolicy_select_workloads",
                                  expected, 4, forbidden, 3));
    PASS();
}

/* Versioned manifests require explicit build selection before a PDB can
 * bind a workload. Own-file proximity is not version-selection authority. */
TEST(probe_k8s_selectors_pick_their_own_version) {
    static const char v1[] =
        "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: kube-controllers\nspec:\n"
        "  template:\n    metadata:\n      labels:\n        app: kube-controllers\n---\n"
        "apiVersion: policy/v1\nkind: PodDisruptionBudget\nmetadata:\n"
        "  name: kube-controllers\nspec:\n  selector:\n    matchLabels:\n"
        "      app: kube-controllers\n---\n"
        "apiVersion: v1\nkind: Service\nmetadata:\n  name: metrics\nspec:\n  selector:\n"
        "    app: kube-controllers\n";
    static const GpfFile files[] = {{"addons/canal/canal_v1.yaml", v1},
                                    {"addons/canal/canal_v2.yaml", v1}};
    /* WHY: file proximity does not select one deployed version. Tried the
     * original ambiguous pair and two separate explicit flat builds; source
     * selectors and matching Deployment declarations are independently anchored.
     * The separate INFRA_MAPS relation remains under its existing contract. */
    const RQ_Forbidden original_forbidden[] = {
        {.case_id = "unconfigured-pdb-version-1",
         .site = {.file = "addons/canal/canal_v1.yaml",
                  .caller =
                      "addons.canal.canal_v1.yaml::policy/PodDisruptionBudget/-/kube-controllers",
                  .start_byte = 257,
                  .end_byte = 297},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "unconfigured-pdb-version-2",
         .site = {.file = "addons/canal/canal_v2.yaml",
                  .caller =
                      "addons.canal.canal_v2.yaml::policy/PodDisruptionBudget/-/kube-controllers",
                  .start_byte = 257,
                  .end_byte = 297},
         .relation = "USAGE",
         .any_target = true}};
    ASSERT_TRUE(gpf_infra_quality(files, 2,
                                  "probe_k8s_selectors_pick_their_own_version/unconfigured", NULL,
                                  0, original_forbidden, 2));
    const RQ_Expected expected[] = {
        {.case_id = "selected-pdb-version-1",
         .site = {.file = "addons/canal/canal_v1.yaml",
                  .caller =
                      "addons.canal.canal_v1.yaml::policy/PodDisruptionBudget/-/kube-controllers",
                  .start_byte = 257,
                  .end_byte = 297},
         .relation = "USAGE",
         .target = {.file = "addons/canal/canal_v1.yaml",
                    .suffix = "addons.canal.canal_v1.yaml::apps/Deployment/-/kube-controllers",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: kube-controllers"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "k8s_selector_binding_proof_missing",
         .why = "The explicit flat Kustomize resource list selects exactly this PDB and "
                "Deployment; the label selector matches this declared pod template. PDB selector "
                "semantics lack an admitted immutable proof adapter.",
         .tried =
             "Preserved both original versioned manifests and the separate INFRA_MAPS contract. "
             "Independently selected each original file, required an exact ESM control in both "
             "drivers, and forbade the other physical version and unselected PDB."},
        {.case_id = "selected-pdb-version-2",
         .site = {.file = "addons/canal/canal_v2.yaml",
                  .caller =
                      "addons.canal.canal_v2.yaml::policy/PodDisruptionBudget/-/kube-controllers",
                  .start_byte = 257,
                  .end_byte = 297},
         .relation = "USAGE",
         .target = {.file = "addons/canal/canal_v2.yaml",
                    .suffix = "addons.canal.canal_v2.yaml::apps/Deployment/-/kube-controllers",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: kube-controllers"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "k8s_selector_binding_proof_missing",
         .why = "The explicit flat Kustomize resource list selects exactly this PDB and "
                "Deployment; the label selector matches this declared pod template. PDB selector "
                "semantics lack an admitted immutable proof adapter.",
         .tried =
             "Preserved both original versioned manifests and the separate INFRA_MAPS contract. "
             "Independently selected each original file, required an exact ESM control in both "
             "drivers, and forbade the other physical version and unselected PDB."}};
    const RQ_Forbidden forbidden[2][2] = {
        {{.case_id = "selected-version-1-not-other-deployment",
          .site = {.file = "addons/canal/canal_v1.yaml",
                   .caller =
                       "addons.canal.canal_v1.yaml::policy/PodDisruptionBudget/-/kube-controllers",
                   .start_byte = 257,
                   .end_byte = 297},
          .relation = "USAGE",
          .target = {.file = "addons/canal/canal_v2.yaml",
                     .suffix = "addons.canal.canal_v2.yaml::apps/Deployment/-/kube-controllers",
                     .label = "Resource",
                     .declaration_line = 4,
                     .declaration_text = "  name: kube-controllers"}},
         {.case_id = "unselected-version-2-no-binding",
          .site = {.file = "addons/canal/canal_v2.yaml",
                   .caller =
                       "addons.canal.canal_v2.yaml::policy/PodDisruptionBudget/-/kube-controllers",
                   .start_byte = 257,
                   .end_byte = 297},
          .relation = "USAGE",
          .any_target = true}},
        {{.case_id = "selected-version-2-not-other-deployment",
          .site = {.file = "addons/canal/canal_v2.yaml",
                   .caller =
                       "addons.canal.canal_v2.yaml::policy/PodDisruptionBudget/-/kube-controllers",
                   .start_byte = 257,
                   .end_byte = 297},
          .relation = "USAGE",
          .target = {.file = "addons/canal/canal_v1.yaml",
                     .suffix = "addons.canal.canal_v1.yaml::apps/Deployment/-/kube-controllers",
                     .label = "Resource",
                     .declaration_line = 4,
                     .declaration_text = "  name: kube-controllers"}},
         {.case_id = "unselected-version-1-no-binding",
          .site = {.file = "addons/canal/canal_v1.yaml",
                   .caller =
                       "addons.canal.canal_v1.yaml::policy/PodDisruptionBudget/-/kube-controllers",
                   .start_byte = 257,
                   .end_byte = 297},
          .relation = "USAGE",
          .any_target = true}}};
    const char *configs[] = {"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: "
                             "Kustomization\nresources:\n  - addons/canal/canal_v1.yaml\n",
                             "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: "
                             "Kustomization\nresources:\n  - addons/canal/canal_v2.yaml\n"};
    const char *scenarios[] = {"probe_k8s_selectors_pick_their_own_version/v1",
                               "probe_k8s_selectors_pick_their_own_version/v2"};
    for (int selected = 0; selected < 2; selected++) {
        const GpfFile configured[] = {
            files[0], files[1], {"kustomization.yaml", configs[selected]}};
        ASSERT_TRUE(gpf_infra_quality(configured, 3, scenarios[selected], &expected[selected], 1,
                                      forbidden[selected], 2));
    }
    ASSERT_EQ(
        gpf_typed_edges(files, 2, "INFRA_MAPS", "Resource", "Resource", "\"kind\":\"selector\""),
        2);
    PASS();
}

/* An Ingress rule's host and paths and an HTTPRoute's hostnames and path
 * matches are routes the cluster exposes: the object DEFINES each, and the
 * route maps to the Service it is sent to. */
TEST(probe_k8s_ingress_and_httproute_expose_routes) {
    static const char doc[] =
        "apiVersion: networking.k8s.io/v1\nkind: Ingress\nmetadata:\n  name: shop\nspec:\n"
        "  rules:\n    - host: shop.example.com\n      http:\n        paths:\n"
        "          - path: /api\n            pathType: Prefix\n            backend:\n"
        "              service:\n                name: api\n                port:\n"
        "                  number: 80\n"
        "          - path: /\n            pathType: Prefix\n            backend:\n"
        "              service:\n                name: web\n                port:\n"
        "                  number: 80\n---\n"
        "apiVersion: gateway.networking.k8s.io/v1\nkind: HTTPRoute\nmetadata:\n  name: api\n"
        "spec:\n  hostnames:\n    - api.example.com\n  rules:\n    - matches:\n"
        "        - path:\n            type: PathPrefix\n            value: /v1\n"
        "      backendRefs:\n        - name: api\n          port: 80\n---\n"
        "apiVersion: v1\nkind: Service\nmetadata:\n  name: api\n---\n"
        "apiVersion: v1\nkind: Service\nmetadata:\n  name: web\n";
    static const GpfFile files[] = {{"k8s/edge.yaml", doc}};
    ASSERT_EQ(gpf_typed_edges(files, 1, "DEFINES", "Resource", "Route", "{"), 3);
    ASSERT_EQ(gpf_typed_edges(files, 1, "INFRA_MAPS", "Route", "Resource", "\"kind\":\"ingress\""),
              2);
    ASSERT_EQ(
        gpf_typed_edges(files, 1, "INFRA_MAPS", "Route", "Resource", "\"kind\":\"httproute\""), 1);
    PASS();
}

/* ── MCP surface: infra filters + topology (P1.5) ───────────────────── */

/* Run MCP `tool` on the indexed fixture; `args_fmt` holds one %s, the
 * project. The response is the caller's to free; NULL when indexing failed. */
static char *gpf_tool_on(const GpfFile *files, int nfiles, const char *tool, const char *args_fmt) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *resp = NULL;
    if (store && lp.srv) {
        char args[2048];
        snprintf(args, sizeof(args), args_fmt, lp.project);
        resp = cbm_mcp_handle_tool(lp.srv, tool, args);
    }
    gpf_cleanup(&lp, store);
    return resp;
}

static const GpfFile k_two_teams[] = {
    {"k8s/a.yaml",
     "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  namespace: team-a\n---\n"
     "apiVersion: v1\nkind: Service\nmetadata:\n  name: web\n  namespace: team-a\n"},
    {"k8s/b.yaml",
     "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: api\n  namespace: team-b\n"}};

/* search_graph narrows Resources by kind and by effective namespace, in the
 * structural and the BM25 query mode alike. */
TEST(probe_mcp_search_filters_kind_and_namespace) {
    char *r = gpf_tool_on(k_two_teams, 2, "search_graph",
                          "{\"project\":\"%s\",\"label\":\"Resource\",\"kind\":\"Deployment\","
                          "\"format\":\"json\"}");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(strstr(r, "Deployment/team-a/web"));
    ASSERT_NOT_NULL(strstr(r, "Deployment/team-b/api"));
    ASSERT_NULL(strstr(r, "Service/team-a/web"));
    free(r);
    r = gpf_tool_on(k_two_teams, 2, "search_graph",
                    "{\"project\":\"%s\",\"label\":\"Resource\",\"namespace\":\"team-a\","
                    "\"format\":\"json\"}");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(strstr(r, "Deployment/team-a/web"));
    ASSERT_NOT_NULL(strstr(r, "Service/team-a/web"));
    ASSERT_NULL(strstr(r, "Deployment/team-b/api"));
    free(r);
    r = gpf_tool_on(k_two_teams, 2, "search_graph",
                    "{\"project\":\"%s\",\"query\":\"web\",\"kind\":\"Service\","
                    "\"format\":\"json\"}");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(strstr(r, "Service/team-a/web"));
    ASSERT_NULL(strstr(r, "Deployment/team-a/web"));
    free(r);
    PASS();
}

/* trace_path mode=topology walks infra edges: from a Service out to its
 * workload and the ConfigMap the workload mounts, and in to the exposed
 * route and the Ingress that declares it. The call graph modes do not. */
TEST(probe_mcp_trace_topology) {
    static const char doc[] =
        "apiVersion: networking.k8s.io/v1\nkind: Ingress\nmetadata:\n  name: shop\nspec:\n"
        "  rules:\n    - host: shop.example.com\n      http:\n        paths:\n"
        "          - path: /api\n            pathType: Prefix\n            backend:\n"
        "              service:\n                name: api\n                port:\n"
        "                  number: 80\n---\n"
        "apiVersion: v1\nkind: Service\nmetadata:\n  name: api\nspec:\n  selector:\n"
        "    app: api\n---\n"
        "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: api\nspec:\n  template:\n"
        "    metadata:\n      labels:\n        app: api\n    spec:\n      volumes:\n"
        "        - name: cfg\n          configMap:\n            name: api-config\n---\n"
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: api-config\n";
    static const GpfFile files[] = {{"k8s/shop.yaml", doc}};
    /* The tool traverses the same exact edge verified below. Its apply context
     * is explicit; all original route, workload and mode assertions remain. */
    const GpfFile configured[] = {files[0],
                                  {"k8s/kustomization.yaml", "resources:\n  - shop.yaml\n"}};
    const RQ_Expected expected[] = {
        {.case_id = "topology-exact-mounted-configmap",
         .site = {.file = "k8s/shop.yaml",
                  .caller = "k8s.shop.yaml::apps/Deployment/-/api",
                  .start_byte = 610,
                  .end_byte = 620},
         .relation = "USAGE",
         .target = {.file = "k8s/shop.yaml",
                    .suffix = "k8s.shop.yaml::core/ConfigMap/-/api-config",
                    .label = "Resource",
                    .declaration_line = 44,
                    .declaration_text = "  name: api-config"},
         .mode = RQ_REQUIRED}};
    ASSERT_TRUE(gpf_infra_quality(configured, 2, "probe_mcp_trace_topology/configured", expected, 1,
                                  NULL, 0));
    char *r = gpf_tool_on(configured, 2, "trace_path",
                          "{\"function_name\":\"Service/api\",\"project\":\"%s\","
                          "\"direction\":\"outbound\",\"mode\":\"topology\",\"format\":\"json\"}");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(strstr(r, "Deployment/-/api"));
    ASSERT_NOT_NULL(strstr(r, "ConfigMap/-/api-config"));
    free(r);
    r = gpf_tool_on(configured, 2, "trace_path",
                    "{\"function_name\":\"Service/api\",\"project\":\"%s\","
                    "\"direction\":\"inbound\",\"mode\":\"topology\",\"format\":\"json\"}");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(strstr(r, "__route__ingress__shop.example")); /* qn groups split at a dot */
    ASSERT_NOT_NULL(strstr(r, "Ingress/-/shop"));
    free(r);
    r = gpf_tool_on(configured, 2, "trace_path",
                    "{\"function_name\":\"Service/api\",\"project\":\"%s\","
                    "\"direction\":\"outbound\",\"format\":\"json\"}");
    ASSERT_NOT_NULL(r);
    ASSERT_NULL(strstr(r, "Deployment/-/api"));
    free(r);
    PASS();
}

/* trace_path mode=topology also follows DEPLOYS: from a Flux Kustomization
 * out to the kustomization its path applies. */
TEST(probe_mcp_trace_topology_follows_deploys) {
    static const GpfFile files[] = {
        {"clusters/sync.yaml",
         "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata:\n"
         "  name: sync\n  namespace: flux-system\nspec:\n  path: ./apps\n"},
        {"apps/kustomization.yaml",
         "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n"
         "resources:\n  - cm.yaml\n"},
        {"apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings\n"}};
    char *r = gpf_tool_on(files, 3, "trace_path",
                          "{\"function_name\":\"Kustomization/sync\",\"project\":\"%s\","
                          "\"direction\":\"outbound\",\"mode\":\"topology\",\"format\":\"json\"}");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(strstr(r, ".apps.kustomization\"")); /* the kustomization's Resource */
    free(r);
    PASS();
}

/* trace_path mode=run walks what a Service runs: out to its workload, the
 * Image the workload runs, the Dockerfile that builds the image, the Go main
 * its ENTRYPOINT starts -- and the environment the container is given.
 * mode=topology stops before the program. */
TEST(probe_mcp_trace_run) {
    static const GpfFile files[] = {
        {"go.mod", "module example.com/app\n\ngo 1.22\n"},
        {"cmd/server/main.go", "package main\n\nfunc main() {}\n"},
        {"svc/Dockerfile", "FROM golang:1.22 AS build\nWORKDIR /src\nCOPY . .\n"
                           "RUN CGO_ENABLED=0 go build -o /out/server ./cmd/server\n"
                           "FROM gcr.io/distroless/static\nCOPY --from=build /out/server /server\n"
                           "ENTRYPOINT [\"/server\"]\n"},
        {"skaffold.yaml", "apiVersion: skaffold/v4beta6\nkind: Config\nbuild:\n  artifacts:\n"
                          "    - image: acme/server\n      context: .\n      docker:\n"
                          "        dockerfile: svc/Dockerfile\n"},
        {"k8s/app.yaml",
         "apiVersion: v1\nkind: Service\nmetadata:\n  name: server\nspec:\n  selector:\n"
         "    app: server\n---\n"
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: server\nspec:\n"
         "  template:\n    metadata:\n      labels:\n        app: server\n    spec:\n"
         "      containers:\n        - name: server\n          image: acme/server:1.0\n"
         "          env:\n            - name: LISTEN_PORT\n              value: \"8080\"\n"}};
    char *r = gpf_tool_on(files, 5, "trace_path",
                          "{\"function_name\":\"Service/server\",\"project\":\"%s\","
                          "\"direction\":\"outbound\",\"mode\":\"run\",\"depth\":5,"
                          "\"format\":\"json\"}");
    ASSERT_NOT_NULL(r);
    /* qualified names are grouped by prefix, split at a dot */
    ASSERT_NOT_NULL(strstr(r, "Deployment/-/server"));
    ASSERT_NOT_NULL(strstr(r, "[\"__image__acme/server\""));         /* dotless: the whole QN */
    ASSERT_NOT_NULL(strstr(r, ".svc\",\"rows\":[[\"Dockerfile\""));  /* its Module */
    ASSERT_NOT_NULL(strstr(r, ".cmd.server\",\"rows\":[[\"main\"")); /* what it starts */
    ASSERT_NOT_NULL(strstr(r, "Deployment/-/server#env/server/LISTEN_PORT"));
    free(r);
    r = gpf_tool_on(files, 5, "trace_path",
                    "{\"function_name\":\"Service/server\",\"project\":\"%s\","
                    "\"direction\":\"outbound\",\"mode\":\"topology\",\"depth\":5,"
                    "\"format\":\"json\"}");
    ASSERT_NOT_NULL(r);
    ASSERT_NULL(strstr(r, ".cmd.server\",\"rows\":[[\"main\""));
    free(r);
    PASS();
}

/* Deploy traversal follows retained DEPLOYS/SPAWNS edges and exact CALLS;
 * call-graph mode still stops at the chart. */
static int gpf_query_int(const GpfFile *files, int nfiles, const char *sql);

TEST(probe_mcp_trace_deploy) {
    static const GpfFile files[] = {
        {"charts/web/Chart.yaml", "apiVersion: v2\nname: web-app\nversion: 0.1.0\n"},
        {"hack/deploy.sh", "#!/bin/sh\n"
                           "cd \"$(dirname \"$0\")/..\"\n"
                           "install_web() {\n"
                           "  helm upgrade --install web charts/web\n"
                           "}\n"
                           "install_web\n"},
        {".github/workflows/release.yml", "on: [push]\n"
                                          "jobs:\n"
                                          "  ship:\n"
                                          "    runs-on: ubuntu-latest\n"
                                          "    steps:\n"
                                          "      - run: ./hack/deploy.sh\n"}};
    char *r = gpf_tool_on(files, 3, "trace_path",
                          "{\"function_name\":\"web-app\",\"project\":\"%s\","
                          "\"direction\":\"inbound\",\"mode\":\"deploy\",\"depth\":4,"
                          "\"format\":\"json\"}");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(strstr(r, "install_web"));
    /* Deploy mode reaches the installing function through DEPLOYS, the script through its own
     * proven call of that function, and the CI job through SPAWNS. */
    ASSERT_NOT_NULL(strstr(r, ".hack\""));
    ASSERT_NOT_NULL(strstr(r, "\"release\""));
    free(r);
    r = gpf_tool_on(files, 3, "trace_path",
                    "{\"function_name\":\"web-app\",\"project\":\"%s\","
                    "\"direction\":\"inbound\",\"format\":\"json\"}");
    ASSERT_NOT_NULL(r);
    ASSERT_NULL(strstr(r, "install_web"));
    free(r);
    /* Preserve the two direct non-controlled relations independently of the
     * shell CALLS edge and the consumer's bounded traversal. */
    ASSERT_EQ(
        gpf_query_int(
            files, 3,
            "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
            "JOIN nodes t ON t.id=e.target_id WHERE e.project=?1 AND ("
            "(e.type='DEPLOYS' AND s.label='Function' AND s.name='install_web' "
            "AND s.file_path='hack/deploy.sh' AND t.label='Chart' AND t.name='web-app' "
            "AND t.file_path='charts/web/Chart.yaml' AND "
            "json_extract(e.properties,'$.via')='helm') OR "
            "(e.type='SPAWNS' AND s.label='Module' AND s.file_path='.github/workflows/release.yml' "
            "AND t.label='Module' AND t.file_path='hack/deploy.sh' "
            "AND json_extract(e.properties,'$.api')='shell' "
            "AND json_extract(e.properties,'$.ci')='github-actions' "
            "AND json_extract(e.properties,'$.job')='ship'))"),
        2);
    const RQ_Expected expected[] = {
        {.case_id = "deploy-script-invokes-install-web",
         .site =
             {.file = "hack/deploy.sh", .caller = "hack.deploy", .start_byte = 92, .end_byte = 103},
         .relation = "CALLS",
         .target = {.file = "hack/deploy.sh",
                    .suffix = "hack.deploy.install_web",
                    .label = "Function",
                    .declaration_line = 3,
                    .declaration_text = "install_web() {"},
         .mode = RQ_REQUIRED}};
    ASSERT_TRUE(gpf_infra_quality(files, 3, "probe_mcp_trace_deploy", expected, 1, NULL, 0));
    PASS();
}

/* ── DEPLOYS (Phase 3) ──────────────────────────────────────────────── */

/* A Flux Kustomization deploys what its path applies: the outermost
 * kustomization below it (its Kustomization node) and a manifest outside any
 * (its File); a HelmRelease its chart -- the repository chart of a Git
 * source, else the external chart of that name; an Argo Application whose
 * path is a chart dir, that Chart. */
TEST(probe_gitops_objects_deploy_their_units) {
    static const GpfFile files[] = {
        {"clusters/prod/flux-system/kustomization.yaml",
         "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n"
         "resources:\n  - gotk-sync.yaml\n"},
        {"clusters/prod/flux-system/gotk-sync.yaml",
         "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\n"
         "metadata:\n  name: flux-system\n  namespace: flux-system\n"
         "spec:\n  path: ./clusters/prod\n"},
        {"clusters/prod/apps.yaml",
         "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\n"
         "metadata:\n  name: apps\n  namespace: flux-system\nspec:\n  path: ./apps\n"},
        {"apps/kustomization.yaml",
         "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n"
         "resources:\n  - release.yaml\n"},
        {"apps/release.yaml",
         "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata:\n  name: web\n"
         "spec:\n  chart:\n    spec:\n      chart: ./charts/web\n      sourceRef:\n"
         "        kind: GitRepository\n        name: flux-system\n---\n"
         "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata:\n  name: redis\n"
         "spec:\n  chart:\n    spec:\n      chart: redis\n      sourceRef:\n"
         "        kind: HelmRepository\n        name: bitnami\n"},
        {"charts/web/Chart.yaml", "apiVersion: v2\nname: web\nversion: 0.1.0\n"},
        {"argo/web.yaml",
         "apiVersion: argoproj.io/v1alpha1\nkind: Application\nmetadata:\n  name: web\n"
         "spec:\n  source:\n    path: charts/web\n"}};
    ASSERT_EQ(gpf_typed_edges(files, 7, "DEPLOYS", "Resource", "Resource", "\"via\":\"flux\""), 2);
    ASSERT_EQ(gpf_typed_edges(files, 7, "DEPLOYS", "Resource", "File", "\"via\":\"flux\""), 1);
    ASSERT_EQ(gpf_typed_edges(files, 7, "DEPLOYS", "Resource", "Chart", "\"via\":\"helmrelease\""),
              2);
    ASSERT_EQ(gpf_typed_edges(files, 7, "DEPLOYS", "Resource", "Chart", "\"via\":\"argo\""), 1);
    PASS();
}

/* Every DEPLOYS edge as "source -> target (via)", sorted, one per line: a
 * function by name, top-level code as "@<file>"; a File or Kustomization by
 * its path, a Chart by name ("(ext)": the external one). NULL on failure;
 * the caller frees. */
static char *gpf_deploys(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce(group_concat(x, char(10)), '') FROM (SELECT "
            "CASE WHEN s.label IN ('Module','File') THEN '@' || s.file_path ELSE s.name END "
            "|| ' -> ' || CASE WHEN t.label='Chart' THEN 'chart:' || t.name || "
            "CASE WHEN json_extract(t.properties,'$.external') THEN '(ext)' ELSE '' END "
            "ELSE t.label || ':' || t.file_path END "
            "|| ' (' || json_extract(e.properties,'$.via') || ')' AS x "
            "FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id "
            "WHERE e.project=?1 AND e.type='DEPLOYS' ORDER BY x)";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

#define GPF_ASSERT_DEPLOYS(files, n, want)                                                  \
    do {                                                                                    \
        char *got_ = gpf_deploys(files, n);                                                 \
        if (!got_ || strcmp(got_, want) != 0) {                                             \
            fprintf(stderr, "DEPLOYS got:\n%s\nwant:\n%s\n", got_ ? got_ : "(null)", want); \
            free(got_);                                                                     \
            FAIL("DEPLOYS edge set");                                                       \
        }                                                                                   \
        free(got_);                                                                         \
    } while (0)

/* Every SPAWNS edge a script or recipe draws (api=shell) as "source ->
 * program", sorted, one per line; top-level code as "@<file>". */
static char *gpf_spawns(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce(group_concat(x, char(10)), '') FROM (SELECT DISTINCT "
            "CASE WHEN s.label IN ('Module','File') THEN '@' || s.file_path ELSE s.name END "
            "|| ' -> ' || t.name AS x "
            "FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id "
            "WHERE e.project=?1 AND e.type='SPAWNS' AND t.label='Process' "
            "AND json_extract(e.properties,'$.api')='shell' ORDER BY x)";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

#define GPF_ASSERT_SPAWNS(files, n, want)                                                  \
    do {                                                                                   \
        char *got_ = gpf_spawns(files, n);                                                 \
        if (!got_ || strcmp(got_, want) != 0) {                                            \
            fprintf(stderr, "SPAWNS got:\n%s\nwant:\n%s\n", got_ ? got_ : "(null)", want); \
            free(got_);                                                                    \
            FAIL("SPAWNS edge set");                                                       \
        }                                                                                  \
        free(got_);                                                                        \
    } while (0)

/* Every SPAWNS and DEPLOYS edge as "source -> target (TYPE api|via[
 * ci[/job]])", sorted, one per line: top-level code as "@<file>"; a script
 * as "script:<path>", a Process as "proc:<name>", a Chart as "chart:<name>",
 * a Make rule as "rule:<file>:<target>". */
static char *gpf_runs(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce(group_concat(x, char(10)), '') FROM (SELECT DISTINCT "
            "CASE WHEN s.label IN ('Module','File') THEN '@' || s.file_path ELSE s.name END "
            "|| ' -> ' || CASE WHEN t.label IN ('Module','File') THEN 'script:' || t.file_path "
            "WHEN t.label='Process' THEN 'proc:' || t.name WHEN t.label='Chart' THEN "
            "'chart:' || t.name WHEN t.label='Function' THEN 'rule:' || t.file_path || ':' || "
            "t.name ELSE t.label || ':' || t.file_path END "
            "|| ' (' || e.type || ' ' || coalesce(json_extract(e.properties,'$.api'), "
            "json_extract(e.properties,'$.via')) || coalesce(' ' || "
            "json_extract(e.properties,'$.ci') || coalesce('/' || "
            "json_extract(e.properties,'$.job'), ''), '') || ')' AS x "
            "FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id "
            "WHERE e.project=?1 AND e.type IN ('SPAWNS','DEPLOYS') ORDER BY x)";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

#define GPF_ASSERT_RUNS(files, n, want)                                                  \
    do {                                                                                 \
        char *got_ = gpf_runs(files, n);                                                 \
        if (!got_ || strcmp(got_, want) != 0) {                                          \
            fprintf(stderr, "runs got:\n%s\nwant:\n%s\n", got_ ? got_ : "(null)", want); \
            free(got_);                                                                  \
            FAIL("SPAWNS / DEPLOYS edge set");                                           \
        }                                                                                \
        free(got_);                                                                      \
    } while (0)

/* Every DEPENDS_ON edge into an Image as "@<file> -> <repository> (via)",
 * sorted, one per line. */
static char *gpf_images(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce(group_concat(x, char(10)), '') FROM (SELECT '@' || s.file_path "
            "|| ' -> ' || t.name || ' (' || json_extract(e.properties,'$.via') || ')' AS x "
            "FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id "
            "WHERE e.project=?1 AND e.type='DEPENDS_ON' AND t.label='Image' ORDER BY x)";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

#define GPF_ASSERT_IMAGES(files, n, want)                                                  \
    do {                                                                                   \
        char *got_ = gpf_images(files, n);                                                 \
        if (!got_ || strcmp(got_, want) != 0) {                                            \
            fprintf(stderr, "images got:\n%s\nwant:\n%s\n", got_ ? got_ : "(null)", want); \
            free(got_);                                                                    \
            FAIL("Image DEPENDS_ON edge set");                                             \
        }                                                                                  \
        free(got_);                                                                        \
    } while (0)

/* A Dockerfile DEPENDS_ON each image it builds on: FROM and COPY --from,
 * with ARG defaults expanded, under the image's repository (as written: a
 * registry host kept, none invented; tag and digest dropped). A stage
 * (by name or index), scratch and an ARG without a value are no image. */
TEST(probe_dockerfile_bases_are_images) {
    static const GpfFile files[] = {
        {"svc/Dockerfile", "ARG GO=1.22\nARG BASE\n"
                           "FROM golang:${GO} AS build\n"
                           "RUN go build -o /out/app ./cmd/app\n"
                           "FROM ${BASE} AS unknown\n"
                           "FROM build AS test\n"
                           "FROM gcr.io/distroless/static@sha256:0123 AS run\n"
                           "COPY --from=build /out/app /app\n"
                           "COPY --from=0 /out/app /app2\n"
                           "COPY --from=docker.io/bitnami/kubectl:1.30 /opt/kubectl /kubectl\n"
                           "FROM scratch\n"
                           "COPY --from=registry.example.com:5000/team/tools:2 /bin/x /x\n"}};
    GPF_ASSERT_IMAGES(files, 1,
                      "@svc/Dockerfile -> docker.io/bitnami/kubectl (copy-from)\n"
                      "@svc/Dockerfile -> gcr.io/distroless/static (from)\n"
                      "@svc/Dockerfile -> golang (from)\n"
                      "@svc/Dockerfile -> registry.example.com:5000/team/tools (copy-from)");
    PASS();
}

/* Every Image node as "<name> [<registry>]", sorted, one per line. */
static char *gpf_image_nodes(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce(group_concat(x, char(10)), '') FROM (SELECT name || ' [' "
            "|| coalesce(json_extract(properties,'$.registry'), '-') || ']' AS x "
            "FROM nodes WHERE project=?1 AND label='Image' ORDER BY x)";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* An image reference that names no registry host keeps it unknown: the Image
 * is the repository as written, registry "unknown" -- never docker.io, which
 * is only Docker's default (a short-name search list or a local-only build
 * tag resolves elsewhere or nowhere). A stated host is kept, its Docker Hub
 * spellings as docker.io with library/ for an official one-segment name.
 * Every source alike: FROM, a K8s container, a compose service, a build tag. */
TEST(probe_image_without_registry_host_stays_unknown) {
    static const GpfFile files[] = {
        {"svc/Dockerfile", "FROM nginx:1.27\nFROM acme/base:2 AS b\nFROM docker.io/redis:7 AS c\n"
                           "FROM index.docker.io/acme/tool AS d\n"
                           "FROM ghcr.io/acme/app@sha256:0123 AS e\n"
                           "FROM localhost:5000/team/x AS f\n"},
        {"app.yaml", "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\nspec:\n"
                     "  template:\n    spec:\n      containers:\n        - name: web\n"
                     "          image: busybox:1.36\n        - name: side\n"
                     "          image: quay.io/acme/sidecar:1\n"},
        {"compose.yaml", "services:\n  web:\n    image: acme/web:1\n"},
        {"build.sh", "#!/bin/sh\ncd \"$(dirname \"$0\")\"\ndocker build -t myapp:dev svc\n"}};
    char *got = gpf_image_nodes(files, 4);
    static const char want[] = "acme/base [unknown]\n"
                               "acme/web [unknown]\n"
                               "busybox [unknown]\n"
                               "docker.io/acme/tool [docker.io]\n"
                               "docker.io/library/redis [docker.io]\n"
                               "ghcr.io/acme/app [ghcr.io]\n"
                               "localhost:5000/team/x [localhost:5000]\n"
                               "myapp [unknown]\n"
                               "nginx [unknown]\n"
                               "quay.io/acme/sidecar [quay.io]";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "image nodes got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("Image nodes and registries");
    }
    free(got);
    PASS();
}

/* A build file named for its variant is a Dockerfile of its own (its own
 * Module, beside the plain Dockerfile): Dockerfile.debug, Dockerfile-test,
 * Containerfile, pipeline.Dockerfile. A template of one (Dockerfile.j2) and a
 * BuildKit ignore file (Dockerfile.dockerignore) are not. */
TEST(probe_dockerfile_variants_are_dockerfiles) {
    static const GpfFile files[] = {{"svc/Dockerfile", "FROM alpine:3.20\n"},
                                    {"svc/Dockerfile.debug", "FROM debian:12\n"},
                                    {"svc/Dockerfile-test", "FROM ubuntu:24.04\n"},
                                    {"svc/Dockerfile.dockerignore", "FROM busybox\n"},
                                    {"Containerfile", "FROM quay.io/podman/stable\n"},
                                    {"tools/pipeline.Dockerfile", "FROM node:20\n"},
                                    {"tools/Dockerfile.j2", "FROM {{ base_image }}\n"}};
    GPF_ASSERT_IMAGES(files, 7,
                      "@Containerfile -> quay.io/podman/stable (from)\n"
                      "@svc/Dockerfile -> alpine (from)\n"
                      "@svc/Dockerfile-test -> ubuntu (from)\n"
                      "@svc/Dockerfile.debug -> debian (from)\n"
                      "@tools/pipeline.Dockerfile -> node (from)");
    PASS();
}

/* A Dockerfile SPAWNS what its last stage runs: the Go main of a binary a
 * stage built and the image copied in, an interpreter script or a file
 * copied in (also a CMD file under an inherited ENTRYPOINT), else the
 * program's Process. sh -c and arguments-only CMDs are no edge. */
TEST(probe_dockerfile_entrypoint_runs) {
    static const GpfFile files[] = {
        {"go.mod", "module example.com/app\n\ngo 1.22\n"},
        {"cmd/server/main.go", "package main\n\nfunc main() {}\n"},
        {"svc/Dockerfile", "FROM golang:1.22 AS build\nWORKDIR /src\nCOPY . .\n"
                           "RUN CGO_ENABLED=0 go build -o /out/server ./cmd/server\n"
                           "FROM gcr.io/distroless/static\nCOPY --from=build /out/server /server\n"
                           "ENTRYPOINT [\"/server\", \"--port\", \"8080\"]\n"},
        {"py/app.py", "print('x')\n"},
        {"py/Dockerfile",
         "FROM python:3.12\nWORKDIR /app\nCOPY . .\nCMD [\"python\", \"app.py\"]\n"},
        {"node/index.js", "console.log(1);\n"},
        {"node/Dockerfile",
         "FROM gcr.io/distroless/nodejs22\nWORKDIR /app\nCOPY index.js .\nCMD [\"index.js\"]\n"},
        {"tool/run.sh", "#!/bin/sh\necho run\n"},
        {"tool/Dockerfile", "FROM alpine:3.20\nCOPY run.sh /usr/local/bin/run.sh\n"
                            "ENTRYPOINT [\"/usr/local/bin/run.sh\"]\n"},
        {"web/Dockerfile", "FROM nginx:1.27\nCMD [\"nginx\", \"-g\", \"daemon off;\"]\n"},
        {"sh/Dockerfile", "FROM alpine:3.20\nCMD sh -c \"echo hi\"\n"},
        {"args/Dockerfile", "FROM gcr.io/distroless/python3\nCMD [\"--flag\"]\n"}};
    GPF_ASSERT_RUNS(files, 12,
                    "@node/Dockerfile -> script:node/index.js (SPAWNS dockerfile)\n"
                    "@py/Dockerfile -> script:py/app.py (SPAWNS dockerfile)\n"
                    "@svc/Dockerfile -> rule:cmd/server/main.go:main (SPAWNS dockerfile)\n"
                    "@tool/Dockerfile -> script:tool/run.sh (SPAWNS dockerfile)\n"
                    "@web/Dockerfile -> proc:nginx (SPAWNS dockerfile)");
    PASS();
}

/* Builtin workload image fields use the exact literal repository Image.
 * A malformed digest, substitution, placeholder or arbitrary custom-resource
 * field provides no such proof. A valid digest has a separate required control. */
static bool gpf_builtin_images_quality(const GpfFile *original);

TEST(probe_k8s_container_images_are_images) {
    static const GpfFile files[] = {
        {"app.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\nspec:\n  template:\n"
         "    spec:\n      initContainers:\n        - name: init\n"
         "          image: busybox@sha256:0123\n      containers:\n        - name: web\n"
         "          image: nginx:1.27\n        - name: side\n          image: $(SIDE_IMAGE)\n"
         "---\napiVersion: batch/v1\nkind: CronJob\nmetadata:\n  name: nightly\nspec:\n"
         "  jobTemplate:\n    spec:\n      template:\n        spec:\n          containers:\n"
         "            - name: job\n              image: ghcr.io/acme/job:2\n"
         "---\napiVersion: example.com/v1\nkind: Widget\nmetadata:\n  name: w\nspec:\n"
         "  template:\n    spec:\n      containers:\n        - name: w\n"
         "          image: quay.io/acme/widget:3\n        - name: placeholder\n"
         "          image: <image-built-from-dockerfile>\n"}};
    ASSERT_TRUE(gpf_builtin_images_quality(files));
    PASS();
}

/* Every DEPENDS_ON {via:build} edge as "<image> -> <dockerfile> (tool)". */
static char *gpf_builds(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce(group_concat(x, char(10)), '') FROM (SELECT s.name || ' -> ' || "
            "t.file_path || ' (' || json_extract(e.properties,'$.tool') || ')' AS x FROM edges e "
            "JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id WHERE "
            "e.project=?1 AND e.type='DEPENDS_ON' AND s.label='Image' AND "
            "json_extract(e.properties,'$.via')='build' ORDER BY x)";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* An image a build configuration builds DEPENDS_ON {via:build} its
 * Dockerfile: skaffold artifacts (top level and profiles; docker.dockerfile
 * relative to the context; a ko artifact is none), compose services with
 * build and image, variables from the project's .env. A service without an
 * image or a build, or a build without a Dockerfile, is none. */
TEST(probe_build_configs_build_images_from_dockerfiles) {
    static const GpfFile files[] = {
        {"src/web/Dockerfile", "FROM nginx:1.27\n"},
        {"src/api/Dockerfile.prod", "FROM golang:1.22\n"},
        {"skaffold.yaml", "apiVersion: skaffold/v4beta6\nkind: Config\nbuild:\n  artifacts:\n"
                          "    - image: web\n      context: src/web\n"
                          "    - image: api\n      context: src/api\n      docker:\n"
                          "        dockerfile: Dockerfile.prod\n"
                          "    - image: koapp\n      ko:\n        main: ./cmd\n"
                          "profiles:\n  - name: dev\n    build:\n      artifacts:\n"
                          "        - image: dev-web\n          context: src/web\n"},
        {".env", "REG=ghcr.io/acme\n# comment\nexport UNUSED='x'\n"},
        {"compose.yaml", "services:\n  frontend:\n    image: ${REG}/frontend:${TAG:-1}\n"
                         "    build:\n      context: ./src/web\n"
                         "  worker:\n    build: ./src/api\n"
                         "  db:\n    image: postgres:16\n"
                         "  missing:\n    image: acme/missing\n    build: ./nope\n"
                         "  unknown:\n    image: ${NOPE}/x\n    build: ./src/web\n"}};
    char *got = gpf_builds(files, 5);
    static const char want[] = "api -> src/api/Dockerfile.prod (skaffold)\n"
                               "dev-web -> src/web/Dockerfile (skaffold)\n"
                               "ghcr.io/acme/frontend -> src/web/Dockerfile (compose)\n"
                               "web -> src/web/Dockerfile (skaffold)";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "builds got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("DEPENDS_ON {via:build} edge set");
    }
    free(got);
    PASS();
}

/* Every edge from a compose service as "<service> -> <target> (TYPE
 * via|ref)", sorted: an Image by repository, a Module by path. */
static char *gpf_compose(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce(group_concat(x, char(10)), '') FROM (SELECT s.name || ' -> ' || "
            "CASE t.label WHEN 'Image' THEN 'Image:' || t.name WHEN 'Module' THEN 'Module:' || "
            "t.file_path ELSE t.name END || ' (' || e.type || ' ' || "
            "coalesce(json_extract(e.properties,'$.via'), json_extract(e.properties,'$.ref')) || "
            "')' AS x FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
            "t.id=e.target_id WHERE e.project=?1 AND s.label='Resource' AND "
            "json_extract(s.properties,'$.ecosystem')='compose' ORDER BY x)";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* A compose service is a Resource {ecosystem:compose}: it DEPENDS_ON the
 * services its depends_on (a list or a mapping) and links name, USAGE
 * {ref:image} its image and DEPENDS_ON {via:build} the Dockerfile its build
 * names (a context, or context + dockerfile). */
TEST(probe_compose_services_are_resources) {
    static const GpfFile files[] = {
        {"web/Dockerfile", "FROM nginx:1.27\n"},
        {"worker/Dockerfile", "FROM python:3.12\n"},
        {"compose.yaml",
         "services:\n  web:\n    image: acme/web:1\n    build: ./web\n"
         "    depends_on:\n      - db\n      - cache\n    links:\n      - cache:c\n"
         "  db:\n    image: postgres:16\n"
         "  cache:\n    image: redis:7\n    depends_on:\n      db:\n"
         "        condition: service_healthy\n"
         "  worker:\n    build:\n      context: .\n      dockerfile: worker/Dockerfile\n"}};
    char *got = gpf_compose(files, 3);
    static const char want[] = "Service/cache -> Image:redis (USAGE image)\n"
                               "Service/cache -> Service/db (DEPENDS_ON depends_on)\n"
                               "Service/db -> Image:postgres (USAGE image)\n"
                               "Service/web -> Image:acme/web (USAGE image)\n"
                               "Service/web -> Module:web/Dockerfile (DEPENDS_ON build)\n"
                               "Service/web -> Service/cache (DEPENDS_ON depends_on)\n"
                               "Service/web -> Service/db (DEPENDS_ON depends_on)\n"
                               "Service/worker -> Module:worker/Dockerfile (DEPENDS_ON build)";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "compose got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("compose service edge set");
    }
    free(got);
    PASS();
}

/* A container build command (docker / podman [buildx] build, in a script or
 * a Make recipe) builds each tagged image from the Dockerfile -f names
 * (against the cwd), else <context>/Dockerfile. A tag of unknown value keeps
 * its repository ($(TAG), $VERSION; a registry port is no tag); an unknown
 * context, or another docker command, is no build. */
TEST(probe_shell_docker_builds_build_images) {
    static const GpfFile files[] = {
        {"Dockerfile", "FROM alpine:3.20\n"},
        {"svc/Dockerfile", "FROM golang:1.22\n"},
        {"Makefile", "DOCKER_REPO ?= quay.io/acme\nTAG ?= $(shell git describe --tags)\n\n"
                     "image:\n\tdocker build --label a=b -t $(DOCKER_REPO)/app:$(TAG) .\n"},
        {"build.sh",
         "#!/bin/sh\ncd \"$(dirname \"$0\")\"\n"
         "docker buildx build --platform linux/amd64 --tag ghcr.io/acme/svc:1 -f svc/Dockerfile .\n"
         "podman build -t \"localhost:5000/team/x:$VERSION\" svc\n"
         "docker build -t unknown-ctx $CTX\ndocker run -t nope .\n"}};
    char *got = gpf_builds(files, 4);
    static const char want[] = "ghcr.io/acme/svc -> svc/Dockerfile (docker-build)\n"
                               "localhost:5000/team/x -> svc/Dockerfile (docker-build)\n"
                               "quay.io/acme/app -> Dockerfile (docker-build)";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "builds got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("docker build DEPENDS_ON {via:build} edge set");
    }
    free(got);
    PASS();
}

/* The integer `sql` (?1 = the project) yields on the indexed files; -1 on
 * failure. */
static int gpf_query_int(const GpfFile *files, int nfiles, const char *sql) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    int out = -1;
    if (store) {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = sqlite3_column_int(st, 0);
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* Build configurations can retain component-pattern build nodes. Runtime
 * references still name their canonical repository: tag similarity supplies
 * no evidence that two versions came from the same component build. */
static bool gpf_component_images_quality(const GpfFile *files, size_t nfiles);

TEST(probe_image_hub_splits_by_component) {
    static const GpfFile files[] = {
        {"svc/a/Dockerfile", "FROM alpine:3.20\n"},
        {"svc/b/Dockerfile", "FROM alpine:3.20\n"},
        {"svc/c/Dockerfile", "FROM alpine:3.20\n"},
        {"solo/Dockerfile", "FROM alpine:3.20\n"},
        {"tool/Dockerfile", "FROM alpine:3.20\n"},
        {"tool/Dockerfile.test", "FROM alpine:3.20\n"},
        {"w1/Dockerfile", "FROM alpine:3.20\n"},
        {"w2/Dockerfile.alt", "FROM alpine:3.20\n"},
        {"v/Dockerfile", "FROM alpine:3.20\n"},
        {"p/Dockerfile", "FROM alpine:3.20\n"},
        {"q/Dockerfile", "FROM alpine:3.20\n"},
        {".env", "V=1.0\n"},
        {"compose.yaml",
         "services:\n  a:\n    image: acme/demo:${V}-a\n    build: ./svc/a\n"
         "  b:\n    image: acme/demo:${V}-b\n    build: ./svc/b\n"
         "  c:\n    image: acme/demo:${V}-c\n    build: ./svc/c\n"
         "  solo:\n    image: acme/solo:1\n    build: ./solo\n"
         "  tool:\n    image: acme/tool\n    build: ./tool\n"
         "  tooltest:\n    image: acme/tool:test\n    build:\n      context: ./tool\n"
         "      dockerfile: Dockerfile.test\n"
         "  w1:\n    image: acme/x:1-w\n    build: ./w1\n"
         "  w2:\n    image: acme/x:1-w\n    build:\n      context: ./w2\n"
         "      dockerfile: Dockerfile.alt\n"
         "  v:\n    image: acme/x:1-v\n    build: ./v\n"
         "  p:\n    image: acme/y:1-p\n    build: ./p\n"
         "  q:\n    image: acme/y:1-q\n    build: ./q\n"},
        {"k8s.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: b\nspec:\n  template:\n"
         "    spec:\n      containers:\n        - name: b\n          image: acme/demo:0.9-b\n"
         "---\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: z\nspec:\n"
         "  template:\n    spec:\n      containers:\n        - name: z\n"
         "          image: acme/demo:0.9-zzz\n"
         "---\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: s\nspec:\n"
         "  template:\n    spec:\n      containers:\n        - name: s\n"
         "          image: acme/solo:2\n"}};
    enum { NF = sizeof(files) / sizeof(files[0]) };
    char *got = gpf_builds(files, NF);
    static const char want_builds[] = "acme/demo:*-a -> svc/a/Dockerfile (compose)\n"
                                      "acme/demo:*-b -> svc/b/Dockerfile (compose)\n"
                                      "acme/demo:*-c -> svc/c/Dockerfile (compose)\n"
                                      "acme/solo -> solo/Dockerfile (compose)\n"
                                      "acme/tool -> tool/Dockerfile (compose)\n"
                                      "acme/tool:test -> tool/Dockerfile.test (compose)\n"
                                      "acme/x -> w1/Dockerfile (compose)\n"
                                      "acme/x -> w2/Dockerfile.alt (compose)\n"
                                      "acme/x:*-v -> v/Dockerfile (compose)\n"
                                      "acme/y:*-p -> p/Dockerfile (compose)\n"
                                      "acme/y:*-q -> q/Dockerfile (compose)";
    if (!got || strcmp(got, want_builds) != 0) {
        fprintf(stderr, "builds got:\n%s\nwant:\n%s\n", got ? got : "(null)", want_builds);
        free(got);
        FAIL("component Images' build edges");
    }
    free(got);
    /* Tag suffixes do not prove build provenance. Every effective runtime
     * scalar must retain its exact canonical repository, including Compose inputs. */
    ASSERT_TRUE(gpf_component_images_quality(files, NF));
    /* Every retained repository or build-pattern Image still has an edge. */
    int orphans = gpf_query_int(files, NF,
                                "SELECT COUNT(*) FROM nodes n WHERE n.project=?1 AND "
                                "n.label='Image' AND NOT EXISTS (SELECT 1 FROM edges e WHERE "
                                "e.source_id=n.id OR e.target_id=n.id)");
    if (orphans != 0) {
        fprintf(stderr, "Images without an edge: %d\n", orphans);
        FAIL("a repository Image left without edges");
    }
    PASS();
}

/* Every CONFIGURES {strategy:env_set} edge as
 * "<owner> -> <container | ->/<name> (<source>)"; then the joins a scoped
 * EnvVar must not have: "#PORT=<EnvVar nodes named PORT>" and
 * "#joined=<edges out of an EnvVar, or env_set edges into a global one>". */
static char *gpf_env_setters(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce((SELECT group_concat(x, char(10)) FROM (SELECT CASE s.label WHEN "
            "'Module' THEN s.file_path ELSE s.name END || ' -> ' || "
            "coalesce(json_extract(t.properties,'$.container'),'-') || '/' || t.name || ' (' || "
            "json_extract(e.properties,'$.source') || ')' AS x FROM edges e JOIN nodes s ON "
            "s.id=e.source_id JOIN nodes t ON t.id=e.target_id WHERE e.project=?1 AND "
            "e.type='CONFIGURES' AND json_extract(e.properties,'$.strategy')='env_set' ORDER BY "
            "x)), '') || char(10) || '#PORT=' || (SELECT count(*) FROM nodes WHERE project=?1 "
            "AND label='EnvVar' AND name='PORT') || char(10) || '#joined=' || (SELECT count(*) "
            "FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id "
            "WHERE e.project=?1 AND (s.label='EnvVar' OR (substr(t.qualified_name,1,7)='__env__' "
            "AND json_extract(e.properties,'$.strategy')='env_set')))";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* The environment a container is given: each name a K8s container's env
 * (any pod template; value or valueFrom), a compose service's environment
 * (map or list) or the last Dockerfile stage's ENV (with the stages it is
 * built FROM, a comment inside the continuation skipped) sets is an EnvVar
 * of that container -- two containers' PORT are two nodes, neither joined to
 * the PORT a program reads. No value reaches the graph. */
TEST(probe_container_env_sets_scoped_envvars) {
    static const GpfFile files[] = {
        {"app.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\nspec:\n  template:\n"
         "    spec:\n      initContainers:\n        - name: init\n          image: busybox\n"
         "          env:\n            - name: MODE\n              value: v-init-mode\n"
         "      containers:\n        - name: web\n          image: nginx\n          env:\n"
         "            - name: PORT\n              value: \"v-8080\"\n"
         "            - name: DB_PASS\n              valueFrom:\n                secretKeyRef:\n"
         "                  name: db\n                  key: pass\n"
         "            - name: POD_IP\n              valueFrom:\n                fieldRef:\n"
         "                  fieldPath: status.podIP\n"
         "---\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: api\nspec:\n"
         "  template:\n    spec:\n      containers:\n        - name: api\n"
         "          image: acme/api\n          env:\n            - name: PORT\n"
         "              value: \"v-9090\"\n"},
        {"compose.yaml", "services:\n  web:\n    image: acme/web\n    environment:\n"
                         "      LOG_LEVEL: v-debug\n      API_KEY: v-s3cr3t\n"
                         "  worker:\n    image: acme/worker\n    environment:\n"
                         "      - QUEUE=v-jobs\n      - VERBOSE\n"},
        {"Dockerfile", "FROM golang:1.22 AS build\nENV CGO_ENABLED=0\n"
                       "FROM alpine:3.20 AS base\nENV TZ=v-utc\n"
                       "FROM base\nENV APP_HOME=/v-app \\\n    # where it lives\n"
                       "    GREETING=\"v-hello world\"\n"},
        {"main.go", "package main\n\nimport \"os\"\n\nfunc main() { _ = os.Getenv(\"PORT\") }\n"}};
    char *got = gpf_env_setters(files, 4);
    static const char want[] = "Deployment/api -> api/PORT (value)\n"
                               "Deployment/web -> init/MODE (value)\n"
                               "Deployment/web -> web/DB_PASS (secretKeyRef)\n"
                               "Deployment/web -> web/POD_IP (fieldRef)\n"
                               "Deployment/web -> web/PORT (value)\n"
                               "Dockerfile -> -/APP_HOME (dockerfile)\n"
                               "Dockerfile -> -/GREETING (dockerfile)\n"
                               "Dockerfile -> -/TZ (dockerfile)\n"
                               "Service/web -> web/API_KEY (compose)\n"
                               "Service/web -> web/LOG_LEVEL (compose)\n"
                               "Service/worker -> worker/QUEUE (compose)\n"
                               "Service/worker -> worker/VERBOSE (compose)\n"
                               "#PORT=3\n#joined=0";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "env setters got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("env_set edge set");
    }
    free(got);
    static const char *const values[] = {"v-init-mode", "v-8080", "v-9090", "v-debug",
                                         "v-s3cr3t",    "v-jobs", "v-utc",  "v-hello"};
    for (size_t i = 0; i < sizeof(values) / sizeof(values[0]); i++) {
        int hits = gpf_graph_mentions(files, 4, values[i]);
        if (hits != 0) {
            fprintf(stderr, "value %s: %d graph mentions\n", values[i], hits);
            FAIL("an env value reached the graph");
        }
    }
    PASS();
}

/* Every env join edge (CONFIGURES {strategy:env_read}) as
 * "<setter file> <container>/<name> -> <reader file>", then the containers
 * the image references name: "#image_containers=...". */
static char *gpf_env_joins(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce((SELECT group_concat(x, char(10)) FROM (SELECT DISTINCT s.file_path "
            "|| ' ' || substr(s.qualified_name, instr(s.qualified_name, '#env/') + 5) || ' -> ' "
            "|| t.file_path AS x FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
            "t.id=e.target_id WHERE e.project=?1 AND e.type='CONFIGURES' AND "
            "json_extract(e.properties,'$.strategy')='env_read' ORDER BY x)), '') || char(10) || "
            "'#image_containers=' || coalesce((SELECT group_concat(c, ',') FROM (SELECT "
            "json_extract(e.properties,'$.container') AS c FROM edges e JOIN nodes s ON "
            "s.id=e.source_id WHERE e.project=?1 AND e.type='USAGE' AND "
            "s.file_path='k8s/shop.yaml' "
            "AND json_extract(e.properties,'$.ref')='image' ORDER BY c)), '')";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* Every CONNECTS_TO edge as "<source file> <source> -> <target> (<key>)". */
static char *gpf_connects(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce((SELECT group_concat(x, char(10)) FROM (SELECT s.file_path || ' ' || "
            "s.name || ' -> ' || t.name || ' (' || json_extract(e.properties,'$.key') || ')' AS x "
            "FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id WHERE "
            "e.project=?1 AND e.type='CONNECTS_TO' ORDER BY x)), '')";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* A service address in a container's environment CONNECTS_TO the Service it
 * names: `svc:port`, a URL's host, `<svc>.<ns>.svc...`, a bare name on an
 * address key -- a K8s Service of the bundle in the named (else the
 * object's own) namespace, a compose service of the same file, its .env
 * interpolated (a name without a value takes the .env's, which may itself
 * name others). Never: another domain's host, a templated value, localhost,
 * a bare name on any other key, a namespace without that Service, a service
 * addressing itself -- and never a value in the graph. */
TEST(probe_env_address_connects_to_its_service) {
    static const GpfFile files[] = {
        {"k8s/app.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  namespace: shop\n"
         "spec:\n  template:\n    spec:\n      containers:\n        - name: web\n"
         "          image: acme/web\n          env:\n"
         "            - name: CATALOG_ADDR\n              value: \"catalog:3550\"\n"
         "            - name: AUTH_URL\n"
         "              value: \"http://auth.auth.svc.cluster.local:8080/login\"\n"
         "            - name: CACHE_HOST\n              value: cache\n"
         "            - name: DB_HOST\n              value: db.example.com\n"
         "            - name: TEMPLATED_ADDR\n              value: \"{{ .Values.addr }}\"\n"
         "            - name: LOCAL_ADDR\n              value: \"localhost:9000\"\n"
         "            - name: GREETING\n              value: catalog\n"
         "            - name: PRICES_ADDR\n              value: \"catalog.other:3550\"\n"
         "            - name: MAIL_URL\n              value: "
         "\"smtp://mail.shop.example.com:25\"\n"},
        {"k8s/svc.yaml", "apiVersion: v1\nkind: Service\nmetadata:\n  name: catalog\n"
                         "  namespace: shop\n---\napiVersion: v1\nkind: Service\nmetadata:\n"
                         "  name: cache\n  namespace: shop\n---\napiVersion: v1\nkind: Service\n"
                         "metadata:\n  name: auth\n  namespace: auth\n---\napiVersion: v1\n"
                         "kind: Service\nmetadata:\n  name: db\n  namespace: shop\n---\n"
                         "apiVersion: v1\nkind: Service\nmetadata:\n  name: mail\n"
                         "  namespace: shop\n"},
        {"compose.yaml",
         "services:\n  web:\n    image: acme/web\n    environment:\n      DB_HOST: db\n"
         "      CACHE_URL: redis://cache:6379\n      API_KEY: s3cr3t-value\n"
         "      WEB_ADDR: web\n  worker:\n    image: acme/worker\n    environment:\n"
         "      - QUEUE_ADDR\n      - OTHER_URL=http://${QUEUE_HOST}:80\n"
         "  db:\n    image: postgres\n  cache:\n    image: redis\n  queue:\n"
         "    image: rabbitmq\n"},
        {".env", "QUEUE_PORT=5672\nQUEUE_HOST=queue\nQUEUE_ADDR=queue:${QUEUE_PORT}\n"}};
    int n = (int)(sizeof(files) / sizeof(files[0]));
    char *got = gpf_connects(files, n);
    static const char want[] = "compose.yaml Service/web -> Service/cache (CACHE_URL)\n"
                               "compose.yaml Service/web -> Service/db (DB_HOST)\n"
                               "compose.yaml Service/worker -> Service/queue (QUEUE_ADDR)\n"
                               "k8s/app.yaml Deployment/web -> Service/auth (AUTH_URL)\n"
                               "k8s/app.yaml Deployment/web -> Service/cache (CACHE_HOST)\n"
                               "k8s/app.yaml Deployment/web -> Service/catalog (CATALOG_ADDR)";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "connects got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("CONNECTS_TO edge set");
    }
    free(got);
    static const char *const values[] = {"catalog:3550", "s3cr3t-value", "redis://cache:6379",
                                         "queue:5672", "/login"};
    for (size_t i = 0; i < sizeof(values) / sizeof(values[0]); i++) {
        int hits = gpf_graph_mentions(files, n, values[i]);
        if (hits != 0) {
            fprintf(stderr, "value %s: %d graph mentions\n", values[i], hits);
            FAIL("an env value reached the graph");
        }
    }
    PASS();
}

/* Every kustomize patch edge as "<source file> <source label> -> <target>
 * @<target file>", then "patch imports <n>" (IMPORTS of patch files), "maps
 * into patches <n>" (INFRA_MAPS into a patch file's documents) and "version
 * <v>" (base/deploy.yaml's Resource), "odd version <v>" (other/odd.yaml's:
 * none without one, missing without the Resource), "labels <l>"
 * (base/deploy.yaml's). */
static char *gpf_patches(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce((SELECT group_concat(x, char(10)) FROM (SELECT s.file_path || ' ' || "
            "s.label || ' -> ' || t.name || ' @' || t.file_path AS x FROM edges e JOIN nodes s ON "
            "s.id=e.source_id JOIN nodes t ON t.id=e.target_id WHERE e.project=?1 AND "
            "e.type='OVERRIDE' AND json_extract(e.properties,'$.via')='patch' ORDER BY x)), '') "
            "|| char(10) || 'patch imports ' || (SELECT COUNT(*) FROM edges e WHERE e.project=?1 "
            "AND e.type='IMPORTS' AND json_extract(e.properties,'$.local_name')='patches') || "
            "char(10) || 'maps into patches ' || (SELECT COUNT(*) FROM edges e JOIN nodes t ON "
            "t.id=e.target_id WHERE e.project=?1 AND e.type='INFRA_MAPS' AND t.file_path LIKE "
            "'overlays/%') || char(10) || 'version ' || coalesce((SELECT "
            "json_extract(properties,'$.version') FROM nodes WHERE project=?1 AND "
            "label='Resource' AND file_path='base/deploy.yaml'), '-') || char(10) || "
            "'odd version ' || coalesce((SELECT coalesce(json_extract(properties,'$.version'), "
            "'none') FROM nodes WHERE project=?1 AND label='Resource' AND "
            "file_path='other/odd.yaml'), 'missing') || char(10) || 'labels ' || "
            "coalesce((SELECT json_extract(properties,'$.labels') FROM nodes WHERE project=?1 "
            "AND label='Resource' AND file_path='base/deploy.yaml'), '-')";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* A kustomization's patches OVERRIDE the objects of its build they change,
 * child -> base: a patch file's documents the objects of the same kind and
 * name; an inline patch (from the kustomization) the object its document
 * names (a `${VAR}` name as written); a JSON6902 operation file (from the
 * file) the objects its target selects -- group and version included; a
 * component's the objects of the build including it; a kind alone every
 * object of that kind. Never: an object outside the build, one the patch
 * does not name (another name, group, version or namespace), a component
 * nothing includes. A label selector meets the objects whose metadata.labels
 * carry its terms (`k=v`, `k`, `k!=v`; a Resource keeps them sorted). A
 * patch file is an import of
 * the kustomization, never a member of its build (a Service selects no patch
 * document). A Resource keeps the version of its apiVersion; a bare group
 * (`apiVersion: example.io`) has none. */
TEST(probe_kustomize_patches_override_their_base) {
    static const GpfFile files[] = {
        {"base/kustomization.yaml", "resources:\n- deploy.yaml\n- svc.yaml\n- cm.yaml\n"},
        {"base/cm.yaml",
         "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: ${APP}-config\n  namespace: app\n"},
        {"base/deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  namespace: app\n"
         "  labels:\n    tier: front\n    app: web\n    note: a=b\nspec:\n"
         "  template:\n    metadata:\n      labels:\n        app: web\n"},
        {"base/svc.yaml",
         "apiVersion: v1\nkind: Service\nmetadata:\n  name: web\n  namespace: app\n"
         "spec:\n  selector:\n    app: web\n"},
        {"overlays/prod/kustomization.yaml",
         "namespace: app\nresources:\n- ../../base\ncomponents:\n- ../../components/extra\n"
         "patches:\n"
         "- path: patch-deploy.yaml\n"
         "- path: ns-patch.yaml\n"
         "- path: all.yaml\n  target:\n    kind: Service\n"
         "- patch: |-\n    apiVersion: v1\n    kind: Service\n    metadata:\n      name: web\n"
         "    spec:\n      type: NodePort\n"
         "- path: ops.yaml\n  target:\n    kind: Deployment\n    name: web\n"
         "- path: ops-v2.yaml\n  target:\n    group: apps\n    version: v2\n    kind: Deployment\n"
         "- path: ops-batch.yaml\n  target:\n    group: batch\n    kind: Deployment\n"
         "- path: sel.yaml\n  target:\n    kind: Deployment\n    labelSelector: tier=none\n"
         "- path: lab.yaml\n  target:\n    kind: Deployment\n    labelSelector: app=web,tier\n"
         "- path: neq.yaml\n  target:\n    kind: Deployment\n    labelSelector: app!=web\n"
         "- path: sel-only.yaml\n  target:\n    labelSelector: tier=front\n"
         "- path: ann.yaml\n  target:\n    kind: Deployment\n    annotationSelector: x=y\n"
         "- path: cm-patch.yaml\n"
         "- patch: |-\n    apiVersion: apps/v1\n    kind: Deployment\n    metadata:\n"
         "      name: missing\n"},
        {"overlays/prod/ops-batch.yaml", "- op: replace\n  path: /spec/replicas\n  value: 5\n"},
        {"overlays/prod/ns-patch.yaml",
         "apiVersion: v1\nkind: Service\nmetadata:\n  name: web\n  namespace: elsewhere\n"},
        {"overlays/prod/all.yaml", "- op: add\n  path: /metadata/labels/tier\n  value: front\n"},
        {"overlays/prod/sel.yaml", "- op: replace\n  path: /spec/replicas\n  value: 6\n"},
        {"overlays/prod/lab.yaml", "- op: replace\n  path: /spec/replicas\n  value: 7\n"},
        {"overlays/prod/neq.yaml", "- op: replace\n  path: /spec/replicas\n  value: 8\n"},
        {"overlays/prod/sel-only.yaml", "- op: replace\n  path: /spec/replicas\n  value: 9\n"},
        {"overlays/prod/ann.yaml", "- op: replace\n  path: /spec/replicas\n  value: 1\n"},
        {"overlays/prod/cm-patch.yaml",
         "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: ${APP}-config\n  namespace: app\n"
         "data:\n  k: v\n"},
        {"overlays/prod/patch-deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  namespace: app\nspec:\n"
         "  template:\n    metadata:\n      labels:\n        app: web\n"},
        {"overlays/prod/ops.yaml", "- op: replace\n  path: /spec/replicas\n  value: 3\n"},
        {"overlays/prod/ops-v2.yaml", "- op: replace\n  path: /spec/replicas\n  value: 4\n"},
        {"components/extra/kustomization.yaml",
         "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\npatches:\n"
         "- patch: |-\n    apiVersion: apps/v1\n    kind: Deployment\n    metadata:\n"
         "      name: web\n      namespace: app\n"},
        {"components/unused/kustomization.yaml",
         "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\npatches:\n"
         "- patch: |-\n    apiVersion: v1\n    kind: Service\n    metadata:\n      name: web\n"},
        {"other/deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  namespace: app\n"},
        {"other/odd.yaml", "apiVersion: example.io\nkind: Thing\nmetadata:\n  name: odd\n"}};
    int n = (int)(sizeof(files) / sizeof(files[0]));
    char *got = gpf_patches(files, n);
    static const char want[] =
        "components/extra/kustomization.yaml File -> Deployment/web @base/deploy.yaml\n"
        "overlays/prod/all.yaml File -> Service/web @base/svc.yaml\n"
        "overlays/prod/cm-patch.yaml Resource -> ConfigMap/${APP}-config @base/cm.yaml\n"
        "overlays/prod/kustomization.yaml File -> Service/web @base/svc.yaml\n"
        "overlays/prod/lab.yaml File -> Deployment/web @base/deploy.yaml\n"
        "overlays/prod/ops.yaml File -> Deployment/web @base/deploy.yaml\n"
        "overlays/prod/patch-deploy.yaml Resource -> Deployment/web @base/deploy.yaml\n"
        "overlays/prod/sel-only.yaml File -> Deployment/web @base/deploy.yaml\n"
        "patch imports 12\n"
        "maps into patches 0\n"
        "version v1\n"
        "odd version none\n"
        "labels app=web,tier=front";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "patches got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("kustomize patch edges");
    }
    free(got);
    PASS();
}

/* Every kustomize chart deploy as "<source file> <source label> -> <target
 * label>/<target name> <properties less line>", then "values imports <n>". */
static char *gpf_kcharts(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce((SELECT group_concat(x, char(10)) FROM (SELECT s.file_path || ' ' || "
            "s.label || ' -> ' || t.label || '/' || t.name || ' ' || json_remove(e.properties, "
            "'$.line') AS x FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
            "t.id=e.target_id WHERE e.project=?1 AND e.type='DEPLOYS' AND "
            "json_extract(e.properties,'$.via')='helmcharts' ORDER BY x)), '') || char(10) || "
            "'values imports ' || (SELECT COUNT(*) FROM edges e WHERE e.project=?1 AND "
            "e.type='IMPORTS' AND json_extract(e.properties,'$.local_name')='valuesFile')";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* A chart a kustomization inflates (`helmCharts`) DEPLOYS from that
 * kustomization -- its Kustomization Resource, else (no `kind:` line) its
 * File -- to the shared external Chart of the name, keeping repository,
 * version, release, namespace and values file; two kustomizations of one
 * chart share it. A chart name with a path names nothing; a values file
 * outside the repository is not kept; one inside is an import. */
TEST(probe_kustomize_helm_charts_deploy_their_chart) {
    static const GpfFile files[] = {
        {"obs/kustomization.yaml",
         "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nhelmCharts:\n"
         "- name: loki\n  repo: https://grafana.github.io/helm-charts\n  version: 6.46.0\n"
         "  releaseName: logs\n  namespace: obs\n  valuesFile: values.yaml\n"
         "- name: org/thing\n  repo: https://example.com/charts\n"},
        {"obs/values.yaml", "loki:\n  auth_enabled: false\n"},
        {"plain/kustomization.yaml",
         "helmCharts:\n- name: loki\n  repo: oci://registry.example.com/charts\n"
         "  valuesFile: ../../../../outside.yaml\n"}};
    int n = (int)(sizeof(files) / sizeof(files[0]));
    char *got = gpf_kcharts(files, n);
    static const char want[] =
        "obs/kustomization.yaml Resource -> Chart/loki {\"via\":\"helmcharts\",\"external\":true,"
        "\"chart\":\"loki\",\"url\":\"https://grafana.github.io/"
        "helm-charts\",\"version\":\"6.46.0\","
        "\"release\":\"logs\",\"namespace\":\"obs\",\"values\":\"obs/values.yaml\"}\n"
        "plain/kustomization.yaml File -> Chart/loki {\"via\":\"helmcharts\",\"external\":true,"
        "\"chart\":\"loki\",\"url\":\"oci://registry.example.com/charts\"}\n"
        "values imports 1";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "kustomize charts got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("kustomize chart deploys");
    }
    free(got);
    PASS();
}

/* Every Terraform reference edge as
 * "<file> <block> -> <file> <block> <type> <ref>:<line>", then a line
 * "other tf edges <n>": USAGE / CALLS edges leaving a .tf file that the
 * module resolver did not make. */
static char *gpf_tf_refs(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce((SELECT group_concat(x, char(10)) FROM (SELECT s.file_path || ' ' || "
            "s.name || ' -> ' || t.file_path || ' ' || t.name || ' ' || e.type || ' ' || "
            "json_extract(e.properties,'$.ref') || ':' || json_extract(e.properties,'$.line') AS x "
            "FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id "
            "WHERE e.project=?1 AND e.type IN ('USAGE','DEPENDS_ON') AND "
            "json_extract(e.properties,'$.via')='terraform' ORDER BY x)), '') || char(10) || "
            "'other tf edges ' || (SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
            "WHERE e.project=?1 AND e.type IN ('USAGE','CALLS') AND s.file_path LIKE '%.tf' AND "
            "coalesce(json_extract(e.properties,'$.via'),'') != 'terraform')";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* A Terraform reference (var.X, local.X, module.X, data.T.N, T.N) USAGEs,
 * from the top-level block holding it, the block declaring it in the same
 * directory -- the module, all its .tf files -- with the reference kind and
 * line; an entry of the block's depends_on list is a DEPENDS_ON (beside a
 * USAGE where the block also uses the object). A bare label (`variable prefix {}`) names its block
 * like a quoted one. A name mentioned in string text, a `/ *` glob inside an interpolation, a
 * reference to the block holding it, and a directory declaring nothing of the name make no edge;
 * the generic name matcher makes none at all for a .tf file. */
TEST(probe_terraform_refs_resolve_in_their_module) {
    static const GpfFile files[] = {
        {"infra/variables.tf", "variable \"region\" {}\nvariable prefix {\n  default = \"x\"\n}\n"
                               "variable \"unused\" {\n  description = \"see var.region\"\n}\n"},
        {"infra/main.tf",
         "locals {\n  name = \"${var.prefix}-svc\"\n  full = \"${local.name}-${var.region}\"\n}\n"
         "resource \"aws_s3_bucket\" \"logs\" {\n  bucket = local.full\n}\n"
         "resource \"aws_s3_bucket_policy\" \"logs\" {\n  bucket = aws_s3_bucket.logs.id\n"
         "  policy = data.aws_iam_policy_document.p.json\n"
         "  depends_on = [aws_s3_bucket.logs]\n}\n"
         "data \"aws_iam_policy_document\" \"p\" {\n  statement {\n"
         "    resources = [\"${aws_s3_bucket.logs.arn}/*\"]\n  }\n}\n"
         "module \"net\" {\n  source = \"./net\"\n  region = var.region\n"
         "  depends_on = [\n    aws_s3_bucket_policy.logs,\n  ]\n}\n"
         "output \"net\" {\n  value = module.net.id\n}\n"},
        {"infra/net/main.tf",
         "variable \"region\" {}\nresource \"aws_vpc\" \"v\" {\n  tags = { r = var.region }\n}\n"},
        {"other/x.tf", "resource \"null_resource\" \"n\" {\n  triggers = { r = var.region }\n}\n"}};
    int n = (int)(sizeof(files) / sizeof(files[0]));
    char *got = gpf_tf_refs(files, n);
    static const char want[] =
        "infra/main.tf data.aws_iam_policy_document.p -> infra/main.tf resource.aws_s3_bucket.logs "
        "USAGE resource:15\n"
        "infra/main.tf locals -> infra/variables.tf variable.prefix USAGE var:2\n"
        "infra/main.tf locals -> infra/variables.tf variable.region USAGE var:3\n"
        "infra/main.tf module.net -> infra/main.tf resource.aws_s3_bucket_policy.logs "
        "DEPENDS_ON resource:22\n"
        "infra/main.tf module.net -> infra/variables.tf variable.region USAGE var:20\n"
        "infra/main.tf output.net -> infra/main.tf module.net USAGE module:26\n"
        "infra/main.tf resource.aws_s3_bucket.logs -> infra/main.tf locals USAGE local:6\n"
        "infra/main.tf resource.aws_s3_bucket_policy.logs -> infra/main.tf "
        "data.aws_iam_policy_document.p USAGE data:10\n"
        "infra/main.tf resource.aws_s3_bucket_policy.logs -> infra/main.tf "
        "resource.aws_s3_bucket.logs DEPENDS_ON resource:11\n"
        "infra/main.tf resource.aws_s3_bucket_policy.logs -> infra/main.tf "
        "resource.aws_s3_bucket.logs USAGE resource:9\n"
        "infra/net/main.tf resource.aws_vpc.v -> infra/net/main.tf variable.region USAGE var:3\n"
        "other tf edges 0";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "terraform refs got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("terraform refs");
    }
    free(got);
    PASS();
}

/* A Terraform / OpenTofu module file (.tf, .tofu) extracts its blocks but no
 * generic calls or usages -- its references are the module resolver's -- while
 * any other HCL file (Packer, Nomad, Terragrunt .hcl) keeps them. */
TEST(probe_terraform_module_files_extract_no_generic_refs) {
    static const char src[] = "locals {\n  watched = [1]\n  result = length(watched)\n}\n";
    static const char *const names[] = {"main.tf", "main.tofu", "main.hcl"};
    int calls[3];
    int usages[3];
    int defs[3];
    for (int i = 0; i < 3; i++) {
        CBMFileResult *r =
            cbm_extract_file(src, (int)strlen(src), CBM_LANG_HCL, "probe", names[i], 0, NULL, NULL);
        ASSERT_NOT_NULL(r);
        calls[i] = r->calls.count;
        usages[i] = r->usages.count;
        defs[i] = r->defs.count;
        cbm_free_result(r);
    }
    for (int i = 0; i < 2; i++) {
        ASSERT_EQ(calls[i], 0);
        ASSERT_EQ(usages[i], 0);
        ASSERT_GT(defs[i], 0);
    }
    ASSERT_GT(calls[2], 0);
    ASSERT_GT(usages[2], 0);
    PASS();
}

/* Every Terraform module IMPORTS and output-read edge as
 * "<file> <block> <type> <target> <local_name|ref>:<line>", the target a
 * Folder's path, the Project, or "<file> <block>". */
static char *gpf_tf_modules(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce((SELECT group_concat(x, char(10)) FROM (SELECT s.file_path || ' ' || "
            "s.name || ' ' || e.type || ' ' || CASE t.label WHEN 'Folder' THEN 'Folder:' || "
            "t.file_path WHEN 'Project' THEN 'Project' ELSE t.file_path || ' ' || t.name END || "
            "' ' || coalesce(json_extract(e.properties,'$.local_name'), "
            "json_extract(e.properties,'$.ref')) || ':' || json_extract(e.properties,'$.line') AS "
            "x "
            "FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id "
            "WHERE e.project=?1 AND json_extract(e.properties,'$.via')='terraform' AND "
            "(e.type='IMPORTS' OR json_extract(e.properties,'$.ref')='output') ORDER BY x)), '')";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* A module call with a local source IMPORTS {local_name} the Folder of that
 * directory (the root directory: the Project), and module.X.Y -- also through
 * an index or a splat -- USAGEs {ref: output} the `output "Y"` there. A
 * registry source (also `modules/net`: a local path starts with ./ or ../), a
 * directory that does not exist, holds no .tf file or lies past the
 * repository root, and an output the module does not declare make none. */
TEST(probe_terraform_module_calls_import_their_source) {
    static const GpfFile files[] = {
        {"main.tf", "module \"net\" {\n  source = \"./modules/net\"\n}\n"
                    "module \"cnt\" {\n  source = \"./modules/net\"\n  count  = 2\n}\n"
                    "module \"reg\" {\n  source  = \"terraform-aws-modules/vpc/aws\"\n"
                    "  version = \"5.0.0\"\n}\n"
                    "module \"gone\" {\n  source = \"./modules/missing\"\n}\n"
                    "module \"out\" {\n  source = \"../outside\"\n}\n"
                    "output \"a\" {\n  value = module.net.id\n}\n"
                    "output \"b\" {\n  value = module.cnt[0].id\n}\n"
                    "output \"c\" {\n  value = module.cnt[*].name\n}\n"
                    "output \"d\" {\n  value = module.net.nope\n}\n"
                    "output \"e\" {\n  value = module.reg.vpc_id\n}\n"
                    "module \"bare\" {\n  source = \"modules/net\"\n}\n"
                    "module \"docs\" {\n  source = \"./docs\"\n}\n"},
        {"docs/README.md", "# docs\n"},
        {"modules/net/main.tf",
         "output \"id\" {\n  value = \"x\"\n}\noutput \"name\" {\n  value = \"n\"\n}\n"},
        {"examples/basic/main.tf", "module \"root\" {\n  source = \"../..\"\n}\noutput \"r\" {\n  "
                                   "value = module.root.a\n}\n"}};
    int n = (int)(sizeof(files) / sizeof(files[0]));
    char *got = gpf_tf_modules(files, n);
    static const char want[] = "examples/basic/main.tf module.root IMPORTS Project root:1\n"
                               "examples/basic/main.tf output.r USAGE main.tf output.a output:5\n"
                               "main.tf module.cnt IMPORTS Folder:modules/net cnt:4\n"
                               "main.tf module.net IMPORTS Folder:modules/net net:1\n"
                               "main.tf output.a USAGE modules/net/main.tf output.id output:19\n"
                               "main.tf output.b USAGE modules/net/main.tf output.id output:22\n"
                               "main.tf output.c USAGE modules/net/main.tf output.name output:25";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "terraform modules got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("terraform modules");
    }
    free(got);
    PASS();
}

/* Every tfvars OVERRIDE edge as "<file> <name> -> <file> <block> <line>",
 * " inferred" appended when the edge says so. */
static char *gpf_tf_tfvars(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce((SELECT group_concat(x, char(10)) FROM (SELECT s.file_path || ' ' || "
            "s.name || ' -> ' || t.file_path || ' ' || t.name || ' ' || "
            "json_extract(e.properties,'$.line') || CASE WHEN "
            "json_extract(e.properties,'$.inferred') THEN ' inferred' ELSE '' END AS x FROM edges "
            "e "
            "JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id WHERE e.project=?1 "
            "AND e.type='OVERRIDE' AND json_extract(e.properties,'$.via')='tfvars' ORDER BY x)), "
            "'')";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* A .tfvars file's assignment to a variable the module of its own directory
 * declares OVERRIDEs {via: tfvars, line} that `variable` block, from the
 * file's Variable of the name. A file whose directory declares none of its
 * names -- no Terraform there, or a module that does not declare them --
 * sets the nearest ancestor module's that does (applied by a -var-file the
 * repository does not state: `inferred`). An undeclared name, and a file
 * with no declaring module above it, make none. */
TEST(probe_terraform_tfvars_override_their_variables) {
    static const GpfFile files[] = {
        {"stack/variables.tf", "variable \"region\" {}\nvariable \"size\" {\n  default = 1\n}\n"},
        {"stack/terraform.tfvars", "region  = \"eu\"\nsize    = 3\nunknown = \"x\"\n"},
        {"stack/prod/prod.tfvars", "region = \"us\"\n"},
        {"stack/sub/main.tf", "resource \"null_resource\" \"x\" {}\n"},
        {"stack/sub/sub.tfvars", "size = 9\n"},
        {"other.tfvars", "region = \"ap\"\n"}};
    int n = (int)(sizeof(files) / sizeof(files[0]));
    char *got = gpf_tf_tfvars(files, n);
    static const char want[] =
        "stack/prod/prod.tfvars region -> stack/variables.tf variable.region 1 inferred\n"
        "stack/sub/sub.tfvars size -> stack/variables.tf variable.size 1 inferred\n"
        "stack/terraform.tfvars region -> stack/variables.tf variable.region 1\n"
        "stack/terraform.tfvars size -> stack/variables.tf variable.size 2";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "terraform tfvars got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("terraform tfvars");
    }
    free(got);
    PASS();
}

/* Every Terragrunt edge as "<file> <block> <type> <target> <local_name |
 * read=<file> | ->:<line>", a Folder target as "Folder:<path>". */
static char *gpf_tg_edges(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce((SELECT group_concat(x, char(10)) FROM (SELECT s.file_path || ' ' || "
            "s.name || ' ' || e.type || ' ' || CASE t.label WHEN 'Folder' THEN 'Folder:' || "
            "t.file_path ELSE t.file_path END || ' ' || coalesce(json_extract(e.properties,"
            "'$.local_name'), 'read=' || json_extract(e.properties,'$.read'), '-') || ':' || "
            "json_extract(e.properties,'$.line') AS x FROM edges e JOIN nodes s ON "
            "s.id=e.source_id JOIN nodes t ON t.id=e.target_id WHERE e.project=?1 AND "
            "json_extract(e.properties,'$.via')='terragrunt' ORDER BY x)), '')";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* A Terragrunt unit (terragrunt.hcl), evaluated in its own context: an
 * include IMPORTS {local_name} the file its path names; a
 * read_terragrunt_config IMPORTS {read} the file it names -- from the block
 * holding the call, or, for a call in an included file (evaluated for the
 * including unit: find_in_parent_folders searches from the UNIT), from the
 * include block bringing that file in; a dependency DEPENDS_ON the other
 * unit; a local terraform source IMPORTS its Folder. A path built from an
 * unknown function (get_env) makes none. */
TEST(probe_terragrunt_units_compose_their_files) {
    static const GpfFile files[] = {
        {"root.hcl",
         "locals {\n  acct = read_terragrunt_config(find_in_parent_folders(\"account.hcl\"))\n}\n"},
        {"live/account.hcl", "locals {\n  id = \"1\"\n}\n"},
        {"live/app/terragrunt.hcl",
         "include \"root\" {\n  path = find_in_parent_folders(\"root.hcl\")\n}\n"
         "include \"common\" {\n"
         "  path = \"${dirname(find_in_parent_folders(\"root.hcl\"))}/_common/app.hcl\"\n}\n"
         "dependency \"db\" {\n  config_path = \"../db\"\n}\n"
         "terraform {\n  source = \"../../modules/app\"\n}\n"
         "locals {\n  env = read_terragrunt_config(\"env.hcl\")\n"
         "  bad = read_terragrunt_config(get_env(\"X\"))\n}\n"},
        {"live/app/env.hcl", "locals {\n  name = \"app\"\n}\n"},
        {"_common/app.hcl",
         "locals {\n  acct = read_terragrunt_config(find_in_parent_folders(\"account.hcl\"))\n}\n"},
        {"live/db/terragrunt.hcl",
         "include \"root\" {\n  path = find_in_parent_folders(\"root.hcl\")\n}\n"},
        {"modules/app/main.tf", "variable \"x\" {}\n"}};
    int n = (int)(sizeof(files) / sizeof(files[0]));
    char *got = gpf_tg_edges(files, n);
    static const char want[] =
        "live/app/terragrunt.hcl dependency.db DEPENDS_ON live/db/terragrunt.hcl db:7\n"
        "live/app/terragrunt.hcl include.common IMPORTS _common/app.hcl common:4\n"
        "live/app/terragrunt.hcl include.common IMPORTS live/account.hcl read=_common/app.hcl:2\n"
        "live/app/terragrunt.hcl include.root IMPORTS live/account.hcl read=root.hcl:2\n"
        "live/app/terragrunt.hcl include.root IMPORTS root.hcl root:1\n"
        "live/app/terragrunt.hcl locals IMPORTS live/app/env.hcl read=live/app/terragrunt.hcl:14\n"
        "live/app/terragrunt.hcl terraform IMPORTS Folder:modules/app -:10\n"
        "live/db/terragrunt.hcl include.root IMPORTS live/account.hcl read=root.hcl:2\n"
        "live/db/terragrunt.hcl include.root IMPORTS root.hcl root:1";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "terragrunt got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("terragrunt");
    }
    free(got);
    PASS();
}

/* Every Ansible IMPORTS edge as "<file> <type> <label>:<target> <local_name>:<line>". */
static char *gpf_ansible(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce((SELECT group_concat(x, char(10)) FROM (SELECT s.file_path || ' ' || "
            "e.type || ' ' || t.label || ':' || t.file_path || ' ' || "
            "json_extract(e.properties,'$.local_name') || ':' || "
            "json_extract(e.properties,'$.line') AS x FROM edges e JOIN nodes s ON "
            "s.id=e.source_id JOIN nodes t ON t.id=e.target_id WHERE e.project=?1 AND "
            "json_extract(e.properties,'$.via')='ansible' ORDER BY x)), '')";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* A playbook or task file (a YAML file whose top level is a sequence)
 * IMPORTS {local_name: <directive>} what its directives name: import_playbook
 * a playbook from its directory; include_tasks / import_tasks a task file
 * from its directory, else from its role's tasks/ (also inside a block:);
 * roles: items and include_role / import_role a role's Folder, roles/<name>
 * at or above its directory. A templated target, an empty directive, and a
 * mapping-topped file (values, CI) with the same keys, make none. */
TEST(probe_ansible_files_import_what_they_name) {
    static const GpfFile files[] = {
        {"site.yml", "- name: Install\n  import_playbook: playbooks/cluster.yml\n"},
        {"playbooks/cluster.yml",
         "- hosts: all\n  roles:\n    - common\n    - role: web\n      when: false\n"
         "    - \"{{ dyn }}\"\n  tasks:\n    - name: a\n      include_tasks: tasks/extra.yml\n"
         "    - name: b\n      ansible.builtin.import_role:\n        name: db\n"},
        {"playbooks/tasks/extra.yml", "- name: x\n  debug:\n    msg: hi\n"},
        {"roles/common/tasks/main.yml",
         "- name: setup\n  include_tasks: setup.yml\n- name: nested\n  block:\n"
         "    - name: inner\n      import_tasks: other.yml\n"},
        {"roles/common/tasks/setup.yml", "- name: s\n  debug:\n    msg: s\n"},
        {"roles/common/tasks/other.yml", "- name: o\n  debug:\n    msg: o\n"},
        {"roles/common/handlers/main.yml", "- name: h\n  include_tasks: setup.yml\n"},
        {"roles/web/tasks/main.yml",
         "- name: w\n  debug:\n    msg: w\n- name: empty\n  include_tasks:\n"},
        {"roles/db/tasks/main.yml", "- name: d\n  debug:\n    msg: d\n"},
        {"values.yaml", "include: playbooks/tasks/extra.yml\n"
                        "users:\n  - name: ops\n    roles:\n      - common\n"}};
    int n = (int)(sizeof(files) / sizeof(files[0]));
    char *got = gpf_ansible(files, n);
    static const char want[] =
        "playbooks/cluster.yml IMPORTS Folder:roles/common roles:3\n"
        "playbooks/cluster.yml IMPORTS Folder:roles/db import_role:11\n"
        "playbooks/cluster.yml IMPORTS Folder:roles/web roles:4\n"
        "playbooks/cluster.yml IMPORTS Module:playbooks/tasks/extra.yml include_tasks:9\n"
        "roles/common/handlers/main.yml IMPORTS Module:roles/common/tasks/setup.yml "
        "include_tasks:2\n"
        "roles/common/tasks/main.yml IMPORTS Module:roles/common/tasks/other.yml import_tasks:6\n"
        "roles/common/tasks/main.yml IMPORTS Module:roles/common/tasks/setup.yml include_tasks:2\n"
        "site.yml IMPORTS Module:playbooks/cluster.yml import_playbook:2";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "ansible got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("ansible");
    }
    free(got);
    PASS();
}

/* Every Resource a Terraform block DEPLOYS as "<block> DEPLOYS <name>
 * <obj_key> v=<version>", then "terraform resources <n>": the Resources
 * marked ecosystem terraform, then each INFRA_MAPS edge into one of them as
 * "<label>:<name> INFRA_MAPS <name>". */
static char *gpf_tf_k8s(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce((SELECT group_concat(x, char(10)) FROM (SELECT s.name || ' DEPLOYS ' "
            "|| "
            "t.name || ' ' || json_extract(t.properties,'$.obj_key') || ' v=' || "
            "coalesce(json_extract(t.properties,'$.version'), '-') AS x FROM edges e JOIN nodes s "
            "ON "
            "s.id=e.source_id JOIN nodes t ON t.id=e.target_id WHERE e.project=?1 AND "
            "e.type='DEPLOYS' AND t.label='Resource' AND "
            "json_extract(e.properties,'$.via')='terraform' ORDER BY x)), '') || char(10) || "
            "'terraform resources ' || (SELECT COUNT(*) FROM nodes WHERE project=?1 AND "
            "label='Resource' AND json_extract(properties,'$.ecosystem')='terraform') || "
            "coalesce((SELECT group_concat(y, '') FROM (SELECT char(10) || s.label || ':' || "
            "s.name "
            "|| ' INFRA_MAPS ' || t.name AS y FROM edges e JOIN nodes s ON s.id=e.source_id JOIN "
            "nodes t ON t.id=e.target_id WHERE e.project=?1 AND e.type='INFRA_MAPS' AND "
            "json_extract(t.properties,'$.ecosystem')='terraform' ORDER BY y)), '')";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* A kubernetes_<type> resource (also _vN) with a literal metadata name is a
 * Resource of the K8s identity scheme, ecosystem terraform, its block
 * DEPLOYS it: the kind its type names, or a kubernetes_manifest's own
 * apiVersion / kind. The name and namespace come from the object's own
 * metadata only (never spec's), a default service account's is `default`;
 * a cluster-scoped kind's obj_key says `_`. A
 * computed name, and a type acting on an object declared elsewhere
 * (kubernetes_annotations), make none. A manifest of the same bundle
 * references it like a manifest's object: the Ingress maps to the Service. */
TEST(probe_terraform_kubernetes_objects_are_resources) {
    static const GpfFile files[] = {
        {"k8s/main.tf",
         "resource \"kubernetes_namespace_v1\" \"apps\" {\n  metadata {\n    name = \"apps\"\n  "
         "}\n}\n"
         "resource \"kubernetes_deployment\" \"web\" {\n  metadata {\n    name      = \"web\"\n"
         "    namespace = \"apps\"\n  }\n  spec {\n    template {\n      metadata {\n"
         "        name = \"inner\"\n      }\n    }\n  }\n}\n"
         "resource \"kubernetes_manifest\" \"cert\" {\n  manifest = {\n"
         "    \"apiVersion\" = \"cert-manager.io/v1\"\n    \"kind\"       = \"Certificate\"\n"
         "    \"metadata\" = {\n      \"name\"      = \"tls\"\n      \"namespace\" = \"apps\"\n"
         "    }\n  }\n}\n"
         "resource \"kubernetes_service\" \"computed\" {\n  metadata {\n    name = var.name\n  "
         "}\n}\n"
         "resource \"kubernetes_api_service\" \"metrics\" {\n  metadata {\n"
         "    name = \"v1beta1.metrics.k8s.io\"\n  }\n  spec {\n    service {\n"
         "      name      = \"metrics-server\"\n      namespace = \"kube-system\"\n    }\n  }\n}\n"
         "resource \"kubernetes_annotations\" \"patch\" {\n  api_version = \"v1\"\n"
         "  kind        = \"ConfigMap\"\n  metadata {\n    name = \"cm\"\n  }\n}\n"
         "resource \"kubernetes_default_service_account\" \"sa\" {\n  metadata {\n"
         "    namespace = \"apps\"\n  }\n}\n"
         "resource \"kubernetes_service_v1\" \"web\" {\n  metadata {\n    name      = \"web\"\n"
         "    namespace = \"apps\"\n  }\n}\n"
         "variable \"name\" {}\n"},
        {"k8s/ingress.yaml",
         "apiVersion: networking.k8s.io/v1\nkind: Ingress\nmetadata:\n  name: web\n"
         "  namespace: apps\nspec:\n  rules:\n    - http:\n        paths:\n          - path: /\n"
         "            pathType: Prefix\n            backend:\n              service:\n"
         "                name: web\n                port:\n                  number: 80\n"}};
    int n = (int)(sizeof(files) / sizeof(files[0]));
    char *got = gpf_tf_k8s(files, n);
    static const char want[] =
        "resource.kubernetes_api_service.metrics DEPLOYS APIService/v1beta1.metrics.k8s.io "
        "apiregistration.k8s.io/APIService/_/v1beta1.metrics.k8s.io v=-\n"
        "resource.kubernetes_default_service_account.sa DEPLOYS ServiceAccount/default "
        "core/ServiceAccount/apps/default v=-\n"
        "resource.kubernetes_deployment.web DEPLOYS Deployment/web apps/Deployment/apps/web v=-\n"
        "resource.kubernetes_manifest.cert DEPLOYS Certificate/tls "
        "cert-manager.io/Certificate/apps/tls v=v1\n"
        "resource.kubernetes_namespace_v1.apps DEPLOYS Namespace/apps core/Namespace/_/apps v=-\n"
        "resource.kubernetes_service_v1.web DEPLOYS Service/web core/Service/apps/web v=-\n"
        "terraform resources 6\n"
        "Resource:Ingress/web INFRA_MAPS Service/web\n"
        "Route:*/ INFRA_MAPS Service/web";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "terraform k8s got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("terraform k8s");
    }
    free(got);
    PASS();
}

/* Every kustomize variant as "<variant> <QN key> <- <base file>:<base>
 * via=<via> kust=<kustomization>:<line>", then "variants <n> defined <m>":
 * the Resources marked ecosystem kustomize, and the DEFINES edges into
 * them. */
static char *gpf_kust_variants(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce((SELECT group_concat(x, char(10)) FROM (SELECT v.name || ' ' || "
            "substr(v.qualified_name, instr(v.qualified_name, '::') + 2) || ' <- ' || "
            "b.file_path || ':' || b.name || ' via=' || json_extract(e.properties,'$.via') || "
            "' kust=' || json_extract(e.properties,'$.kustomization') || ':' || "
            "json_extract(e.properties,'$.line') AS x FROM edges e JOIN nodes v ON "
            "v.id=e.source_id JOIN nodes b ON b.id=e.target_id WHERE e.project=?1 AND "
            "e.type='OVERRIDE' AND json_extract(v.properties,'$.ecosystem')='kustomize' ORDER BY "
            "x)), '') || char(10) || 'variants ' || (SELECT COUNT(*) FROM nodes WHERE project=?1 "
            "AND label='Resource' AND json_extract(properties,'$.ecosystem')='kustomize') || "
            "' defined ' || (SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN "
            "nodes t ON t.id=e.target_id WHERE e.project=?1 AND e.type='DEFINES' AND "
            "s.label='File' AND json_extract(t.properties,'$.ecosystem')='kustomize')";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* A kustomization's namePrefix / nameSuffix deploys its objects under other
 * names: each renamed object gets a variant Resource with the deployed name
 * (ecosystem kustomize), which the kustomization's File DEFINES and which
 * OVERRIDEs {via: namePrefix | nameSuffix, kustomization, line} the object.
 * Prefixes compose down the build (outer first), as do suffixes (inner
 * first); the outermost namespace wins; a cluster-scoped kind keeps its
 * own (none); CRDs, Namespaces and apiregistration APIServices keep their
 * names (kustomize's skip list) and get none; a prefix with a `/` is no
 * name part and renames nothing. */
TEST(probe_kustomize_name_prefix_makes_variants) {
    static const GpfFile files[] = {
        {"base/kustomization.yaml",
         "namespace: system\nnamePrefix: b-\nresources:\n- app.yaml\n- crd.yaml\n- ns.yaml\n"
         "- apisvc.yaml\n- role.yaml\n"},
        {"base/app.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  namespace: system\n"
         "---\napiVersion: v1\nkind: Service\nmetadata:\n  name: web\n  namespace: system\n"},
        {"base/crd.yaml",
         "apiVersion: apiextensions.k8s.io/v1\nkind: CustomResourceDefinition\nmetadata:\n"
         "  name: widgets.example.com\n"},
        {"base/ns.yaml", "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: system\n"},
        {"base/apisvc.yaml", "apiVersion: apiregistration.k8s.io/v1\nkind: APIService\nmetadata:\n"
                             "  name: v1.example.com\n"},
        {"base/role.yaml",
         "apiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRole\nmetadata:\n"
         "  name: reader\n"},
        {"overlay/kustomization.yaml",
         "namespace: prod\nnamePrefix: p-\nnameSuffix: -x\nresources:\n- ../base\n"},
        {"suffixed/kustomization.yaml", "nameSuffix: -s\nresources:\n- ../base\n"},
        {"slashed/kustomization.yaml", "namePrefix: x/\nresources:\n- ../base\n"}};
    int n = (int)(sizeof(files) / sizeof(files[0]));
    char *got = gpf_kust_variants(files, n);
    static const char want[] =
        "ClusterRole/b-reader rbac.authorization.k8s.io/ClusterRole/-/b-reader <- "
        "base/role.yaml:ClusterRole/reader via=namePrefix kust=base/kustomization.yaml:2\n"
        "ClusterRole/b-reader-s rbac.authorization.k8s.io/ClusterRole/-/b-reader-s <- "
        "base/role.yaml:ClusterRole/reader via=nameSuffix kust=suffixed/kustomization.yaml:1\n"
        "ClusterRole/p-b-reader-x rbac.authorization.k8s.io/ClusterRole/-/p-b-reader-x <- "
        "base/role.yaml:ClusterRole/reader via=namePrefix kust=overlay/kustomization.yaml:2\n"
        "Deployment/b-web apps/Deployment/system/b-web <- base/app.yaml:Deployment/web "
        "via=namePrefix kust=base/kustomization.yaml:2\n"
        "Deployment/b-web-s apps/Deployment/system/b-web-s <- base/app.yaml:Deployment/web "
        "via=nameSuffix kust=suffixed/kustomization.yaml:1\n"
        "Deployment/p-b-web-x apps/Deployment/prod/p-b-web-x <- base/app.yaml:Deployment/web "
        "via=namePrefix kust=overlay/kustomization.yaml:2\n"
        "Service/b-web core/Service/system/b-web <- base/app.yaml:Service/web via=namePrefix "
        "kust=base/kustomization.yaml:2\n"
        "Service/b-web-s core/Service/system/b-web-s <- base/app.yaml:Service/web "
        "via=nameSuffix kust=suffixed/kustomization.yaml:1\n"
        "Service/p-b-web-x core/Service/prod/p-b-web-x <- base/app.yaml:Service/web "
        "via=namePrefix kust=overlay/kustomization.yaml:2\n"
        "variants 9 defined 9";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "kustomize variants got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("kustomize variants");
    }
    free(got);
    PASS();
}

/* Every Ansible task / handler as "<file>:<line> <kind> <name> [<module>]",
 * then every notify as "<task> -> <handler file>:<handler> @<line>", then
 * "functions <n> defined <m>". */
static char *gpf_ansible_tasks(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce((SELECT group_concat(x, char(10)) FROM (SELECT file_path || ':' || "
            "start_line || ' ' || json_extract(properties,'$.kind') || ' ' || name || ' [' || "
            "json_extract(properties,'$.module') || ']' AS x FROM nodes WHERE project=?1 AND "
            "label='Function' AND json_extract(properties,'$.source')='ansible' ORDER BY "
            "file_path, start_line)), '') || char(10) || coalesce((SELECT group_concat(y, "
            "char(10)) FROM (SELECT s.name || ' -> ' || t.file_path || ':' || t.name || ' @' || "
            "json_extract(e.properties,'$.line') AS y FROM edges e JOIN nodes s ON "
            "s.id=e.source_id JOIN nodes t ON t.id=e.target_id WHERE e.project=?1 AND "
            "e.type='CALLS' AND json_extract(e.properties,'$.via')='notify' ORDER BY y)), '') || "
            "char(10) || 'functions ' || (SELECT COUNT(*) FROM nodes WHERE project=?1 AND "
            "label='Function' AND json_extract(properties,'$.source')='ansible') || ' defined ' "
            "|| (SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
            "t.id=e.target_id WHERE e.project=?1 AND e.type='DEFINES' AND s.label='File' AND "
            "json_extract(t.properties,'$.source')='ansible')";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* An Ansible task (or handler) is a Function, named by its `name:` (else its
 * module), {source: ansible, kind, module}, which its file DEFINES:
 * a playbook's pre_tasks / tasks / post_tasks / handlers, a role's tasks/
 * and handlers/ files, and files an Ansible file includes; a block's tasks
 * (block, rescue, always), never the block itself. A notify CALLS {via:
 * notify, line} each handler of that name or listen topic in the task's own
 * role (a nested role is the directory before its tasks/), else in its file.
 * A YAML list that is no Ansible (a JSON patch) holds none. */
TEST(probe_ansible_tasks_are_functions) {
    static const GpfFile files[] = {
        {"site.yml",
         "- hosts: all\n  pre_tasks:\n    - name: Prep\n      command: /bin/true\n"
         "  roles:\n    - web\n  handlers:\n    - name: reload all\n"
         "      service: name=x state=reloaded\n  tasks:\n    - name: Ping\n      ping:\n"
         "      notify: reload all\n- import_playbook: other.yml\n"},
        {"other.yml",
         "- hosts: db\n  tasks:\n    - debug: msg=hi\n    - include_tasks: tasks/common.yml\n"},
        {"tasks/common.yml",
         "- name: Common\n  shell: echo c\n- name: \"Say \\\"hi\\\"\"\n  debug:\n    msg: x\n"
         "- name: 'it''s'\n  debug:\n    msg: y\n- name: Act\n  action: shell echo a\n"},
        {"roles/web/tasks/main.yml",
         "- name: Install web\n  package:\n    name: nginx\n  notify:\n    - restart web\n"
         "    - web changed\n- block:\n    - name: Inside block\n      template:\n"
         "        src: a.j2\n        dest: /etc/a\n      notify: restart web\n  rescue:\n"
         "    - name: Rescue it\n      debug:\n        msg: failed\n  always:\n"
         "    - name: Always it\n      ansible.builtin.file:\n        path: /tmp/x\n"
         "        state: absent\n- include_tasks: extra.yml\n"},
        {"roles/web/tasks/extra.yml", "- name: Extra\n  command: echo hi\n"},
        {"roles/web/handlers/main.yml",
         "- name: restart web\n  service:\n    name: nginx\n    state: restarted\n"
         "- name: log change\n  debug:\n    msg: changed\n  listen: web changed\n"},
        {"roles/apps/csi/tasks/main.yml", "- name: Csi\n  command: x\n  notify: restart csi\n"},
        {"roles/apps/csi/handlers/main.yml", "- name: restart csi\n  command: y\n"},
        {"roles/apps/lb/handlers/main.yml", "- name: restart csi\n  command: w\n"},
        {"roles/other/handlers/main.yml", "- name: restart csi\n  command: z\n"},
        {"patches/ops.yaml", "- op: add\n  path: /x\n  value: 1\n"}};
    int n = (int)(sizeof(files) / sizeof(files[0]));
    char *got = gpf_ansible_tasks(files, n);
    static const char want[] = "other.yml:3 task debug [debug]\n"
                               "other.yml:4 task include_tasks [include_tasks]\n"
                               "roles/apps/csi/handlers/main.yml:1 handler restart csi [command]\n"
                               "roles/apps/csi/tasks/main.yml:1 task Csi [command]\n"
                               "roles/apps/lb/handlers/main.yml:1 handler restart csi [command]\n"
                               "roles/other/handlers/main.yml:1 handler restart csi [command]\n"
                               "roles/web/handlers/main.yml:1 handler restart web [service]\n"
                               "roles/web/handlers/main.yml:5 handler log change [debug]\n"
                               "roles/web/tasks/extra.yml:1 task Extra [command]\n"
                               "roles/web/tasks/main.yml:1 task Install web [package]\n"
                               "roles/web/tasks/main.yml:8 task Inside block [template]\n"
                               "roles/web/tasks/main.yml:14 task Rescue it [debug]\n"
                               "roles/web/tasks/main.yml:18 task Always it [file]\n"
                               "roles/web/tasks/main.yml:22 task include_tasks [include_tasks]\n"
                               "site.yml:3 task Prep [command]\n"
                               "site.yml:8 handler reload all [service]\n"
                               "site.yml:11 task Ping [ping]\n"
                               "tasks/common.yml:1 task Common [shell]\n"
                               "tasks/common.yml:3 task Say \"hi\" [debug]\n"
                               "tasks/common.yml:6 task it's [debug]\n"
                               "tasks/common.yml:9 task Act [shell]\n"
                               "Csi -> roles/apps/csi/handlers/main.yml:restart csi @3\n"
                               "Inside block -> roles/web/handlers/main.yml:restart web @12\n"
                               "Install web -> roles/web/handlers/main.yml:log change @4\n"
                               "Install web -> roles/web/handlers/main.yml:restart web @4\n"
                               "Ping -> site.yml:reload all @13\n"
                               "functions 21 defined 21";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "ansible tasks got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("ansible tasks");
    }
    free(got);
    PASS();
}

/* Every DEPLOYS an Ansible task draws, as "<task> DEPLOYS <file> [<module>]",
 * then "deploys <n>". */
static char *gpf_ansible_deploys(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce((SELECT group_concat(x, char(10)) FROM (SELECT s.name || ' DEPLOYS ' "
            "|| t.file_path || ' [' || json_extract(e.properties,'$.module') || ']' AS x FROM "
            "edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id WHERE "
            "e.project=?1 AND e.type='DEPLOYS' AND json_extract(e.properties,'$.via')='ansible' "
            "ORDER BY x)), '') || char(10) || 'deploys ' || (SELECT COUNT(*) FROM edges WHERE "
            "project=?1 AND type='DEPLOYS' AND json_extract(properties,'$.via')='ansible')";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* A kube / k8s task DEPLOYS {via: ansible, module, line} the repository file
 * it applies, traced to the template (or copied file) its play rendered it
 * from: a renderer whose dest is the very filename; {{ item.item.K }} over
 * "{{ R.results }}" -> each item of the renderer registering R (its list
 * literal, a set_fact's or the role defaults'); {{ item.K }} -> each of the
 * task's own items through the renderer of that dest; a literal filename a
 * looped renderer renders to; a k8s template: argument. A template is found
 * before a copy to the same dest. A remote copy (even of a name the role's
 * files/ holds), a delete by name, apply nothing in the repository. */
TEST(probe_ansible_kube_tasks_deploy_their_templates) {
    static const GpfFile files[] = {
        {"roles/app/tasks/main.yml",
         "- name: Render manifests\n  template:\n    src: \"{{ item.file }}.j2\"\n"
         "    dest: \"{{ conf }}/{{ item.file }}\"\n  with_items:\n    - {name: a, file: a.yml}\n"
         "    - {name: b, file: b.yml}\n  register: app_manifests\n"
         "- name: Apply manifests\n  kube:\n    filename: \"{{ conf }}/{{ item.item.file }}\"\n"
         "  with_items:\n    - \"{{ app_manifests.results }}\"\n"
         "- name: Render single\n  template:\n    src: single.yml.j2\n"
         "    dest: \"{{ conf }}/single.yml\"\n"
         "- name: Apply single\n  kube:\n    filename: \"{{ conf }}/single.yml\"\n"
         "- name: Copy static\n  copy:\n    src: static.yml\n    dest: \"{{ conf }}/static.yml\"\n"
         "- name: Apply static\n  kube:\n    filename: \"{{ conf }}/static.yml\"\n"
         "- name: Render from defaults\n  template:\n    src: \"{{ item.file }}.j2\"\n"
         "    dest: \"{{ conf }}/x/{{ item.file }}\"\n  loop: \"{{ app_extra }}\"\n"
         "  register: extra_out\n"
         "- name: Apply from defaults\n  kube:\n    filename: \"{{ conf }}/x/{{ item.item.file "
         "}}\"\n"
         "  loop: \"{{ extra_out.results }}\"\n"
         "- name: Facts\n  set_fact:\n    sf_list:\n      - {file: e.yml}\n"
         "- name: Render from facts\n  template:\n    src: \"{{ item.file }}.j2\"\n"
         "    dest: \"{{ conf }}/f/{{ item.file }}\"\n  with_items: \"{{ sf_list }}\"\n"
         "  register: sf_out\n"
         "- name: Apply set_fact\n  kube:\n    filename: \"{{ conf }}/f/{{ item.item.file }}\"\n"
         "  with_items: \"{{ sf_out.results }}\"\n"
         "- name: Pairs\n  template:\n    src: \"{{ item.name }}/{{ item.file }}.j2\"\n"
         "    dest: \"{{ conf }}/{{ item.name }}-{{ item.file }}\"\n  with_items:\n"
         "    - {name: c, file: c.yml}\n"
         "- name: Apply pairs\n  kube:\n    filename: \"{{ conf }}/{{ item.name }}-{{ item.file "
         "}}\"\n"
         "  loop:\n    - {name: c, file: c.yml}\n"
         "- name: Rendered literal\n  template:\n    src: \"{{ item.file }}.j2\"\n"
         "    dest: \"{{ conf }}/r/{{ item.file }}\"\n  with_items:\n    - {file: ipam.yml}\n"
         "- name: Apply literal\n  kube:\n    filename: \"{{ conf }}/r/ipam.yml\"\n"
         "- name: Copy first\n  copy:\n    src: first.yml\n    dest: \"{{ conf }}/dup.yml\"\n"
         "- name: Render dup\n  template:\n    src: dup.yml.j2\n    dest: \"{{ conf }}/dup.yml\"\n"
         "- name: Apply dup\n  kube:\n    filename: \"{{ conf }}/dup.yml\"\n"
         "- name: Remote copy\n  copy:\n    src: static.yml\n    dest: \"{{ conf }}/remote.yml\"\n"
         "    remote_src: true\n"
         "- name: Apply remote\n  kube:\n    filename: \"{{ conf }}/remote.yml\"\n"
         "- name: Delete by name\n  kube:\n    name: old\n    resource: deploy\n    state: absent\n"
         "- name: k8s template\n  kubernetes.core.k8s:\n    template: tpl.yml.j2\n"},
        {"roles/app/defaults/main.yml", "app_extra:\n  - file: d.yml\nother: 1\n"},
        {"roles/app/templates/a.yml.j2", "kind: ConfigMap\n"},
        {"roles/app/templates/b.yml.j2", "kind: ConfigMap\n"},
        {"roles/app/templates/c/c.yml.j2", "kind: ConfigMap\n"},
        {"roles/app/templates/d.yml.j2", "kind: ConfigMap\n"},
        {"roles/app/templates/e.yml.j2", "kind: ConfigMap\n"},
        {"roles/app/templates/ipam.yml.j2", "kind: ConfigMap\n"},
        {"roles/app/templates/single.yml.j2", "kind: ConfigMap\n"},
        {"roles/app/templates/tpl.yml.j2", "kind: ConfigMap\n"},
        {"roles/app/templates/dup.yml.j2", "kind: ConfigMap\n"},
        {"roles/app/files/first.yml", "kind: ConfigMap\n"},
        {"roles/app/files/static.yml", "kind: ConfigMap\n"}};
    int n = (int)(sizeof(files) / sizeof(files[0]));
    char *got = gpf_ansible_deploys(files, n);
    static const char want[] =
        "Apply dup DEPLOYS roles/app/templates/dup.yml.j2 [kube]\n"
        "Apply from defaults DEPLOYS roles/app/templates/d.yml.j2 [kube]\n"
        "Apply literal DEPLOYS roles/app/templates/ipam.yml.j2 [kube]\n"
        "Apply manifests DEPLOYS roles/app/templates/a.yml.j2 [kube]\n"
        "Apply manifests DEPLOYS roles/app/templates/b.yml.j2 [kube]\n"
        "Apply pairs DEPLOYS roles/app/templates/c/c.yml.j2 [kube]\n"
        "Apply set_fact DEPLOYS roles/app/templates/e.yml.j2 [kube]\n"
        "Apply single DEPLOYS roles/app/templates/single.yml.j2 [kube]\n"
        "Apply static DEPLOYS roles/app/files/static.yml [kube]\n"
        "k8s template DEPLOYS roles/app/templates/tpl.yml.j2 [kubernetes.core.k8s]\n"
        "deploys 10";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "ansible deploys got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("ansible deploys");
    }
    free(got);
    PASS();
}

/* Every script HTTP_CALLS edge as "<source file> <route QN>", then a line
 * "edgeless routes <n>": the Route nodes no edge touches. */
static char *gpf_script_http(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce((SELECT group_concat(x, char(10)) FROM (SELECT s.file_path || ' ' || "
            "t.qualified_name AS x FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
            "t.id=e.target_id WHERE e.project=?1 AND e.type='HTTP_CALLS' AND "
            "json_extract(e.properties,'$.via') IN ('curl','wget') ORDER BY x)), '') || char(10) "
            "|| 'edgeless routes ' || (SELECT COUNT(*) FROM nodes r WHERE r.project=?1 AND "
            "r.label='Route' AND NOT EXISTS (SELECT 1 FROM edges e WHERE e.target_id=r.id OR "
            "e.source_id=r.id))";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* A script's curl / wget of a URL it states HTTP_CALLS the Route of it, with
 * the method its flags give (-X, a body POST, -T PUT, -I HEAD, -G GET,
 * wget --post-data POST): a Kubernetes Service of the repository by the path
 * alone (`api`, `api.default.svc.cluster.local`), where that service's
 * handler meets it; any other host -- a download, a domain under the
 * service's name, localhost even beside a Service named so -- with the host
 * kept, and no path-only Route made for it. Constant words resolve
 * (`v${VER}`), unknown parts are `{}`, also in an unquoted word the grammar
 * splits; never a proxy's or referer's URL, and never the user info or the
 * query (a token) in the graph. */
TEST(probe_script_http_request_calls_its_route) {
    static const GpfFile files[] = {
        {"k8s/api.yaml", "apiVersion: v1\nkind: Service\nmetadata:\n  name: api\n"},
        {"k8s/localhost.yaml", "apiVersion: v1\nkind: Service\nmetadata:\n  name: localhost\n"},
        {"dl.sh", "#!/bin/bash\nVER=1.2.3\n"
                  "curl -fsSL https://github.com/x/y/releases/download/v1/tool.tgz -o tool.tgz\n"
                  "curl -X POST -d '{}' \"http://api:8080/v1/items?token=s3cr3t-token\"\n"
                  "wget --post-data=x https://user:pw-secret@dl.example.com/a/b\n"
                  "curl -I https://example.com/head\n"
                  "curl -x http://proxy.local:3128 https://example.org/target\n"
                  "curl -LO \"https://dl.example.com/v${VER}/x-${VER}.tgz\"\n"
                  "curl \"https://dl.example.com/r/${UNKNOWN}/y.tgz\"\n"
                  "curl http://localhost:9090/metrics\n"
                  "curl -G -d q=1 https://example.com/search\n"
                  "curl --request DELETE https://example.com/item/1\n"
                  "curl -fsSLO https://dl.example.com/u/$OS-$ARCH.tgz\n"
                  "curl -LO https://dl.example.com/j/`uname`.tgz\n"
                  "curl -LO https://dl.example.com/q/${VER}$OS.tgz\n"
                  "curl https://api.example.com/v1/ping\n"
                  "curl http://api.default.svc.cluster.local:8080/health\n"
                  "curl \"https://${HOST}/api/x\"\n"},
        {"Makefile",
         "TOOL_VERSION=0.1.1\nARCH=amd64\n"
         "install:\n\tcurl -sSL https://get.example.com/install.sh | sh\n"
         "bin/tool:\n\tcurl -sLo \"bin/tool\" https://dl.example.com/t/v$(TOOL_VERSION)/"
         "tool_v$(TOOL_VERSION)_$(OS)_$(ARCH)\n"}};
    int n = (int)(sizeof(files) / sizeof(files[0]));
    char *got = gpf_script_http(files, n);
    static const char want[] =
        "Makefile __route__GET__https://dl.example.com/t/v0.1.1/tool_v0.1.1_{}_amd64\n"
        "Makefile __route__GET__https://get.example.com/install.sh\n"
        "dl.sh __route__DELETE__https://example.com/item/1\n"
        "dl.sh __route__GET__/health\n"
        "dl.sh __route__GET__http://localhost:9090/metrics\n"
        "dl.sh __route__GET__https://api.example.com/v1/ping\n"
        "dl.sh __route__GET__https://dl.example.com/j/{}.tgz\n"
        "dl.sh __route__GET__https://dl.example.com/q/{}{}.tgz\n"
        "dl.sh __route__GET__https://dl.example.com/r/{}/y.tgz\n"
        "dl.sh __route__GET__https://dl.example.com/u/{}-{}.tgz\n"
        "dl.sh __route__GET__https://dl.example.com/v1.2.3/x-1.2.3.tgz\n"
        "dl.sh __route__GET__https://example.com/search\n"
        "dl.sh __route__GET__https://example.org/target\n"
        "dl.sh __route__GET__https://github.com/x/y/releases/download/v1/tool.tgz\n"
        "dl.sh __route__HEAD__https://example.com/head\n"
        "dl.sh __route__POST__/v1/items\n"
        "dl.sh __route__POST__https://dl.example.com/a/b\n"
        "edgeless routes 0";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "script http got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("script HTTP_CALLS edge set");
    }
    free(got);
    static const char *const values[] = {"s3cr3t-token", "pw-secret", "proxy.local"};
    for (size_t i = 0; i < sizeof(values) / sizeof(values[0]); i++) {
        int hits = gpf_graph_mentions(files, n, values[i]);
        if (hits != 0) {
            fprintf(stderr, "value %s: %d graph mentions\n", values[i], hits);
            FAIL("a credential, token or proxy reached the graph");
        }
    }
    PASS();
}

/* A variable a container sets joins the readers of its name inside the files
 * its own image is built from: the K8s container's image (two containers,
 * two images), built by compose from a Dockerfile; the stages the last one
 * copies from; a JSON-form COPY opening with `/` and a COPY with a trailing
 * blank before a comment (both lost by the grammar); a context the sources
 * exist under (the repository root for side/, web/ for web/). Never: the same
 * name in another container's image, a file the context's .dockerignore
 * drops, a test file, or a reader in no image. */
TEST(probe_container_env_joins_its_own_image_readers) {
    static const GpfFile files[] = {
        {"k8s/shop.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: shop\nspec:\n  template:\n"
         "    spec:\n      containers:\n        - name: web\n          image: acme/web:1\n"
         "          env:\n            - name: PORT\n              value: \"8080\"\n"
         "            - name: MODE\n              value: prod\n"
         "        - name: side\n          image: acme/side:2\n          env:\n"
         "            - name: PORT\n              value: \"9090\"\n"},
        {"compose.yaml", "services:\n  web:\n    image: acme/web:1\n    build: ./web\n"
                         "    environment:\n      API_KEY: k\n"
                         "  side:\n    image: acme/side:2\n    build:\n      context: .\n"
                         "      dockerfile: side/Dockerfile\n"},
        {"web/Dockerfile", "FROM node:20 AS build\nCOPY src ./src \n# a comment after a blank\n"
                           "RUN npm ci\nFROM node:20-slim\nCOPY --from=build /src /app/src\n"
                           "COPY [\"/lib/\", \"/app/lib/\"]\nCOPY gen ./gen\n"
                           "CMD [\"node\", \"src/server.js\"]\n"},
        {"web/.dockerignore", "gen/skip.js\n"},
        {"web/src/server.js", "const port = process.env.PORT;\nconst mode = process.env.MODE;\n"
                              "const key = process.env.API_KEY;\n"},
        {"web/src/server.test.js", "const p = process.env.PORT;\n"},
        {"web/lib/conf.js", "module.exports = process.env.MODE;\n"},
        {"web/gen/a.js", "exports.m = process.env.MODE;\n"},
        {"web/gen/skip.js", "exports.m = process.env.MODE;\n"},
        {"side/Dockerfile", "FROM golang:1.22 AS build\nCOPY side/main.go shared/util.go /src/\n"
                            "RUN go build -o /app /src\nFROM alpine:3.20\n"
                            "COPY --from=build /app /app\nENTRYPOINT [\"/app\"]\n"},
        {"side/main.go",
         "package main\n\nimport \"os\"\n\nfunc main() { _ = os.Getenv(\"PORT\") }\n"},
        {"shared/util.go", "package shared\n\nimport \"os\"\n\n"
                           "func Port() string { return os.Getenv(\"PORT\") }\n"},
        {"other/main.go", "package main\n\nimport \"os\"\n\n"
                          "func main() { _ = os.Getenv(\"PORT\"); _ = os.Getenv(\"MODE\") }\n"}};
    char *got = gpf_env_joins(files, (int)(sizeof(files) / sizeof(files[0])));
    static const char want[] = "compose.yaml web/API_KEY -> web/src/server.js\n"
                               "k8s/shop.yaml side/PORT -> shared/util.go\n"
                               "k8s/shop.yaml side/PORT -> side/main.go\n"
                               "k8s/shop.yaml web/MODE -> web/gen/a.js\n"
                               "k8s/shop.yaml web/MODE -> web/lib/conf.js\n"
                               "k8s/shop.yaml web/MODE -> web/src/server.js\n"
                               "k8s/shop.yaml web/PORT -> web/src/server.js\n"
                               "#image_containers=side,web";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "env joins got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("env_read edge set");
    }
    free(got);
    PASS();
}

#define GPF_CM(name) "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: " name "\n"
#define GPF_KUST "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n"

/* A HelmRelease whose chartRef names an OCIRepository deploys the chart that
 * repository fetches: the external chart of the oci:// URL's last segment.
 * No edge for a URL with a variable, nor for a chartRef naming nothing. */
TEST(probe_helmrelease_chartref_deploys_its_oci_chart) {
    static const GpfFile files[] = {
        {"apps/web/kustomization.yaml",
         GPF_KUST "namespace: web\nresources:\n  - ocirepository.yaml\n  - helmrelease.yaml\n"},
        {"apps/web/ocirepository.yaml",
         "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata:\n"
         "  name: web-chart\nspec:\n  interval: 15m\n  ref:\n    tag: 1.2.3\n"
         "  url: oci://ghcr.io/acme/charts/web-app\n"},
        {"apps/web/helmrelease.yaml",
         "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata:\n  name: web\n"
         "spec:\n  chartRef:\n    kind: OCIRepository\n    name: web-chart\n  values:\n"
         "    fullnameOverride: '{{ .Release.Name }}'\n"},
        {"apps/db/kustomization.yaml",
         GPF_KUST "namespace: db\nresources:\n  - ocirepository.yaml\n  - helmrelease.yaml\n"},
        {"apps/db/ocirepository.yaml",
         "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata:\n"
         "  name: db-chart\nspec:\n  url: oci://ghcr.io/acme/charts/${CHART}\n"},
        {"apps/db/helmrelease.yaml",
         "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata:\n  name: db\n"
         "spec:\n  chartRef:\n    kind: OCIRepository\n    name: db-chart\n"},
        {"apps/x/helmrelease.yaml",
         "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata:\n  name: x\n"
         "spec:\n  chartRef:\n    kind: OCIRepository\n    name: missing\n"}};
    GPF_ASSERT_DEPLOYS(files, 7, "HelmRelease/web -> chart:web-app(ext) (helmrelease)");
    PASS();
}

/* A chartRef may deploy only the unique OCI source in every explicit build.
 * Nearby files, competing declarations and a namespace mismatch are not proof. */
TEST(probe_helmrelease_chartref_requires_unique_explicit_source) {
    static const GpfFile release = {"app/release.yaml",
                                    "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\n"
                                    "metadata:\n  name: web\n  namespace: web\nspec:\n  chartRef:\n"
                                    "    kind: OCIRepository\n    name: chart\n"};
    static const GpfFile source = {"app/source.yaml",
                                   "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\n"
                                   "metadata:\n  name: chart\n  namespace: web\nspec:\n"
                                   "  url: oci://registry.example.test/charts/web\n"};
    static const GpfFile alternative = {
        "app/alternative.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\n"
                                "metadata:\n  name: chart\n  namespace: web\nspec:\n"
                                "  url: oci://other.example.test/charts/web\n"};
    const GpfFile unconfigured[] = {release, source};
    GPF_ASSERT_DEPLOYS(unconfigured, 2, "");
    const GpfFile selected[] = {
        release,
        source,
        alternative,
        {"app/kustomization.yaml", GPF_KUST "resources:\n  - release.yaml\n  - source.yaml\n"}};
    GPF_ASSERT_DEPLOYS(selected, 4, "HelmRelease/web -> chart:web(ext) (helmrelease)");
    const GpfFile ambiguous[] = {release,
                                 source,
                                 alternative,
                                 {"app/kustomization.yaml",
                                  GPF_KUST "resources:\n  - release.yaml\n  - source.yaml\n"
                                           "  - alternative.yaml\n"}};
    GPF_ASSERT_DEPLOYS(ambiguous, 4, "");
    const GpfFile wrong_namespace[] = {
        release,
        {"app/source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\n"
                            "metadata:\n  name: chart\n  namespace: other\nspec:\n"
                            "  url: oci://registry.example.test/charts/web\n"},
        selected[3]};
    GPF_ASSERT_DEPLOYS(wrong_namespace, 3, "");
    const GpfFile duplicate[] = {release,
                                 {"app/source.yaml",
                                  "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\n"
                                  "metadata:\n  name: chart\n  namespace: web\nspec:\n"
                                  "  url: oci://registry.example.test/charts/web\n---\n"
                                  "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\n"
                                  "metadata:\n  name: chart\n  namespace: web\nspec:\n"
                                  "  url: oci://other.example.test/charts/web\n"},
                                 selected[3]};
    GPF_ASSERT_DEPLOYS(duplicate, 3, "");
    const GpfFile unread_duplicate[] = {
        release,
        {"app/source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\n"
                            "metadata:\n  name: chart\n  namespace: web\nspec:\n"
                            "  url: oci://registry.example.test/charts/web\n---\n"
                            "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\n"
                            "metadata:\n  name: chart\n  namespace: web\nspec:\n"
                            "  url: oci://other.example.test/charts/${CHART}\n"},
        selected[3]};
    GPF_ASSERT_DEPLOYS(unread_duplicate, 3, "");
    PASS();
}

/* kubectl apply -f deploys a manifest, a directory's manifests (-R: below
 * it too; not a file holding no objects), a glob's; -k and a kustomize build
 * piped into kubectl a Kustomization. The source is the function running the
 * command, top-level code its file; paths resolve against the cd'd root. */
TEST(probe_shell_kubectl_deploys_what_it_applies) {
    static const GpfFile files[] = {
        {"manifests/app.yaml", GPF_CM("app")},
        {"manifests/svc.yaml", GPF_CM("svc")},
        {"manifests/notes.yaml", "notes:\n  - not an object\n"},
        {"manifests/nested/cm.yaml", GPF_CM("cm")},
        {"crds/a.yaml", GPF_CM("a")},
        {"crds/b.yaml", GPF_CM("b")},
        {"overlays/prod/kustomization.yaml", GPF_KUST},
        {"overlays/dev/kustomization.yaml", GPF_KUST},
        {"hack/deploy.sh", "#!/usr/bin/env bash\n"
                           "cd \"$(dirname \"$0\")/..\"\n\n"
                           "apply_all() {\n"
                           "  kubectl apply -f manifests/app.yaml\n"
                           "  kubectl --namespace \"$NS\" apply -f manifests\n"
                           "  kubectl apply -Rf manifests/nested\n"
                           "}\n\n"
                           "kubectl apply -f 'crds/*.yaml'\n"
                           "kubectl apply -k overlays/prod\n"
                           "kustomize build overlays/dev | sed s/a/b/ | kubectl apply -f -\n"
                           "cat <<EOF | kubectl apply -f -\nkind: Inline\nEOF\n"}};
    GPF_ASSERT_DEPLOYS(files, 9,
                       "@hack/deploy.sh -> File:crds/a.yaml (kubectl)\n"
                       "@hack/deploy.sh -> File:crds/b.yaml (kubectl)\n"
                       "@hack/deploy.sh -> Resource:overlays/dev/kustomization.yaml (kustomize)\n"
                       "@hack/deploy.sh -> Resource:overlays/prod/kustomization.yaml (kubectl)\n"
                       "apply_all -> File:manifests/app.yaml (kubectl)\n"
                       "apply_all -> File:manifests/nested/cm.yaml (kubectl)\n"
                       "apply_all -> File:manifests/svc.yaml (kubectl)");
    PASS();
}

/* helm install / upgrade deploys its chart: a chart dir (through options,
 * a wrapper, $(git rev-parse --show-toplevel)), else the external chart of
 * repo/name or oci://.../name; a helm template piped into kubectl too. No
 * edge from an unknown word that may shift the chart's position, nor from
 * a variable an if/else leaves undecided. */
TEST(probe_shell_helm_deploys_its_chart) {
    static const GpfFile files[] = {
        {"charts/web/Chart.yaml", "apiVersion: v2\nname: web\nversion: 0.1.0\n"},
        {"charts/web/values-prod.yaml", "replicas: 2\n"},
        {"charts/api/Chart.yaml", "apiVersion: v2\nname: api\nversion: 0.1.0\n"},
        {"scripts/install.sh",
         "#!/bin/sh\n"
         "ROOT=\"$(git rev-parse --show-toplevel)\"\n"
         "NS=${NAMESPACE:-web}\n"
         "install_web() {\n"
         "  retry 3 helm upgrade --install --namespace \"$NS\" --create-namespace --atomic "
         "web \"$ROOT/charts/web/\"\n"
         "}\n"
         "install_cache() {\n"
         "  helm install cache bitnami/redis --version 1.2.3\n"
         "  helm install queue oci://registry.example.com/charts/nats\n"
         "}\n"
         "render_web() {\n"
         "  helm template web charts/web -f charts/web/values-prod.yaml | kubectl apply -f -\n"
         "}\n"
         "split_args() {\n"
         "  helm upgrade $ARGS charts/api\n"
         "}\n"
         "branchy() {\n"
         "  CHART=charts/web\n"
         "  if [ -n \"$PROD\" ]; then CHART=charts/api; fi\n"
         "  helm upgrade --install x \"$CHART\"\n"
         "}\n"}};
    GPF_ASSERT_DEPLOYS(files, 4,
                       "install_cache -> chart:nats(ext) (helm)\n"
                       "install_cache -> chart:redis(ext) (helm)\n"
                       "install_web -> chart:web (helm)\n"
                       "render_web -> chart:web (helm)");
    PASS();
}

/* Constant propagation: SCRIPT_ROOT from $(cd "$(dirname ...)" && pwd -P),
 * pushd / popd, an option whose value is unknown, an array holding the
 * command; a subshell's cd does not leak; with no directory tracked a path
 * resolves against the script's dir or the root -- only when one of the two
 * has it. */
TEST(probe_shell_paths_follow_the_script) {
    static const GpfFile files[] = {
        {"apps/web/deployment.yaml", GPF_CM("deployment")},
        {"apps/web/service.yaml", GPF_CM("service")},
        {"apps/web/sub/cm.yaml", GPF_CM("cm")},
        {"apps/web/sub/extra.yaml", GPF_CM("extra")},
        {"apps/web/config.yaml", GPF_CM("config-here")},
        {"config.yaml", GPF_CM("config-root")},
        {"apps/web/deploy.sh", "#!/usr/bin/env bash\n"
                               "SCRIPT_ROOT=$(cd \"$(dirname \"${BASH_SOURCE[0]}\")\" && pwd -P)\n"
                               "context=\"${KUBECTL_CONTEXT:-gke}\"\n"
                               "if ! kubectl config get-contexts \"${context}\"; then\n"
                               "  context=\"other\"\n"
                               "fi\n"
                               "pushd \"${SCRIPT_ROOT}\" >/dev/null\n"
                               "kubectl --context=\"${context}\" apply -f .\n"
                               "popd >/dev/null\n"
                               "( cd sub && kubectl apply -f cm.yaml )\n"
                               "kubectl apply -f extra.yaml\n"
                               "ambiguous() { kubectl apply -f config.yaml; }\n"
                               "unique() { kubectl apply -f deployment.yaml; }\n"
                               "deploy() {\n"
                               "  local kubectl=(\n"
                               "    kubectl\n"
                               "    --context=\"${context}\"\n"
                               "  )\n"
                               "  \"${kubectl[@]}\" apply -f service.yaml\n"
                               "}\n"}};
    GPF_ASSERT_DEPLOYS(files, 7,
                       "@apps/web/deploy.sh -> File:apps/web/config.yaml (kubectl)\n"
                       "@apps/web/deploy.sh -> File:apps/web/deployment.yaml (kubectl)\n"
                       "@apps/web/deploy.sh -> File:apps/web/service.yaml (kubectl)\n"
                       "@apps/web/deploy.sh -> File:apps/web/sub/cm.yaml (kubectl)\n"
                       "deploy -> File:apps/web/service.yaml (kubectl)\n"
                       "unique -> File:apps/web/deployment.yaml (kubectl)");
    PASS();
}

/* A program a script starts by a path SPAWNS the Process of its name: from
 * a value, or a literal last segment after an unknown prefix; inside $( )
 * and < <( ) too; past a wrapper. Not a script of the repository, not
 * `command -v X` (a lookup, not a run). */
TEST(probe_shell_programs_by_path_spawn) {
    static const GpfFile files[] = {
        {"hack/lib.sh", "#!/bin/sh\necho lib\n"},
        {"hack/run.sh", "#!/usr/bin/env bash\n"
                        "cd \"$(dirname \"$0\")/..\"\n"
                        "./_build/kubermatic-installer deploy kubermatic-master --config x.yaml\n"
                        "\"${INSTALL_DIR}/kubermatic-installer\" deploy seed-mla\n"
                        "./hack/lib.sh\n"
                        "if command -v bin/checker >/dev/null; then echo ok; fi\n"
                        "version=$(bin/werf version)\n"
                        "retry 3 /usr/local/bin/helm version\n"
                        "while read -r l; do echo \"$l\"; done < <(./bin/lister --all)\n"
                        "run() { bin/tool --flag; }\n"}};
    GPF_ASSERT_SPAWNS(files, 2,
                      "@hack/run.sh -> helm\n"
                      "@hack/run.sh -> kubermatic-installer\n"
                      "@hack/run.sh -> lister\n"
                      "@hack/run.sh -> werf\n"
                      "run -> tool");
    PASS();
}

/* Make recipes start programs by path as well: through a variable whose
 * prefix is unknown ($(GOBIN)/controller-gen); in a rule tree-sitter-make
 * leaves inside an ERROR (an indented, nested ifeq before it), with a
 * variable assigned after the conditionals; and line by line where the
 * grammar runs one recipe_line over several lines (a quote opened in
 * "Don't" and closed lines later) -- there a repository script stays out. */
TEST(probe_make_programs_by_path_spawn) {
    static const GpfFile files[] = {
        {"a/tools/validate.sh", "#!/bin/sh\necho ok\n"},
        {"a/Makefile", "check:\n"
                       "\t@docker run --rm img -f $(file)\n"
                       "\t@./tools/validate.sh\n"
                       "\t@./_build/tool run\n"
                       "\t@echo \"Don't forget\"\n"
                       "\tdocker run --rm img -c \"/app/check.sh 2>/dev/null | sed '/Checking all "
                       "the pages/ d; /^Maybe wrong/d' | sort -u\"\n"},
        {"b/Makefile", "LOCALBIN ?= $(shell pwd)/bin\n\n"
                       "envs:\n"
                       "  ifeq ($(A),)\n"
                       " \t\texport A=1\n"
                       "  endif\n"
                       "  ifeq ($(B),)\n"
                       "\t\tifeq ($(C),)\n"
                       "\t\t\texport B := $(C)\n"
                       "\t\telse\n"
                       "\t\t\texport B := x\n"
                       "\t\tendif\n"
                       " \tendif\n\n"
                       "\texport R := $(B)\n\n"
                       "TOOL ?= $(LOCALBIN)/lint-tool\n\n"
                       "build: envs\n"
                       "\tbin/werf build\n"
                       "\t$(TOOL) run\n"},
        {"c/Makefile", "GOBIN ?= $(shell go env GOPATH)/bin\n"
                       "CONTROLLER_GEN = $(GOBIN)/controller-gen\n\n"
                       "generate:\n"
                       "\t$(CONTROLLER_GEN) object paths=./...\n\n"
                       "probe:\n"
                       "\t@command -v bin/checker >/dev/null || echo missing\n"}};
    GPF_ASSERT_SPAWNS(files, 4,
                      "build -> lint-tool\n"
                      "build -> werf\n"
                      "check -> tool\n"
                      "generate -> controller-gen");
    PASS();
}

/* A script of the repository a command runs SPAWNS that script (its Module):
 * by its path, through a shell (bash -e FILE), past `env A="$X/y"`; not
 * itself. CI runs commands too: a GitHub workflow's run: steps from the
 * workspace (`${{ github.workspace }}`, env literals, working-directory;
 * an unknown `${{ }}` prefix leaves a Process; no pwsh step, no step of a
 * Windows job), a composite action's from its directory
 * (`${{ github.action_path }}`), a decorated Prow job's container command
 * exec'd from the checkout (`bash -c TEXT` as shell; undecorated: the
 * directory unknown, so a Process). Their edges carry the CI and the job. */
TEST(probe_ci_and_scripts_run_scripts) {
    static const GpfFile files[] = {
        {"charts/web/Chart.yaml", "apiVersion: v2\nname: web\nversion: 0.1.0\n"},
        {"hack/lib/helper.sh", "#!/bin/sh\necho helper\n"},
        {"hack/lint.sh", "#!/bin/sh\necho lint\n"},
        {"hack/ci/run-e2e.sh", "#!/bin/sh\necho e2e\n"},
        {"hack/ci/lint.sh", "#!/bin/sh\necho lint\n"},
        {"ops/apply.sh", "#!/bin/sh\necho apply\n"},
        {"hack/test.sh", "#!/usr/bin/env bash\n"
                         "bash -e \"$(dirname \"$0\")/lib/helper.sh\"\n"
                         "env OUT=\"$TMPDIR/x.rpm\" ./hack/lint.sh\n"},
        {"hack/e2e.sh", "#!/bin/sh\n"
                        "./hack/test.sh\n"
                        "[ -n \"$AGAIN\" ] || AGAIN=1 \"$0\"\n"},
        {"Makefile", "test:\n"
                     "\t./hack/test.sh\n"
                     "\t/bin/bash hack/lint.sh\n"},
        {".github/workflows/ci.yml", "name: ci\n"
                                     "on: [push]\n"
                                     "env:\n"
                                     "  CHART_DIR: charts/web\n"
                                     "jobs:\n"
                                     "  test:\n"
                                     "    runs-on: ubuntu-latest\n"
                                     "    steps:\n"
                                     "      - uses: actions/checkout@v4\n"
                                     "      - run: ./hack/test.sh\n"
                                     "      - name: multi\n"
                                     "        run: |\n"
                                     "          sh hack/lint.sh --fix\n"
                                     "          ${{ github.workspace }}/hack/e2e.sh\n"
                                     "          helm upgrade --install web \"$CHART_DIR\"\n"
                                     "          ./bin/tool --x\n"
                                     "      - working-directory: ops\n"
                                     "        run: ./apply.sh\n"
                                     "      - run: \"${{ matrix.dir }}/hack/other.sh\"\n"
                                     "      - shell: pwsh\n"
                                     "        run: ./hack/ci/lint.sh\n"
                                     "  win:\n"
                                     "    runs-on: windows-latest\n"
                                     "    steps:\n"
                                     "      - run: ./hack/ci/run-e2e.sh\n"},
        {".github/actions/setup/setup.sh", "#!/bin/sh\necho setup\n"},
        {".github/actions/setup/action.yml", "name: setup\n"
                                             "runs:\n"
                                             "  using: composite\n"
                                             "  steps:\n"
                                             "    - shell: bash\n"
                                             "      run: ${{ github.action_path }}/setup.sh\n"},
        {".prow/jobs.yaml", "presubmits:\n"
                            "  - name: pre-e2e\n"
                            "    decorate: true\n"
                            "    spec:\n"
                            "      containers:\n"
                            "        - image: x\n"
                            "          command:\n"
                            "            - \"./hack/ci/run-e2e.sh\"\n"
                            "  - name: pre-lint\n"
                            "    decorate: true\n"
                            "    spec:\n"
                            "      containers:\n"
                            "        - command: [\"bash\", \"-c\", \"./hack/ci/lint.sh\"]\n"
                            "  - name: pre-raw\n"
                            "    spec:\n"
                            "      containers:\n"
                            "        - command: [\"./hack/ci/run-e2e.sh\"]\n"}};
    GPF_ASSERT_RUNS(
        files, 13,
        "@.github/actions/setup/action.yml -> script:.github/actions/setup/setup.sh "
        "(SPAWNS shell github-actions)\n"
        "@.github/workflows/ci.yml -> chart:web (DEPLOYS helm github-actions/test)\n"
        "@.github/workflows/ci.yml -> proc:other.sh (SPAWNS shell github-actions/test)\n"
        "@.github/workflows/ci.yml -> proc:tool (SPAWNS shell github-actions/test)\n"
        "@.github/workflows/ci.yml -> script:hack/e2e.sh (SPAWNS shell github-actions/test)\n"
        "@.github/workflows/ci.yml -> script:hack/lint.sh (SPAWNS shell github-actions/test)\n"
        "@.github/workflows/ci.yml -> script:hack/test.sh (SPAWNS shell github-actions/test)\n"
        "@.github/workflows/ci.yml -> script:ops/apply.sh (SPAWNS shell github-actions/test)\n"
        "@.prow/jobs.yaml -> proc:run-e2e.sh (SPAWNS exec prow/pre-raw)\n"
        "@.prow/jobs.yaml -> script:hack/ci/lint.sh (SPAWNS shell prow/pre-lint)\n"
        "@.prow/jobs.yaml -> script:hack/ci/run-e2e.sh (SPAWNS exec prow/pre-e2e)\n"
        "@hack/e2e.sh -> script:hack/test.sh (SPAWNS shell)\n"
        "@hack/test.sh -> script:hack/lib/helper.sh (SPAWNS shell)\n"
        "@hack/test.sh -> script:hack/lint.sh (SPAWNS shell)\n"
        "test -> proc:bash (SPAWNS shell)\n"
        "test -> script:hack/lint.sh (SPAWNS shell)\n"
        "test -> script:hack/test.sh (SPAWNS shell)");
    PASS();
}

/* The Make model of a project: every Function of a Makefile / .mk as
 * "fn <file>:<name> <start>-<end>[ +variants]" and every IMPORTS edge of one as
 * "import <file> -> <label>:<path> (<local_name>)", sorted. */
static char *gpf_make_model(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce(group_concat(x, char(10)), '') FROM (SELECT x FROM ("
            "SELECT 'fn ' || file_path || ':' || name || ' ' || start_line || '-' || end_line "
            "|| CASE WHEN json_extract(properties,'$.variants') IS NULL THEN '' ELSE "
            "' +variants' END AS x FROM nodes WHERE project=?1 AND label='Function' AND (file_path "
            "LIKE "
            "'%Makefile' OR file_path LIKE '%.mk') UNION ALL SELECT 'import ' || s.file_path || "
            "' -> ' || t.label || ':' || t.file_path || ' (' || "
            "json_extract(e.properties,'$.local_name') || ')' FROM edges e JOIN nodes s ON "
            "s.id=e.source_id JOIN nodes t ON t.id=e.target_id WHERE e.project=?1 AND "
            "e.type='IMPORTS' AND (s.file_path LIKE '%Makefile' OR s.file_path LIKE '%.mk')) "
            "ORDER BY x)";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* A rule naming several targets (`up down:`, `kind-% seed-%:`) is a node per
 * target over the rule's span -- `make down` reaches its own node, and the
 * shared recipe's edges leave from every target. Special targets (.PHONY)
 * and target-specific variable lines (`up: export X := 1`) are none.
 * include / -include import the named files (a name holding a
 * variable stays out), local name the file's base name. */
TEST(probe_make_rules_have_a_node_per_target) {
    static const GpfFile files[] = {{"Makefile", "include mk/tools.mk\n"
                                                 "-include mk/opt.mk $(VAR)/x.mk\n\n"
                                                 ".PHONY: up down\n"
                                                 "up down: deps\n"
                                                 "\t./hack/deploy.sh\n\n"
                                                 "kind-% seed-%:\n"
                                                 "\techo pattern\n\n"
                                                 "single:\n"
                                                 "\techo one\n\n"
                                                 "up kind-%: export MODE := fast\n"
                                                 "only-var: private Y = 1\n"},
                                    {"mk/tools.mk", "tool:\n\techo tool\n"},
                                    {"mk/opt.mk", "opt:\n\techo opt\n"},
                                    {"hack/deploy.sh", "#!/bin/sh\necho deploy\n"},
                                    {"hack/ci.sh", "#!/bin/sh\nmake down\n"}};
    char *got = gpf_make_model(files, 5);
    const char *want = "fn Makefile:down 5-8\n"
                       "fn Makefile:kind-% 8-11\n"
                       "fn Makefile:seed-% 8-11\n"
                       "fn Makefile:single 11-14\n"
                       "fn Makefile:up 5-8\n"
                       "fn mk/opt.mk:opt 1-3\n"
                       "fn mk/tools.mk:tool 1-3";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "make model got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("Make model");
    }
    free(got);
    /* WHY: both literal imports are genuine under the intended root invocation,
     * but no Make invocation/search-domain proof exists. Tried the original
     * files and recipes unchanged; preserve target extraction and SPAWNS below. */
    const RQ_Expected expected[] = {
        {.case_id = "make-literal-mk/tools.mk",
         .site =
             {.file = "Makefile", .caller = "Makefile.__file__", .start_byte = 0, .end_byte = 19},
         .relation = "IMPORTS",
         .target = {.file = "mk/tools.mk",
                    .suffix = "mk.tools.mk.__file__",
                    .label = "File",
                    .declaration_line = 1,
                    .declaration_text = "tool:"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "make_include_invocation_scope_unproven",
         .why = "The root-invoked Make include is real but its invocation directory and "
                "include-search domain are not certified.",
         .tried = "Retained both literal file paths, all seven extracted target ranges, "
                  "dynamic-path rejection and all three SPAWNS contracts; source-authored File "
                  "targets remain counted."},
        {.case_id = "make-literal-mk/opt.mk",
         .site =
             {.file = "Makefile", .caller = "Makefile.__file__", .start_byte = 20, .end_byte = 38},
         .relation = "IMPORTS",
         .target = {.file = "mk/opt.mk",
                    .suffix = "mk.opt.mk.__file__",
                    .label = "File",
                    .declaration_line = 1,
                    .declaration_text = "opt:"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "make_include_invocation_scope_unproven",
         .why = "The root-invoked Make include is real but its invocation directory and "
                "include-search domain are not certified.",
         .tried = "Retained both literal file paths, all seven extracted target ranges, "
                  "dynamic-path rejection and all three SPAWNS contracts; source-authored File "
                  "targets remain counted."}};
    const RQ_Forbidden forbidden[] = {{.case_id = "make-dynamic-path-not-a-literal-target",
                                       .site = {.file = "Makefile",
                                                .caller = "Makefile.__file__",
                                                .start_byte = 39,
                                                .end_byte = 50},
                                       .relation = "IMPORTS",
                                       .any_target = true}};
    ASSERT_TRUE(gpf_infra_quality(files, 5, "probe_make_rules_have_a_node_per_target", expected, 2,
                                  forbidden, 1));
    GPF_ASSERT_RUNS(files, 5,
                    "@hack/ci.sh -> rule:Makefile:down (SPAWNS shell)\n"
                    "down -> script:hack/deploy.sh (SPAWNS shell)\n"
                    "up -> script:hack/deploy.sh (SPAWNS shell)");
    PASS();
}

/* make TARGET SPAWNS the rule of TARGET the makefile reads: the
 * directory's Makefile (-C, --directory=), a -f file, a rule of a file it
 * includes; from scripts, CI steps and recipes ($(MAKE)), on the line of
 * the command (a multi-line variable expanded before it adds no lines). No
 * edge for an unknown target or -C value, a target no rule has, a rule only
 * inside a conditional (no node), or no target. */
TEST(probe_make_targets_spawn_their_rules) {
    static const GpfFile files[] = {{"Makefile", "include mk/common.mk\n"
                                                 "VERSION := 1\n"
                                                 "KEYS = a \\\n"
                                                 "       b \\\n"
                                                 "       c\n\n"
                                                 "build:\n"
                                                 "\tgo build ./...\n\n"
                                                 "test: build\n"
                                                 "\t$(MAKE) lint\n"
                                                 "\t$(MAKE) -C sub check\n\n"
                                                 "ifeq ($(CI),true)\n"
                                                 "cond:\n"
                                                 "\techo ci\n"
                                                 "endif\n\n"
                                                 "keys:\n"
                                                 "\t@for k in $(KEYS); do \\\n"
                                                 "\t  echo $$k; \\\n"
                                                 "\tdone; \\\n"
                                                 "\t$(MAKE) build\n\n"
                                                 "after:\n"
                                                 "\techo one\n"},
                                    {"mk/common.mk", "lint:\n\techo lint\n"},
                                    {"sub/Makefile", "check:\n\techo check\n"},
                                    {"hack/ci.sh", "#!/bin/sh\n"
                                                   "make test VERBOSE=1\n"
                                                   "make -C sub -j 4 check\n"
                                                   "make -f Makefile build unknown-target\n"
                                                   "make \"$TARGET\"\n"
                                                   "make -C \"$DIR\" build\n"
                                                   "make cond\n"
                                                   "make\n"
                                                   "gmake --directory=sub check\n"},
                                    {".github/workflows/ci.yml", "on: [push]\n"
                                                                 "jobs:\n"
                                                                 "  ci:\n"
                                                                 "    runs-on: ubuntu-latest\n"
                                                                 "    steps:\n"
                                                                 "      - run: make -j8 build\n"}};
    GPF_ASSERT_RUNS(
        files, 5,
        "@.github/workflows/ci.yml -> rule:Makefile:build (SPAWNS shell github-actions/ci)\n"
        "@hack/ci.sh -> rule:Makefile:build (SPAWNS shell)\n"
        "@hack/ci.sh -> rule:Makefile:test (SPAWNS shell)\n"
        "@hack/ci.sh -> rule:sub/Makefile:check (SPAWNS shell)\n"
        "keys -> rule:Makefile:build (SPAWNS shell)\n"
        "test -> rule:mk/common.mk:lint (SPAWNS shell)\n"
        "test -> rule:sub/Makefile:check (SPAWNS shell)");
    PASS();
}

/* A Terraform helm_release DEPLOYS its chart: a chart dir of the module
 * (./charts/x, ${path.module}/charts/x), else -- with a repository, or as
 * oci://.../<name> -- the external chart of that name. A variable chart or a
 * directory without a Chart.yaml is no edge; a file opening with comments is
 * read all the same. */
TEST(probe_terraform_helm_release_deploys_its_chart) {
    static const GpfFile files[] = {
        {"infra/charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n"},
        {"infra/charts/api/Chart.yaml", "apiVersion: v2\nname: api\nversion: 0.1.0\n"},
        {"infra/main.tf", "################\n# Helm releases\n################\n\n"
                          "resource \"helm_release\" \"app\" {\n"
                          "  name  = \"app\"\n  chart = \"./charts/app\"\n}\n\n"
                          "resource \"helm_release\" \"api\" {\n"
                          "  name  = \"api\"\n  chart = \"${path.module}/charts/api\"\n"
                          "  set {\n    name  = \"replicas\"\n    value = \"2\"\n  }\n}\n\n"
                          "resource \"helm_release\" \"cache\" {\n"
                          "  name       = \"cache\"\n"
                          "  repository = \"https://charts.example.com\"\n"
                          "  chart      = \"redis\"\n}\n\n"
                          "resource \"helm_release\" \"queue\" {\n"
                          "  name  = \"queue\"\n"
                          "  chart = \"oci://registry.example.com/charts/nats\"\n}\n\n"
                          "resource \"helm_release\" \"dynamic\" {\n"
                          "  name  = \"dynamic\"\n  chart = var.chart\n}\n\n"
                          "resource \"helm_release\" \"missing\" {\n"
                          "  name  = \"missing\"\n  chart = \"./charts/missing\"\n}\n"}};
    GPF_ASSERT_DEPLOYS(files, 3,
                       "resource.helm_release.api -> chart:api (terraform)\n"
                       "resource.helm_release.app -> chart:app (terraform)\n"
                       "resource.helm_release.cache -> chart:redis(ext) (terraform)\n"
                       "resource.helm_release.queue -> chart:nats(ext) (terraform)");
    PASS();
}

/* Go code that loads a chart DEPLOYS it: a call of a loader (a function
 * reading <param>/Chart.yaml, or the Helm SDK's loader.Load) whose argument
 * evaluates over the package's constants, filepath.Join and a local assigned
 * once. Under an unknown root (opt.ChartsDirectory) the tail names the one
 * chart dir ending with it; a tail two dirs end with takes the root the
 * other loads under that root showed. A parameter is not a constant of the
 * same name. */
TEST(probe_go_installer_deploys_charts) {
    static const GpfFile files[] = {
        {"go.mod", "module example.com/inst\n\ngo 1.22\n"},
        {"charts/monitoring/node-exporter/Chart.yaml",
         "apiVersion: v2\nname: node-exporter\nversion: 0.1.0\n"},
        {"charts/minio/Chart.yaml", "apiVersion: v2\nname: minio\nversion: 0.1.0\n"},
        {"charts/mla/minio/Chart.yaml", "apiVersion: v2\nname: mla-minio\nversion: 0.1.0\n"},
        {"charts/telemetry/Chart.yaml", "apiVersion: v2\nname: telemetry\nversion: 0.1.0\n"},
        {"pkg/sdk/testdata/examplechart/Chart.yaml",
         "apiVersion: v2\nname: examplechart\nversion: 0.1.0\n"},
        {"pkg/helm/types.go", "package helm\n\n"
                              "import (\n\t\"os\"\n\t\"path/filepath\"\n)\n\n"
                              "type Chart struct{ Name string }\n\n"
                              "func LoadChart(directory string) (*Chart, error) {\n"
                              "\tf, err := os.Open(filepath.Join(directory, \"Chart.yaml\"))\n"
                              "\tif err != nil {\n\t\treturn nil, err\n\t}\n"
                              "\tdefer f.Close()\n\treturn &Chart{}, nil\n}\n"},
        {"pkg/stack/stack.go",
         "package stack\n\n"
         "import (\n\t\"path/filepath\"\n\n"
         "\t\"example.com/inst/pkg/helm\"\n)\n\n"
         "const (\n"
         "\tMonitoringPrefix      = \"monitoring\"\n"
         "\tNodeExporterChartName = \"node-exporter\"\n"
         "\tMinioChartName        = \"minio\"\n"
         "\tchartName             = \"telemetry\"\n)\n\n"
         "type Options struct{ ChartsDirectory string }\n\n"
         "func deployNodeExporter(opt Options) {\n"
         "\thelm.LoadChart(filepath.Join(opt.ChartsDirectory, "
         "MonitoringPrefix, NodeExporterChartName))\n}\n\n"
         "func deployMinio(opt Options) {\n"
         "\tname := MinioChartName\n"
         "\thelm.LoadChart(filepath.Join(opt.ChartsDirectory, name))\n}\n\n"
         "func deployTelemetry(opt Options) {\n"
         "\thelm.LoadChart(filepath.Join(opt.ChartsDirectory, "
         "\"telemetry\"))\n}\n\n"
         "func deployFromArg(opt Options, chartName string) {\n"
         "\thelm.LoadChart(filepath.Join(opt.ChartsDirectory, chartName))\n}\n"},
        {"pkg/sdk/load.go", "package sdk\n\n"
                            "import \"helm.sh/helm/v3/pkg/chart/loader\"\n\n"
                            "func loadExample() {\n"
                            "\t_, _ = loader.Load(\"testdata/examplechart\")\n}\n"}};
    GPF_ASSERT_DEPLOYS(files, 9,
                       "deployMinio -> chart:minio (helm-sdk)\n"
                       "deployNodeExporter -> chart:node-exporter (helm-sdk)\n"
                       "deployTelemetry -> chart:telemetry (helm-sdk)\n"
                       "loadExample -> chart:examplechart (helm-sdk)");
    PASS();
}

/* A Make recipe runs in its Makefile's directory with the Makefile's
 * variables expanded ($(shell pwd), ?=, a conditional's: unknown): a
 * kustomize build into kubectl, kubectl -f a directory, find -name -exec
 * kubectl apply -f {}. */
TEST(probe_make_recipes_deploy) {
    static const GpfFile files[] = {
        {"operator/config/default/kustomization.yaml", GPF_KUST},
        {"operator/config/prod/kustomization.yaml", GPF_KUST},
        {"operator/config/dev/kustomization.yaml", GPF_KUST},
        {"operator/config/crd/widgets.yaml", GPF_CM("widgets")},
        {"operator/config/flux/flux-a.yaml", GPF_CM("flux-a")},
        {"operator/config/flux/other.yaml", GPF_CM("other")},
        {"operator/config/flux/sub/flux-b.yaml", GPF_CM("flux-b")},
        {"operator/Makefile", "LOCALBIN ?= $(shell pwd)/bin\n"
                              "KUBECTL ?= kubectl\n"
                              "KUSTOMIZE ?= $(LOCALBIN)/kustomize\n"
                              "ifeq ($(ENV),prod)\n"
                              "OVERLAY = config/prod\n"
                              "else\n"
                              "OVERLAY = config/dev\n"
                              "endif\n\n"
                              "deploy:\n"
                              "\t$(KUSTOMIZE) build config/default | $(KUBECTL) apply -f -\n\n"
                              "install:\n"
                              "\t@$(KUBECTL) apply -f config/crd\n\n"
                              "flux:\n"
                              "\tfind ./config/flux -type f -name \"flux-*\" -exec $(KUBECTL) "
                              "apply -f {} \\;\n\n"
                              "overlay:\n"
                              "\t$(KUBECTL) apply -k $(OVERLAY)\n"}};
    GPF_ASSERT_DEPLOYS(files, 8,
                       "deploy -> Resource:operator/config/default/kustomization.yaml (kustomize)\n"
                       "flux -> File:operator/config/flux/flux-a.yaml (kubectl)\n"
                       "flux -> File:operator/config/flux/sub/flux-b.yaml (kubectl)\n"
                       "install -> File:operator/config/crd/widgets.yaml (kubectl)");
    PASS();
}

/* ── Helm symbolic (Phase 2) ────────────────────────────────────────── */

/* Symbolic resource declarations preserve source identity. A volume binding
 * also needs an enabled target and a complete rendering context; expression
 * equality alone does not establish that a conditional ConfigMap exists. */
TEST(probe_helm_templated_objects_are_resources) {
    static const GpfFile files[] = {
        {"chart/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n"},
        {"chart/templates/_helpers.tpl", "{{- define \"app.fullname\" -}}app{{- end }}\n"},
        {"chart/templates/configmap.yaml",
         "{{- if .Values.enabled }}\napiVersion: v1\nkind: ConfigMap\nmetadata:\n"
         "  name: {{ include \"app.fullname\" . }}-config\n"
         "  namespace: {{ .Release.Namespace }}\n  labels:\n"
         "    {{- include \"app.labels\" . | nindent 4 }}\n"
         "data:\n  key: {{ .Values.value | quote }}\n{{- end }}\n"},
        {"chart/templates/deployment.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n"
         "  name: {{ include \"app.fullname\" . }}\nspec:\n  template:\n    spec:\n"
         "      volumes:\n        - name: cfg\n          configMap:\n"
         "            name: {{ include \"app.fullname\" . }}-config\n"}};
    /* WHY: source templates retain their exact named-template call bindings, but
     * an unselected conditional resource is not an available volume target. Tried
     * the unchanged chart plus a separate enabled chart with complete helpers and
     * explicit namespace; both retain a required exact ESM control. */
    const RQ_Expected original_expected[] = {
        {.case_id = "original-deployment-fullname-1",
         .site = {.file = "chart/templates/deployment.yaml",
                  .caller = "chart.templates.deployment.yaml::apps/Deployment/-/<app.fullname>",
                  .start_byte = 58,
                  .end_byte = 82},
         .relation = "CALLS",
         .target = {.file = "chart/templates/_helpers.tpl",
                    .suffix = "chart.templates._helpers.app.fullname",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "{{- define \"app.fullname\" -}}app{{- end }}"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_named_template_site_proof_missing",
         .why = "The literal named template resolves uniquely in this chart family; binding "
                "identity is independent of whether the surrounding conditional is selected. The "
                "include pass lacks immutable occurrence evidence.",
         .tried =
             "Kept every original fixture byte; separately supplied enabled defaults, complete "
             "helper definitions and explicit namespace. Independently anchored declarations and "
             "source occurrences run with both drivers; wrong targets and roles fail."},
        {.case_id = "original-deployment-fullname-2",
         .site = {.file = "chart/templates/deployment.yaml",
                  .caller = "chart.templates.deployment.yaml::apps/Deployment/-/<app.fullname>",
                  .start_byte = 191,
                  .end_byte = 215},
         .relation = "CALLS",
         .target = {.file = "chart/templates/_helpers.tpl",
                    .suffix = "chart.templates._helpers.app.fullname",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "{{- define \"app.fullname\" -}}app{{- end }}"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_named_template_site_proof_missing",
         .why = "The literal named template resolves uniquely in this chart family; binding "
                "identity is independent of whether the surrounding conditional is selected. The "
                "include pass lacks immutable occurrence evidence.",
         .tried =
             "Kept every original fixture byte; separately supplied enabled defaults, complete "
             "helper definitions and explicit namespace. Independently anchored declarations and "
             "source occurrences run with both drivers; wrong targets and roles fail."},
        {.case_id = "original-configmap-fullname-1",
         .site = {.file = "chart/templates/configmap.yaml",
                  .caller =
                      "chart.templates.configmap.yaml::core/ConfigMap/-/<app.fullname>-config",
                  .start_byte = 78,
                  .end_byte = 102},
         .relation = "CALLS",
         .target = {.file = "chart/templates/_helpers.tpl",
                    .suffix = "chart.templates._helpers.app.fullname",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "{{- define \"app.fullname\" -}}app{{- end }}"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_named_template_site_proof_missing",
         .why = "The literal named template resolves uniquely in this chart family; binding "
                "identity is independent of whether the surrounding conditional is selected. The "
                "include pass lacks immutable occurrence evidence.",
         .tried =
             "Kept every original fixture byte; separately supplied enabled defaults, complete "
             "helper definitions and explicit namespace. Independently anchored declarations and "
             "source occurrences run with both drivers; wrong targets and roles fail."}};
    const RQ_Forbidden original_forbidden[] = {
        {.case_id = "unselected-original-configmap-volume",
         .site = {.file = "chart/templates/deployment.yaml",
                  .caller = "chart.templates.deployment.yaml::apps/Deployment/-/<app.fullname>",
                  .start_byte = 188,
                  .end_byte = 225},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "undefined-original-labels-template",
         .site = {.file = "chart/templates/configmap.yaml",
                  .caller =
                      "chart.templates.configmap.yaml::core/ConfigMap/-/<app.fullname>-config",
                  .start_byte = 169,
                  .end_byte = 191},
         .relation = "CALLS",
         .any_target = true}};
    ASSERT_TRUE(gpf_infra_quality(files, 4, "probe_helm_templated_objects_are_resources/original",
                                  original_expected, 3, original_forbidden, 2));
    const GpfFile configured[] = {
        {"configured/chart/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n"},
        {"configured/chart/templates/_helpers.tpl",
         "{{- define \"app.fullname\" -}}app{{- end }}\n{{- define \"app.labels\" -}}app: app{{- "
         "end }}\n"},
        {"configured/chart/templates/configmap.yaml",
         "{{- if .Values.enabled }}\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ "
         "include \"app.fullname\" . }}-config\n  namespace: quality\n  labels:\n    {{- include "
         "\"app.labels\" . | nindent 4 }}\ndata:\n  key: {{ .Values.value | quote }}\n{{- end "
         "}}\n"},
        {"configured/chart/templates/deployment.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: {{ include \"app.fullname\" . "
         "}}\n  namespace: quality\nspec:\n  template:\n    spec:\n      volumes:\n        - name: "
         "cfg\n          configMap:\n            name: {{ include \"app.fullname\" . }}-config\n"},
        {"configured/chart/values.yaml", "enabled: true\nvalue: selected\n"}};
    const RQ_Expected configured_expected[] = {
        {.case_id = "enabled-deployment-fullname-1",
         .site = {.file = "configured/chart/templates/deployment.yaml",
                  .caller = "configured.chart.templates.deployment.yaml::apps/Deployment/quality/"
                            "<app.fullname>",
                  .start_byte = 58,
                  .end_byte = 82},
         .relation = "CALLS",
         .target = {.file = "configured/chart/templates/_helpers.tpl",
                    .suffix = "configured.chart.templates._helpers.app.fullname",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "{{- define \"app.fullname\" -}}app{{- end }}"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_named_template_site_proof_missing",
         .why = "The literal named template resolves uniquely in this chart family; binding "
                "identity is independent of whether the surrounding conditional is selected. The "
                "include pass lacks immutable occurrence evidence.",
         .tried =
             "Kept every original fixture byte; separately supplied enabled defaults, complete "
             "helper definitions and explicit namespace. Independently anchored declarations and "
             "source occurrences run with both drivers; wrong targets and roles fail."},
        {.case_id = "enabled-deployment-fullname-2",
         .site = {.file = "configured/chart/templates/deployment.yaml",
                  .caller = "configured.chart.templates.deployment.yaml::apps/Deployment/quality/"
                            "<app.fullname>",
                  .start_byte = 212,
                  .end_byte = 236},
         .relation = "CALLS",
         .target = {.file = "configured/chart/templates/_helpers.tpl",
                    .suffix = "configured.chart.templates._helpers.app.fullname",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "{{- define \"app.fullname\" -}}app{{- end }}"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_named_template_site_proof_missing",
         .why = "The literal named template resolves uniquely in this chart family; binding "
                "identity is independent of whether the surrounding conditional is selected. The "
                "include pass lacks immutable occurrence evidence.",
         .tried =
             "Kept every original fixture byte; separately supplied enabled defaults, complete "
             "helper definitions and explicit namespace. Independently anchored declarations and "
             "source occurrences run with both drivers; wrong targets and roles fail."},
        {.case_id = "enabled-volume-configmap",
         .site = {.file = "configured/chart/templates/deployment.yaml",
                  .caller = "configured.chart.templates.deployment.yaml::apps/Deployment/quality/"
                            "<app.fullname>",
                  .start_byte = 209,
                  .end_byte = 246},
         .relation = "USAGE",
         .target = {.file = "configured/chart/templates/configmap.yaml",
                    .suffix = "configured.chart.templates.configmap.yaml::core/ConfigMap/quality/"
                              "<app.fullname>-config",
                    .label = "Resource",
                    .declaration_line = 5,
                    .declaration_text = "  name: {{ include \"app.fullname\" . }}-config"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_rendered_resource_binding_proof_missing",
         .why = "Enabled=true and complete helpers render the named ConfigMap in the same explicit "
                "quality namespace; the immutable raw-to-rendered reference bridge is missing.",
         .tried =
             "Kept every original fixture byte; separately supplied enabled defaults, complete "
             "helper definitions and explicit namespace. Independently anchored declarations and "
             "source occurrences run with both drivers; wrong targets and roles fail."},
        {.case_id = "enabled-configmap-fullname-1",
         .site = {.file = "configured/chart/templates/configmap.yaml",
                  .caller = "configured.chart.templates.configmap.yaml::core/ConfigMap/quality/"
                            "<app.fullname>-config",
                  .start_byte = 78,
                  .end_byte = 102},
         .relation = "CALLS",
         .target = {.file = "configured/chart/templates/_helpers.tpl",
                    .suffix = "configured.chart.templates._helpers.app.fullname",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "{{- define \"app.fullname\" -}}app{{- end }}"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_named_template_site_proof_missing",
         .why = "The literal named template resolves uniquely in this chart family; binding "
                "identity is independent of whether the surrounding conditional is selected. The "
                "include pass lacks immutable occurrence evidence.",
         .tried =
             "Kept every original fixture byte; separately supplied enabled defaults, complete "
             "helper definitions and explicit namespace. Independently anchored declarations and "
             "source occurrences run with both drivers; wrong targets and roles fail."},
        {.case_id = "enabled-configmap-labels",
         .site = {.file = "configured/chart/templates/configmap.yaml",
                  .caller = "configured.chart.templates.configmap.yaml::core/ConfigMap/quality/"
                            "<app.fullname>-config",
                  .start_byte = 152,
                  .end_byte = 174},
         .relation = "CALLS",
         .target = {.file = "configured/chart/templates/_helpers.tpl",
                    .suffix = "configured.chart.templates._helpers.app.labels",
                    .label = "Function",
                    .declaration_line = 2,
                    .declaration_text = "{{- define \"app.labels\" -}}app: app{{- end }}"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_named_template_site_proof_missing",
         .why = "The complete configured chart defines app.labels exactly once; its literal call "
                "needs immutable source proof.",
         .tried =
             "Kept every original fixture byte; separately supplied enabled defaults, complete "
             "helper definitions and explicit namespace. Independently anchored declarations and "
             "source occurrences run with both drivers; wrong targets and roles fail."}};
    const RQ_Forbidden configured_forbidden[] = {
        {.case_id = "volume-is-not-template-function",
         .site = {.file = "configured/chart/templates/deployment.yaml",
                  .caller = "configured.chart.templates.deployment.yaml::apps/Deployment/quality/"
                            "<app.fullname>",
                  .start_byte = 209,
                  .end_byte = 246},
         .relation = "USAGE",
         .target = {.file = "configured/chart/templates/_helpers.tpl",
                    .suffix = "configured.chart.templates._helpers.app.fullname",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "{{- define \"app.fullname\" -}}app{{- end }}"}}};
    ASSERT_TRUE(gpf_infra_quality(configured, 5,
                                  "probe_helm_templated_objects_are_resources/enabled",
                                  configured_expected, 5, configured_forbidden, 1));
    PASS();
}

/* Every DEPLOYS edge out of a Chart and CONFIGURES edge into one, as
 * "<source> -> <label>:<target> (<type>[ <ref>])", a File by its path,
 * sorted, one per line. */
static char *gpf_chart_edges(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce(group_concat(x, char(10)), '') FROM (SELECT CASE WHEN "
            "s.label='File' THEN s.file_path ELSE s.name END || ' -> ' || t.label || ':' || "
            "t.name || ' (' || e.type || coalesce(' ' || json_extract(e.properties,'$.ref'), '') "
            "|| coalesce(' ' || json_extract(e.properties,'$.phase'), '') "
            "|| ')' AS x FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
            "t.id=e.target_id WHERE e.project=?1 AND ((s.label='Chart' AND e.type='DEPLOYS') OR "
            "(t.label='Chart' AND e.type='CONFIGURES')) ORDER BY x)";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* A chart deploys what its templates render: Chart -DEPLOYS {via: chart}->
 * each object of its templates/ (a subchart's objects are the subchart's),
 * and each Image those objects use ({ref: image}) -- a templated one rendered
 * from values.yaml (`nginx`: host-less, kept as written). Its values files
 * (values.yaml, values-*.yaml beside Chart.yaml) CONFIGURE it; a values file
 * elsewhere (ci/) does not. */
TEST(probe_helm_chart_deploys_its_objects) {
    static const GpfFile files[] = {
        {"charts/web/Chart.yaml", "apiVersion: v2\nname: web\nversion: 1.0.0\n"},
        {"charts/web/values.yaml", "image:\n  repository: nginx\n  tag: \"1.27\"\nreplicas: 1\n"},
        {"charts/web/values-prod.yaml", "replicas: 3\n"},
        {"charts/web/ci/test-values.yaml", "replicas: 2\n"},
        {"charts/web/templates/deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\nspec:\n"
         "  replicas: {{ .Values.replicas }}\n  template:\n    spec:\n      containers:\n"
         "        - name: web\n"
         "          image: \"{{ .Values.image.repository }}:{{ .Values.image.tag }}\"\n"
         "        - name: side\n          image: ghcr.io/acme/sidecar:1\n"},
        {"charts/web/templates/svc.yaml",
         "apiVersion: v1\nkind: Service\nmetadata:\n  name: web\nspec:\n  selector:\n"
         "    app: web\n"},
        {"charts/web/charts/db/Chart.yaml", "apiVersion: v2\nname: db\nversion: 0.1.0\n"},
        {"charts/web/charts/db/templates/sts.yaml",
         "apiVersion: apps/v1\nkind: StatefulSet\nmetadata:\n  name: db\nspec:\n  template:\n"
         "    spec:\n      containers:\n        - name: db\n"
         "          image: ghcr.io/acme/db:16\n"}};
    char *got = gpf_chart_edges(files, 8);
    static const char want[] = "charts/web/values-prod.yaml -> Chart:web (CONFIGURES)\n"
                               "charts/web/values.yaml -> Chart:web (CONFIGURES)\n"
                               "db -> Image:ghcr.io/acme/db (DEPLOYS image)\n"
                               "db -> Resource:StatefulSet/db (DEPLOYS)\n"
                               "web -> Image:ghcr.io/acme/sidecar (DEPLOYS image)\n"
                               "web -> Image:nginx (DEPLOYS image)\n"
                               "web -> Resource:Deployment/web (DEPLOYS)\n"
                               "web -> Resource:Service/web (DEPLOYS)";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "chart edges got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("Chart DEPLOYS / CONFIGURES edge set");
    }
    free(got);
    PASS();
}

/* A literal in a transformed template still needs the render context: an
 * enabled literal survives, while a false conditional contributes no image. */
TEST(probe_helm_chart_literal_images_follow_rendered_condition) {
    static const GpfFile files[] = {
        {"chart/Chart.yaml", "apiVersion: v2\nname: app\nversion: 1.0.0\n"},
        {"chart/values.yaml", "sidecar: false\n"},
        {"chart/templates/deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: app\nspec:\n"
         "  template:\n    spec:\n      containers:\n        - name: main\n"
         "          image: ghcr.io/acme/main:1\n"
         "{{- if .Values.sidecar }}\n        - name: hidden\n"
         "          image: ghcr.io/acme/hidden:1\n{{- end }}\n"}};
    char *got = gpf_chart_edges(files, 3);
    static const char want[] = "app -> Image:ghcr.io/acme/main (DEPLOYS image)\n"
                               "app -> Resource:Deployment/app (DEPLOYS)\n"
                               "chart/values.yaml -> Chart:app (CONFIGURES)";
    if (!got || strcmp(got, want)) {
        fprintf(stderr, "conditional chart edges got:\n%s\nwant:\n%s\n", got ? got : "(null)",
                want);
        free(got);
        FAIL("Chart literal images must respect rendered conditions");
    }
    free(got);
    PASS();
}

/* A chart also deploys the CRDs of its crds/ ({phase: crds}), which Helm
 * installs before its templates. werf deploys the `.helm` dir beside its
 * werf.yaml as a chart without Chart.yaml, named after the werf project
 * (templated werf.yaml lines before it are skipped); a `.helm` with its own
 * Chart.yaml stays that chart, and a werf.yaml without `.helm` makes none. */
TEST(probe_helm_chart_crds_and_werf_chart) {
    static const GpfFile files[] = {
        {"charts/op/Chart.yaml", "apiVersion: v2\nname: op\nversion: 1.0.0\n"},
        {"charts/op/crds/widget.yaml",
         "apiVersion: apiextensions.k8s.io/v1\nkind: CustomResourceDefinition\nmetadata:\n"
         "  name: widgets.acme.io\nspec:\n  group: acme.io\n  names:\n    kind: Widget\n"
         "    plural: widgets\n  scope: Namespaced\n"},
        {"charts/op/templates/deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: op\n"},
        {"site/werf.yaml", "{{- $_ := set . \"Env\" (.Env | default \"prod\") }}\n\n"
                           "project: site-web\nconfigVersion: 1\n---\nimage: web\nfrom: nginx\n"},
        {"site/.helm/values.yaml", "replicas: 1\n"},
        {"site/.helm/templates/deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: site\nspec:\n"
         "  replicas: {{ .Values.replicas }}\n"},
        {"plain/werf.yaml", "project: plain\nconfigVersion: 1\n"},
        {"own/werf.yaml", "project: own-werf\nconfigVersion: 1\n"},
        {"own/.helm/Chart.yaml", "apiVersion: v2\nname: own\nversion: 0.1.0\n"},
        {"own/.helm/templates/cm.yaml",
         "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: own\n"}};
    char *got = gpf_chart_edges(files, 10);
    static const char want[] =
        "op -> Resource:CustomResourceDefinition/widgets.acme.io (DEPLOYS crds)\n"
        "op -> Resource:Deployment/op (DEPLOYS)\n"
        "own -> Resource:ConfigMap/own (DEPLOYS)\n"
        "site-web -> Resource:Deployment/site (DEPLOYS)\n"
        "site/.helm/values.yaml -> Chart:site-web (CONFIGURES)";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "chart edges got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("crds/ and werf chart edge set");
    }
    free(got);
    PASS();
}

/* Every edge into an Image (or out of one into an Image), one line each:
 * `source -> Image:name TYPE image=.. [via=..] [ref=..] [default_of=..]
 * [override_of=..] [inferred=..] [values_key=..]`, sorted. */
static char *gpf_image_rows(cbm_store_t *store, const char *project, bool without_usage) {
    sqlite3_stmt *st = NULL;
    char *out = NULL;
    const char *sql =
        "SELECT coalesce(group_concat(x, char(10)), '') FROM (SELECT s.label || ':' || s.name "
        "|| ' -> Image:' || t.name || ' ' || e.type || ' image=' || "
        "coalesce(json_extract(e.properties,'$.image'),'-') || "
        "coalesce(' via=' || json_extract(e.properties,'$.via'), '') || "
        "coalesce(' ref=' || json_extract(e.properties,'$.ref'), '') || "
        "coalesce(' default_of=' || json_extract(e.properties,'$.default_of'), '') || "
        "coalesce(' override_of=' || json_extract(e.properties,'$.override_of'), '') || "
        "coalesce(' inferred=' || json_extract(e.properties,'$.inferred'), '') || "
        "coalesce(' values_key=' || json_extract(e.properties,'$.values_key'), '') AS x "
        "FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id "
        "WHERE e.project=?1 AND t.label='Image' AND (?2=0 OR e.type<>'USAGE') ORDER BY x)";
    if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
        sqlite3_bind_int(st, 2, without_usage);
        if (sqlite3_step(st) == SQLITE_ROW)
            out = strdup((const char *)sqlite3_column_text(st, 0));
    }
    sqlite3_finalize(st);
    return out;
}

/* Image is an intentional synthetic repository ontology: pass_docker.c creates
 * __image__<repository> with empty source path and 0/0 lines. Do not fabricate a
 * project declaration for it. Every row below is anchored in authored YAML;
 * exact occurrence multiplicity is separate from its deduplicated graph projection. */
typedef struct {
    const char *case_id, *row, *repository, *image;
    RQ_Site site;
    int declaration_line;
    const char *declaration_text;
    RQ_Mode mode;
    const char *gap_id, *why, *tried;
} GpfImageExpected;

static const GpfFile GPF_IMAGE_LITERAL_CONTROL = {
    "quality/image.yaml", "apiVersion: v1\nkind: Pod\nmetadata:\n  name: quality-image\nspec:\n"
                          "  containers:\n    - name: control\n"
                          "      image: registry.example.test/quality/control:1\n"};
static const GpfImageExpected GPF_IMAGE_LITERAL_REQUIRED = {
    .case_id = "required-literal-pod-repository-image",
    .repository = "registry.example.test/quality/control",
    .image = "registry.example.test/quality/control:1",
    .site = {.file = "quality/image.yaml",
             .caller = "quality.image.yaml::core/Pod/-/quality-image",
             .needle = "registry.example.test/quality/control:1"},
    .declaration_line = 4,
    .declaration_text = "  name: quality-image",
    .mode = RQ_REQUIRED};

/* Remove only the explicitly authored USAGE row from the old exact row set.
 * All DEPLOYS/default/override metadata remains byte-exact and required. */
static char *gpf_image_required_rows(const char *want, const GpfImageExpected *expected,
                                     size_t count) {
    if (!want || !expected || !count || count > 16)
        return NULL;
    bool used[16] = {false};
    char *out = calloc(strlen(want) + 1, 1);
    if (!out)
        return NULL;
    size_t written = 0;
    for (const char *line = want; *line;) {
        size_t length = strcspn(line, "\n");
        size_t match = count;
        for (size_t i = 0; i < count; i++) {
            if (!expected[i].row || strlen(expected[i].row) != length ||
                memcmp(line, expected[i].row, length))
                continue;
            if (used[i]) {
                free(out);
                return NULL;
            }
            match = i;
        }
        if (match != count) {
            if (!strstr(expected[match].row, " USAGE ")) {
                free(out);
                return NULL;
            }
            /* Multiple exact sites can project to this one source/target row.
             * Occurrence identity and multiplicity are verified independently. */
            for (size_t i = 0; i < count; i++)
                if (expected[i].row && !strcmp(expected[i].row, expected[match].row))
                    used[i] = true;
        } else {
            if (written)
                out[written++] = '\n';
            memcpy(out + written, line, length);
            written += length;
        }
        line += length + (line[length] == '\n');
    }
    for (size_t i = 0; i < count; i++)
        if (!used[i]) {
            free(out);
            return NULL;
        }
    return out;
}

/* Exact occurrence, exact synthetic target, persisted projection, and closed
 * caller/role counts. Another target or role cannot satisfy a missing image.
 * Recovered gaps fail until independently reviewed and promoted to required. */
/* Compare emitted occurrence multiplicity separately from the deduplicated
 * graph projection. Every covered caller and exact site remains closed. */
static bool gpf_image_projection(cbm_store_t *store, const char *project,
                                 const GpfImageExpected *expected, const RQ_State *states,
                                 const bool *found, size_t count) {
    sqlite3 *db = cbm_store_get_db(store);
    size_t all_edges = 0, all_occurrences = 0;
    bool ok = true;
    for (size_t i = 0; i < count; i++) {
        size_t caller_edges = 0, caller_occurrences = 0, site_occurrences = 0;
        for (size_t j = 0; j < count; j++) {
            if (!found[j])
                continue;
            bool first_edge = true;
            for (size_t k = 0; k < j; k++)
                first_edge &= !found[k] ||
                              !rq_equal(expected[k].site.caller, expected[j].site.caller) ||
                              !rq_equal(expected[k].repository, expected[j].repository);
            if (!i) {
                all_edges += first_edge;
                all_occurrences++;
            }
            if (rq_equal(expected[i].site.caller, expected[j].site.caller)) {
                caller_occurrences++;
                caller_edges += first_edge;
            }
            site_occurrences += rq_equal(expected[i].site.file, expected[j].site.file) &&
                                states[i].start == states[j].start &&
                                states[i].end == states[j].end;
        }
        const char *sql =
            "SELECT (SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
            "WHERE e.project=?1 AND s.qualified_name=?2 AND e.type IN "
            "('IMPORTS','CALLS','CALL_REFERENCE','USAGE','READS','WRITES','USES_TYPE') "
            /* Kept by their producer's marker, with no occurrence to count here. */
            "AND coalesce(json_extract(e.properties,'$.strategy'),'')<>'helm_include' "
            "AND coalesce(json_extract(e.properties,'$.ref'),'image')='image'),"
            "(SELECT count(*) FROM resolver_occurrences r WHERE r.project=?1 AND r.emitted=1 "
            "AND r.scope=?2),"
            "(SELECT count(*) FROM resolver_occurrences r WHERE r.project=?1 AND r.emitted=1 "
            "AND r.file_path=?3 AND r.start_byte=?4 AND r.end_byte=?5)";
        char scope[1024];
        int length = snprintf(scope, sizeof(scope), "%s.%s", project, expected[i].site.caller);
        if (length < 0 || (size_t)length >= sizeof(scope))
            return false;
        sqlite3_stmt *row = NULL;
        bool checked =
            sqlite3_prepare_v2(db, sql, -1, &row, NULL) == SQLITE_OK &&
            sqlite3_bind_text(row, 1, project, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
            sqlite3_bind_text(row, 2, scope, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
            sqlite3_bind_text(row, 3, expected[i].site.file, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
            sqlite3_bind_int64(row, 4, states[i].start) == SQLITE_OK &&
            sqlite3_bind_int64(row, 5, states[i].end) == SQLITE_OK &&
            sqlite3_step(row) == SQLITE_ROW;
        if (checked)
            checked = sqlite3_column_int64(row, 0) == (sqlite3_int64)caller_edges &&
                      sqlite3_column_int64(row, 1) == (sqlite3_int64)caller_occurrences &&
                      sqlite3_column_int64(row, 2) == (sqlite3_int64)site_occurrences &&
                      sqlite3_step(row) == SQLITE_DONE;
        sqlite3_finalize(row);
        ok &= checked;
    }
    const char *sql = "SELECT (SELECT count(*) FROM edges e JOIN nodes t ON t.id=e.target_id "
                      "WHERE e.project=?1 AND e.type='USAGE' AND t.label='Image'),"
                      "(SELECT count(*) FROM resolver_occurrences r LEFT JOIN nodes t "
                      "ON t.project=r.project AND t.qualified_name=r.target WHERE r.project=?1 "
                      "AND r.emitted=1 AND (t.label='Image' OR substr(r.target,1,9)='__image__'))";
    sqlite3_stmt *row = NULL;
    bool checked = sqlite3_prepare_v2(db, sql, -1, &row, NULL) == SQLITE_OK &&
                   sqlite3_bind_text(row, 1, project, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
                   sqlite3_step(row) == SQLITE_ROW;
    if (checked)
        checked = sqlite3_column_int64(row, 0) == (sqlite3_int64)all_edges &&
                  sqlite3_column_int64(row, 1) == (sqlite3_int64)all_occurrences &&
                  sqlite3_step(row) == SQLITE_DONE;
    sqlite3_finalize(row);
    return ok && checked;
}

static bool gpf_image_occurrences(cbm_store_t *store, const char *project, const char *suite,
                                  const RQ_Source *sources, size_t nsources,
                                  const GpfImageExpected *expected, size_t count) {
    enum { MAX_IMAGES = 16 };
    if (!expected || !count || count > MAX_IMAGES)
        return false;
    RQ_State states[MAX_IMAGES] = {0};
    bool found[MAX_IMAGES] = {false};
    sqlite3 *db = cbm_store_get_db(store);
    RQ_Report report = {.genuine = count};
    if (!rq_generation(db, project))
        report.metadata_errors++;
    const char *sql =
        "SELECT count(*),"
        "(SELECT count(*) FROM nodes n WHERE n.project=?1 AND n.qualified_name=?3 "
        "AND n.file_path=?2 AND n.label='Resource' AND n.start_line<=?9 AND n.end_line>=?9) "
        "FROM resolver_occurrences r "
        "JOIN nodes s ON s.project=r.project AND s.qualified_name=r.scope "
        "JOIN nodes t ON t.project=r.project AND t.qualified_name=r.target "
        "JOIN edges e ON e.project=r.project AND e.source_id=s.id AND e.target_id=t.id "
        "AND e.type=r.relation "
        "WHERE r.project=?1 AND r.file_path=?2 AND r.scope=?3 AND r.start_byte=?4 "
        "AND r.end_byte=?5 AND r.target=?6 AND r.emitted=1 AND r.relation='USAGE' "
        "AND r.source_origin=?10 AND r.domain=?11 AND s.file_path=?2 AND s.label='Resource' "
        "AND s.start_line<=?9 AND s.end_line>=?9 AND t.label='Image' AND t.name=?7 "
        "AND t.file_path='' AND t.start_line=0 AND t.end_line=0 "
        "AND json_extract(e.properties,'$.ref')='image' "
        "AND json_extract(e.properties,'$.image')=?8";
    for (size_t i = 0; i < count; i++) {
        const GpfImageExpected *item = expected + i;
        RQ_State *state = states + i;
        state->site = &item->site;
        RQ_Target owner = {.file = item->site.file,
                           .suffix = item->site.caller,
                           .label = "Resource",
                           .declaration_line = item->declaration_line,
                           .declaration_text = item->declaration_text};
        bool anchor = rq_anchor(state, sources, nsources);
        bool unique = true;
        for (size_t j = 0; j < i; j++) {
            bool same_site = rq_equal(item->site.file, expected[j].site.file) &&
                             state->start == states[j].start && state->end == states[j].end;
            unique &= !rq_equal(item->case_id, expected[j].case_id) &&
                      (!same_site || (rq_equal(item->site.caller, expected[j].site.caller) &&
                                      !rq_equal(item->repository, expected[j].repository)));
        }
        bool mode = item->mode == RQ_REQUIRED || item->mode == RQ_KNOWN_GAP;
        bool gap = item->mode != RQ_KNOWN_GAP ||
                   (rq_text(item->gap_id) && rq_text(item->why) && rq_text(item->tried));
        if (!unique || !anchor || !mode || !gap || !rq_text(item->case_id) ||
            !rq_text(item->repository) || !rq_text(item->image) ||
            !rq_target_anchor(&owner, sources, nsources)) {
            report.invalid_oracle++;
            continue;
        }
        const RQ_Source *source = rq_source_input(sources, nsources, item->site.file);
        if (!source || !rq_snapshot(db, project, source)) {
            report.metadata_errors++;
            continue;
        }
        char scope[1024], target[1024];
        int scope_length = snprintf(scope, sizeof(scope), "%s.%s", project, item->site.caller);
        int target_length = snprintf(target, sizeof(target), "__image__%s", item->repository);
        if (scope_length < 0 || (size_t)scope_length >= sizeof(scope) || target_length < 0 ||
            (size_t)target_length >= sizeof(target)) {
            report.invalid_oracle++;
            continue;
        }
        sqlite3_stmt *row = NULL;
        bool ok = sqlite3_prepare_v2(db, sql, -1, &row, NULL) == SQLITE_OK &&
                  sqlite3_bind_text(row, 1, project, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
                  sqlite3_bind_text(row, 2, item->site.file, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
                  sqlite3_bind_text(row, 3, scope, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
                  sqlite3_bind_int64(row, 4, state->start) == SQLITE_OK &&
                  sqlite3_bind_int64(row, 5, state->end) == SQLITE_OK &&
                  sqlite3_bind_text(row, 6, target, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
                  sqlite3_bind_text(row, 7, item->repository, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
                  sqlite3_bind_text(row, 8, item->image, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
                  sqlite3_bind_int(row, 9, state->line) == SQLITE_OK &&
                  sqlite3_bind_int(row, 10, CBM_SOURCE_ORIGIN_RAW) == SQLITE_OK &&
                  sqlite3_bind_int(row, 11, CBM_RESOLVE_DOMAIN_PROJECT) == SQLITE_OK &&
                  sqlite3_step(row) == SQLITE_ROW;
        found[i] = ok && sqlite3_column_int(row, 0) == 1;
        if (ok) {
            report.wrong += sqlite3_column_int(row, 0) > 1;
            report.metadata_errors += sqlite3_column_int(row, 1) != 1;
            ok = sqlite3_step(row) == SQLITE_DONE;
        }
        sqlite3_finalize(row);
        report.metadata_errors += !ok;
        report.found += found[i];
        report.missing += !found[i];
        report.required_missing += !found[i] && item->mode == RQ_REQUIRED;
        report.recovered_gaps += found[i] && item->mode == RQ_KNOWN_GAP;
        if (!found[i])
            fprintf(stderr,
                    "[IMAGE_RQ] QUALITY_MISS case=%s gap=%s file=%s target=%s WHY=%s TRIED=%s\n",
                    item->case_id, item->gap_id ? item->gap_id : "required", item->site.file,
                    target, item->why ? item->why : "required exact literal repository image",
                    item->tried ? item->tried : "independent raw Pod source control");
    }
    if (!report.invalid_oracle && !report.metadata_errors)
        report.wrong += !gpf_image_projection(store, project, expected, states, found, count);
    fprintf(stderr,
            "[IMAGE_RQ] %s genuine=%zu found=%zu missing=%zu wrong=%zu required_missing=%zu "
            "recovered_gaps=%zu metadata_errors=%zu invalid_oracle=%zu\n",
            suite, report.genuine, report.found, report.missing, report.wrong,
            report.required_missing, report.recovered_gaps, report.metadata_errors,
            report.invalid_oracle);
    return !report.wrong && !report.required_missing && !report.recovered_gaps &&
           !report.metadata_errors && !report.invalid_oracle;
}

static bool gpf_image_quality(const GpfFile *files, size_t nfiles, const char *suite,
                              const char *want, const GpfImageExpected *expected, size_t count) {
    enum { MAX_FILES = 24, MAX_IMAGES = 16, PADDING = 56 };
    if (!expected || !count || count >= MAX_IMAGES || nfiles > MAX_FILES)
        return false;
    char *required = gpf_image_required_rows(want, expected, count);
    if (!required) {
        free(required);
        return false;
    }
    GpfFile indexed[MAX_FILES + 2 + PADDING];
    RQ_Source sources[MAX_FILES + 2];
    GpfImageExpected goldens[MAX_IMAGES];
    char names[PADDING][48];
    for (size_t i = 0; i < nfiles; i++) {
        indexed[i] = files[i];
        sources[i] = (RQ_Source){.path = files[i].name, .text = files[i].content};
    }
    indexed[nfiles] = GPF_IMAGE_LITERAL_CONTROL;
    sources[nfiles] = (RQ_Source){.path = GPF_IMAGE_LITERAL_CONTROL.name,
                                  .text = GPF_IMAGE_LITERAL_CONTROL.content};
    indexed[nfiles + 1] = GPF_IMPORT_ECMA_CONTROL;
    sources[nfiles + 1] =
        (RQ_Source){.path = GPF_IMPORT_ECMA_CONTROL.name, .text = GPF_IMPORT_ECMA_CONTROL.content};
    for (size_t i = 0; i < count; i++)
        goldens[i] = expected[i];
    goldens[count] = GPF_IMAGE_LITERAL_REQUIRED;
    bool ok = true;
    for (int driver = 0; driver < 2; driver++) {
        size_t total = nfiles + 2;
        for (int i = 0; driver && i < PADDING; i++) {
            snprintf(names[i], sizeof(names[i]), "quality_padding/image_%02d.js", i);
            indexed[total++] = (GpfFile){names[i], "export const padding = 1;\n"};
        }
        GpfProj lp;
        cbm_store_t *store = gpf_index_files(&lp, indexed, (int)total);
        char label[192];
        snprintf(label, sizeof(label), "%s/%s", suite, driver ? "parallel" : "sequential");
        if (!store) {
            ok = false;
            gpf_cleanup(&lp, store);
            continue;
        }
        char *actual = gpf_image_rows(store, lp.project, true);
        bool rows = actual && !strcmp(actual, required);
        if (!rows)
            fprintf(stderr, "[IMAGE_RQ] %s required deployment rows got:\n%s\nwant:\n%s\n", label,
                    actual ? actual : "(null)", required);
        free(actual);
        bool images = gpf_image_occurrences(store, lp.project, label, sources, nfiles + 2, goldens,
                                            count + 1);
        RQ_Report control;
        bool calls = rq_check(store, lp.project, label, sources + nfiles + 1, 1,
                              &GPF_IMPORT_ECMA_REQUIRED, 1, NULL, 0, &control, stderr);
        ok &= rows && images && calls;
        gpf_cleanup(&lp, store);
    }
    free(required);
    return ok;
}

/* Two exact uses of one repository yield two occurrences and one graph edge. */
static bool gpf_image_multisite_quality(const GpfFile *files, size_t nfiles, const char *suite,
                                        const char *want, const GpfImageExpected *expected,
                                        size_t count) {
    if (nfiles > 23 || count > 12)
        return false;
    GpfFile configured[24];
    GpfImageExpected goldens[15];
    for (size_t i = 0; i < nfiles; i++)
        configured[i] = files[i];
    configured[nfiles] = (GpfFile){
        "quality/repeat.yaml",
        "apiVersion: v1\nkind: Pod\nmetadata:\n  name: quality-repeat\nspec:\n  containers:\n    - "
        "name: first\n      image: registry.example.test/quality/repeat:1\n    - name: second\n    "
        "  image: registry.example.test/quality/repeat:1\n"};
    for (size_t i = 0; i < count; i++)
        goldens[i] = expected[i];
    goldens[count + 0] = (GpfImageExpected){
        .case_id = "required-repeat-site-1",
        .row = "Resource:Pod/quality-repeat -> Image:registry.example.test/quality/repeat USAGE "
               "image=registry.example.test/quality/repeat:1 ref=image",
        .repository = "registry.example.test/quality/repeat",
        .image = "registry.example.test/quality/repeat:1",
        .site = {.file = "quality/repeat.yaml",
                 .caller = "quality.repeat.yaml::core/Pod/-/quality-repeat",
                 .needle = "registry.example.test/quality/repeat:1",
                 .occurrence_index = 0},
        .declaration_line = 4,
        .declaration_text = "  name: quality-repeat",
        .mode = RQ_REQUIRED};
    goldens[count + 1] = (GpfImageExpected){
        .case_id = "required-repeat-site-2",
        .row = "Resource:Pod/quality-repeat -> Image:registry.example.test/quality/repeat USAGE "
               "image=registry.example.test/quality/repeat:1 ref=image",
        .repository = "registry.example.test/quality/repeat",
        .image = "registry.example.test/quality/repeat:1",
        .site = {.file = "quality/repeat.yaml",
                 .caller = "quality.repeat.yaml::core/Pod/-/quality-repeat",
                 .needle = "registry.example.test/quality/repeat:1",
                 .occurrence_index = 1},
        .declaration_line = 4,
        .declaration_text = "  name: quality-repeat",
        .mode = RQ_REQUIRED};

    static const char projection[] =
        "Resource:Pod/quality-repeat -> Image:registry.example.test/quality/repeat USAGE "
        "image=registry.example.test/quality/repeat:1 ref=image";
    size_t length = strlen(want) + strlen(projection) + 2;
    char *rows = malloc(length);
    if (!rows)
        return false;
    snprintf(rows, length, "%s\n%s", want, projection);
    bool ok = gpf_image_quality(configured, nfiles + 1, suite, rows, goldens, count + 2);
    free(rows);
    return ok;
}

static bool gpf_component_images_quality(const GpfFile *files, size_t nfiles) {
    const GpfImageExpected expected[] = {
        {.case_id = "compose-repository-a",
         .row = "Resource:Service/a -> Image:acme/demo USAGE image=acme/demo:1.0-a ref=image",
         .repository = "acme/demo",
         .image = "acme/demo:1.0-a",
         .site = {.file = "compose.yaml",
                  .caller = "compose::compose/Service/-/a",
                  .start_byte = 26,
                  .end_byte = 42},
         .declaration_line = 2,
         .declaration_text = "  a:",
         .mode = RQ_REQUIRED},
        {.case_id = "compose-repository-b",
         .row = "Resource:Service/b -> Image:acme/demo USAGE image=acme/demo:1.0-b ref=image",
         .repository = "acme/demo",
         .image = "acme/demo:1.0-b",
         .site = {.file = "compose.yaml",
                  .caller = "compose::compose/Service/-/b",
                  .start_byte = 78,
                  .end_byte = 94},
         .declaration_line = 5,
         .declaration_text = "  b:",
         .mode = RQ_REQUIRED},
        {.case_id = "compose-repository-c",
         .row = "Resource:Service/c -> Image:acme/demo USAGE image=acme/demo:1.0-c ref=image",
         .repository = "acme/demo",
         .image = "acme/demo:1.0-c",
         .site = {.file = "compose.yaml",
                  .caller = "compose::compose/Service/-/c",
                  .start_byte = 130,
                  .end_byte = 146},
         .declaration_line = 8,
         .declaration_text = "  c:",
         .mode = RQ_REQUIRED},
        {.case_id = "compose-repository-solo",
         .row = "Resource:Service/solo -> Image:acme/solo USAGE image=acme/solo:1 ref=image",
         .repository = "acme/solo",
         .image = "acme/solo:1",
         .site = {.file = "compose.yaml",
                  .caller = "compose::compose/Service/-/solo",
                  .start_byte = 185,
                  .end_byte = 196},
         .declaration_line = 11,
         .declaration_text = "  solo:",
         .mode = RQ_REQUIRED},
        {.case_id = "compose-repository-tool",
         .row = "Resource:Service/tool -> Image:acme/tool USAGE image=acme/tool ref=image",
         .repository = "acme/tool",
         .image = "acme/tool",
         .site = {.file = "compose.yaml",
                  .caller = "compose::compose/Service/-/tool",
                  .start_byte = 234,
                  .end_byte = 243},
         .declaration_line = 14,
         .declaration_text = "  tool:",
         .mode = RQ_REQUIRED},
        {.case_id = "compose-repository-tooltest",
         .row = "Resource:Service/tooltest -> Image:acme/tool USAGE image=acme/tool:test ref=image",
         .repository = "acme/tool",
         .image = "acme/tool:test",
         .site = {.file = "compose.yaml",
                  .caller = "compose::compose/Service/-/tooltest",
                  .start_byte = 285,
                  .end_byte = 299},
         .declaration_line = 17,
         .declaration_text = "  tooltest:",
         .mode = RQ_REQUIRED},
        {.case_id = "compose-repository-w1",
         .row = "Resource:Service/w1 -> Image:acme/x USAGE image=acme/x:1-w ref=image",
         .repository = "acme/x",
         .image = "acme/x:1-w",
         .site = {.file = "compose.yaml",
                  .caller = "compose::compose/Service/-/w1",
                  .start_byte = 384,
                  .end_byte = 394},
         .declaration_line = 22,
         .declaration_text = "  w1:",
         .mode = RQ_REQUIRED},
        {.case_id = "compose-repository-w2",
         .row = "Resource:Service/w2 -> Image:acme/x USAGE image=acme/x:1-w ref=image",
         .repository = "acme/x",
         .image = "acme/x:1-w",
         .site = {.file = "compose.yaml",
                  .caller = "compose::compose/Service/-/w2",
                  .start_byte = 428,
                  .end_byte = 438},
         .declaration_line = 25,
         .declaration_text = "  w2:",
         .mode = RQ_REQUIRED},
        {.case_id = "compose-repository-v",
         .row = "Resource:Service/v -> Image:acme/x USAGE image=acme/x:1-v ref=image",
         .repository = "acme/x",
         .image = "acme/x:1-v",
         .site = {.file = "compose.yaml",
                  .caller = "compose::compose/Service/-/v",
                  .start_byte = 519,
                  .end_byte = 529},
         .declaration_line = 30,
         .declaration_text = "  v:",
         .mode = RQ_REQUIRED},
        {.case_id = "compose-repository-p",
         .row = "Resource:Service/p -> Image:acme/y USAGE image=acme/y:1-p ref=image",
         .repository = "acme/y",
         .image = "acme/y:1-p",
         .site = {.file = "compose.yaml",
                  .caller = "compose::compose/Service/-/p",
                  .start_byte = 561,
                  .end_byte = 571},
         .declaration_line = 33,
         .declaration_text = "  p:",
         .mode = RQ_REQUIRED},
        {.case_id = "compose-repository-q",
         .row = "Resource:Service/q -> Image:acme/y USAGE image=acme/y:1-q ref=image",
         .repository = "acme/y",
         .image = "acme/y:1-q",
         .site = {.file = "compose.yaml",
                  .caller = "compose::compose/Service/-/q",
                  .start_byte = 603,
                  .end_byte = 613},
         .declaration_line = 36,
         .declaration_text = "  q:",
         .mode = RQ_REQUIRED},
        {.case_id = "workload-repository-b",
         .row = "Resource:Deployment/b -> Image:acme/demo USAGE image=acme/demo:0.9-b ref=image",
         .repository = "acme/demo",
         .image = "acme/demo:0.9-b",
         .site = {.file = "k8s.yaml",
                  .caller = "k8s.yaml::apps/Deployment/-/b",
                  .needle = "acme/demo:0.9-b"},
         .declaration_line = 4,
         .declaration_text = "  name: b",
         .mode = RQ_REQUIRED},
        {.case_id = "workload-repository-z",
         .row = "Resource:Deployment/z -> Image:acme/demo USAGE image=acme/demo:0.9-zzz ref=image",
         .repository = "acme/demo",
         .image = "acme/demo:0.9-zzz",
         .site = {.file = "k8s.yaml",
                  .caller = "k8s.yaml::apps/Deployment/-/z",
                  .needle = "acme/demo:0.9-zzz"},
         .declaration_line = 15,
         .declaration_text = "  name: z",
         .mode = RQ_REQUIRED},
        {.case_id = "workload-repository-s",
         .row = "Resource:Deployment/s -> Image:acme/solo USAGE image=acme/solo:2 ref=image",
         .repository = "acme/solo",
         .image = "acme/solo:2",
         .site = {.file = "k8s.yaml",
                  .caller = "k8s.yaml::apps/Deployment/-/s",
                  .needle = "acme/solo:2"},
         .declaration_line = 26,
         .declaration_text = "  name: s",
         .mode = RQ_REQUIRED}};
    static const char image_rows[] =
        "Module:p/Dockerfile -> Image:alpine DEPENDS_ON image=alpine:3.20 "
        "via=from\nModule:q/Dockerfile -> Image:alpine DEPENDS_ON image=alpine:3.20 "
        "via=from\nModule:solo/Dockerfile -> Image:alpine DEPENDS_ON image=alpine:3.20 "
        "via=from\nModule:svc/a/Dockerfile -> Image:alpine DEPENDS_ON image=alpine:3.20 "
        "via=from\nModule:svc/b/Dockerfile -> Image:alpine DEPENDS_ON image=alpine:3.20 "
        "via=from\nModule:svc/c/Dockerfile -> Image:alpine DEPENDS_ON image=alpine:3.20 "
        "via=from\nModule:tool/Dockerfile -> Image:alpine DEPENDS_ON image=alpine:3.20 "
        "via=from\nModule:tool/Dockerfile.test -> Image:alpine DEPENDS_ON image=alpine:3.20 "
        "via=from\nModule:v/Dockerfile -> Image:alpine DEPENDS_ON image=alpine:3.20 "
        "via=from\nModule:w1/Dockerfile -> Image:alpine DEPENDS_ON image=alpine:3.20 "
        "via=from\nModule:w2/Dockerfile.alt -> Image:alpine DEPENDS_ON image=alpine:3.20 "
        "via=from\nResource:Deployment/b -> Image:acme/demo USAGE image=acme/demo:0.9-b "
        "ref=image\nResource:Deployment/s -> Image:acme/solo USAGE image=acme/solo:2 "
        "ref=image\nResource:Deployment/z -> Image:acme/demo USAGE image=acme/demo:0.9-zzz "
        "ref=image\nResource:Service/a -> Image:acme/demo USAGE image=acme/demo:1.0-a "
        "ref=image\nResource:Service/b -> Image:acme/demo USAGE image=acme/demo:1.0-b "
        "ref=image\nResource:Service/c -> Image:acme/demo USAGE image=acme/demo:1.0-c "
        "ref=image\nResource:Service/p -> Image:acme/y USAGE image=acme/y:1-p "
        "ref=image\nResource:Service/q -> Image:acme/y USAGE image=acme/y:1-q "
        "ref=image\nResource:Service/solo -> Image:acme/solo USAGE image=acme/solo:1 "
        "ref=image\nResource:Service/tool -> Image:acme/tool USAGE image=acme/tool "
        "ref=image\nResource:Service/tooltest -> Image:acme/tool USAGE image=acme/tool:test "
        "ref=image\nResource:Service/v -> Image:acme/x USAGE image=acme/x:1-v "
        "ref=image\nResource:Service/w1 -> Image:acme/x USAGE image=acme/x:1-w "
        "ref=image\nResource:Service/w2 -> Image:acme/x USAGE image=acme/x:1-w ref=image";
    return gpf_image_quality(files, nfiles, "probe_image_hub_splits_by_component", image_rows,
                             expected, 14);
}

/* The original short digest is invalid, and a custom Widget's arbitrary
 * containers field establishes no Kubernetes runtime-image semantics. Preserve
 * both sources as negatives; exercise the same digest feature with a valid Pod.
 * The image oracle closes every Image edge/occurrence, including those negatives. */
static bool gpf_builtin_images_quality(const GpfFile *original) {
    const GpfFile configured[] = {
        original[0],
        {"quality/digest.yaml",
         "apiVersion: v1\nkind: Pod\nmetadata:\n  name: digest-control\nspec:\n  containers:\n    "
         "- name: init\n      image: "
         "busybox@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n"}};
    const GpfImageExpected expected[] = {
        {.case_id = "required-original-deployment-literal",
         .repository = "nginx",
         .image = "nginx:1.27",
         .site = {.file = "app.yaml",
                  .caller = "app.yaml::apps/Deployment/-/web",
                  .needle = "nginx:1.27"},
         .declaration_line = 4,
         .declaration_text = "  name: web",
         .mode = RQ_REQUIRED,
         .row = "Resource:Deployment/web -> Image:nginx USAGE image=nginx:1.27 ref=image"},
        {.case_id = "required-original-cronjob-literal",
         .repository = "ghcr.io/acme/job",
         .image = "ghcr.io/acme/job:2",
         .site = {.file = "app.yaml",
                  .caller = "app.yaml::batch/CronJob/-/nightly",
                  .needle = "ghcr.io/acme/job:2"},
         .declaration_line = 20,
         .declaration_text = "  name: nightly",
         .mode = RQ_REQUIRED,
         .row = "Resource:CronJob/nightly -> Image:ghcr.io/acme/job USAGE image=ghcr.io/acme/job:2 "
                "ref=image"},
        {.case_id = "required-valid-digest-counterpart",
         .repository = "busybox",
         .image = "busybox@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
         .site = {.file = "quality/digest.yaml",
                  .caller = "quality.digest.yaml::core/Pod/-/digest-control",
                  .needle = "busybox@sha256:"
                            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
         .declaration_line = 4,
         .declaration_text = "  name: digest-control",
         .mode = RQ_REQUIRED,
         .row = "Resource:Pod/digest-control -> Image:busybox USAGE "
                "image=busybox@sha256:"
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef ref=image"}};
    static const char rows[] =
        "Resource:Deployment/web -> Image:nginx USAGE image=nginx:1.27 "
        "ref=image\nResource:CronJob/nightly -> Image:ghcr.io/acme/job USAGE "
        "image=ghcr.io/acme/job:2 ref=image\nResource:Pod/digest-control -> Image:busybox USAGE "
        "image=busybox@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef "
        "ref=image";
    return gpf_image_quality(configured, 2, "probe_k8s_container_images_are_images", rows, expected,
                             3);
}

/* An image a default value names keeps its Image, its edge marked
 * default_of with the ARGs / variables whose default supplied it (an ARG
 * chain names each); a literal FROM is unmarked. A build's arguments (compose
 * build.args, docker build --build-arg) make the Image it builds DEPEND_ON
 * the base they choose {override_of}; compose's .env replacing an inline
 * default is the effective edge's override_of, with the inactive default kept
 * as metadata. Only the selected effective image has a runtime USAGE edge. */
TEST(probe_docker_default_images_marked_and_overrides) {
    static const GpfFile files[] = {
        {"src/web/Dockerfile", "ARG GO=1.22\nARG BASE=golang:${GO}\nFROM ${BASE} AS build\n"
                               "RUN go build -o /app .\nFROM alpine:3.20\n"
                               "COPY --from=build /app /app\n"},
        {"src/api/Dockerfile", "ARG PY=3.12\nFROM python:${PY}-slim\n"},
        {".env", "REG=ghcr.io/acme\nAPI_IMG=ghcr.io/acme/api-custom\n"},
        {"compose.yaml", "services:\n  web:\n    image: ${REG}/web:${TAG:-dev}\n    build:\n"
                         "      context: ./src/web\n      args:\n        GO: \"1.23\"\n"
                         "  api:\n    image: ${API_IMG:-ghcr.io/acme/api}\n    build: ./src/api\n"},
        {"scripts/build.sh", "#!/bin/sh\nIMAGE=${IMAGE:-ghcr.io/acme/api-ci}\n"
                             "docker build --build-arg PY=3.13 -t \"$IMAGE:1\" src/api\n"}};
    static const char want[] =
        "Image:ghcr.io/acme/api-ci -> Image:python DEPENDS_ON image=python:3.13-slim via=from "
        "override_of=[\"PY\"]\n"
        "Image:ghcr.io/acme/web -> Image:golang DEPENDS_ON image=golang:1.23 via=from "
        "default_of=[\"BASE\"] override_of=[\"GO\"]\n"
        "Module:src/api/Dockerfile -> Image:python DEPENDS_ON image=python:3.12-slim via=from "
        "default_of=[\"PY\"]\n"
        "Module:src/web/Dockerfile -> Image:alpine DEPENDS_ON image=alpine:3.20 via=from\n"
        "Module:src/web/Dockerfile -> Image:golang DEPENDS_ON image=golang:1.22 via=from "
        "default_of=[\"GO\",\"BASE\"]\n"
        "Resource:Service/api -> Image:ghcr.io/acme/api-custom USAGE image=ghcr.io/acme/api-custom "
        "ref=image override_of=[\"API_IMG\"]\n"
        "Resource:Service/web -> Image:ghcr.io/acme/web USAGE image=ghcr.io/acme/web:dev "
        "ref=image default_of=[\"TAG\"]";
    /* The .env override selects api-custom. Its inactive inline default stays
     * metadata, never a second runtime USAGE. Both effective bindings are supported
     * and required; the closed Image domain forbids the inactive default edge. */
    const GpfImageExpected expected[] = {
        {.case_id = "compose-selected-api-environment",
         .row = "Resource:Service/api -> Image:ghcr.io/acme/api-custom USAGE "
                "image=ghcr.io/acme/api-custom ref=image override_of=[\"API_IMG\"]",
         .repository = "ghcr.io/acme/api-custom",
         .image = "ghcr.io/acme/api-custom",
         .site = {.file = "compose.yaml",
                  .caller = "compose::compose/Service/-/api",
                  .needle = "${API_IMG:-ghcr.io/acme/api}"},
         .declaration_line = 8,
         .declaration_text = "  api:",
         .mode = RQ_REQUIRED},
        {.case_id = "compose-selected-web-environment-and-default",
         .row = "Resource:Service/web -> Image:ghcr.io/acme/web USAGE image=ghcr.io/acme/web:dev "
                "ref=image default_of=[\"TAG\"]",
         .repository = "ghcr.io/acme/web",
         .image = "ghcr.io/acme/web:dev",
         .site = {.file = "compose.yaml",
                  .caller = "compose::compose/Service/-/web",
                  .needle = "${REG}/web:${TAG:-dev}"},
         .declaration_line = 2,
         .declaration_text = "  web:",
         .mode = RQ_REQUIRED}};
    if (!gpf_image_quality(files, 5, "probe_docker_default_images_marked_and_overrides", want,
                           expected, 2)) {
        FAIL("default-valued images and their in-repo overrides");
    }
    ASSERT_EQ(
        gpf_query_int(files, 5,
                      "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t "
                      "ON t.id=e.target_id WHERE e.project=?1 AND s.name='Service/api' AND "
                      "t.name='ghcr.io/acme/api-custom' AND e.type='USAGE' AND "
                      "json_extract(e.properties,'$.default_image')='ghcr.io/acme/api' AND "
                      "json_extract(e.properties,'$.override_source')='.env'"),
        1);
    PASS();
}

/* The chart a Helm template renders (helm_template.c): the container image
 * from the chart family's defaults -- an include helper (bitnami
 * common.images.image, a dict argument, .Chart.AppVersion), a `with`
 * binding, the taken branch of an if / else, a subchart under its alias with
 * the parent's values and global. The object USES it {default_of: the values
 * keys read}; the chart DEPLOYS it. A value that does not render -- a
 * .Release field, lookup, a missing define, a field of a missing map -- names
 * no Image. */
TEST(probe_helm_template_images_render_with_defaults) {
    static const GpfFile files[] = {
        {"charts/app/Chart.yaml",
         "apiVersion: v2\nname: app\nversion: 1.2.3\nappVersion: \"2.0.1\"\ndependencies:\n"
         "  - name: db\n    version: 0.1.0\n    alias: database\n"},
        {"charts/app/values.yaml",
         "image:\n  registry: ghcr.io\n  repository: acme/app\n  tag: \"\"\n  digest: \"\"\n"
         "sidecar:\n  image: { repository: docker.io/library/busybox, tag: 1.36 }\nglobal: {}\n"
         "database:\n  image:\n    tag: \"17\"\n"},
        {"charts/app/templates/_helpers.tpl",
         "{{- define \"common.images.image\" -}}\n"
         "{{- $registryName := default .imageRoot.registry ((.global).imageRegistry) -}}\n"
         "{{- $termination := .imageRoot.tag | toString -}}\n"
         "{{- if not .imageRoot.tag }}\n  {{- if .chart }}\n"
         "    {{- $termination = .chart.AppVersion | toString -}}\n  {{- end -}}\n{{- end -}}\n"
         "{{- if $registryName }}\n"
         "{{- printf \"%s/%s:%s\" $registryName .imageRoot.repository $termination -}}\n"
         "{{- else -}}\n{{- printf \"%s:%s\" .imageRoot.repository $termination -}}\n"
         "{{- end -}}\n{{- end -}}\n"},
        {"charts/app/templates/deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: app\nspec:\n  template:\n"
         "    spec:\n      containers:\n        - name: app\n          image: {{ include "
         "\"common.images.image\" (dict \"imageRoot\" .Values.image \"global\" .Values.global "
         "\"chart\" .Chart) }}\n        - name: side\n          {{- with .Values.sidecar.image }}\n"
         "          image: \"{{ .repository }}:{{ .tag }}\"\n          {{- end }}\n"
         "        - name: rel\n          image: \"ghcr.io/acme/{{ .Release.Name }}:1\"\n"
         "        - name: miss\n          image: {{ include \"nope\" . }}\n"
         "        - name: deep\n          image: \"{{ .Values.absent.repository }}\"\n"},
        {"charts/app/charts/db/Chart.yaml",
         "apiVersion: v2\nname: db\nversion: 0.1.0\nappVersion: \"16.4\"\n"},
        {"charts/app/charts/db/values.yaml",
         "image:\n  repository: bitnami/postgresql\n  tag: \"\"\n  digest: \"\"\n"},
        {"charts/app/charts/db/templates/sts.yaml",
         "apiVersion: apps/v1\nkind: StatefulSet\nmetadata:\n  name: db\nspec:\n  template:\n"
         "    spec:\n      containers:\n        - name: db\n          {{- if .Values.image.digest "
         "}}\n"
         "          image: \"{{ .Values.global.imageRegistry | default \"docker.io\" }}/"
         "{{ .Values.image.repository }}@{{ .Values.image.digest }}\"\n          {{- else }}\n"
         "          image: \"{{ .Values.global.imageRegistry | default \"docker.io\" }}/"
         "{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}\"\n"
         "          {{- end }}\n"}};
    static const char want[] =
        "Chart:app -> Image:docker.io/library/busybox DEPLOYS image=- via=chart ref=image "
        "default_of=[\"sidecar.image.repository\",\"sidecar.image.tag\"]\n"
        "Chart:app -> Image:ghcr.io/acme/app DEPLOYS image=- via=chart ref=image "
        "default_of=[\"image.registry\",\"global.imageRegistry\",\"image.tag\","
        "\"image.repository\"]\n"
        "Chart:db -> Image:docker.io/bitnami/postgresql DEPLOYS image=- via=chart ref=image "
        "default_of=[\"global.imageRegistry\",\"image.repository\",\"image.tag\"]\n"
        "Resource:Deployment/app -> Image:docker.io/library/busybox USAGE "
        "image=docker.io/library/busybox:1.36 ref=image "
        "default_of=[\"sidecar.image.repository\",\"sidecar.image.tag\"]\n"
        "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:2.0.1 "
        "ref=image default_of=[\"image.registry\",\"global.imageRegistry\",\"image.tag\","
        "\"image.repository\"]\n"
        "Resource:StatefulSet/db -> Image:docker.io/bitnami/postgresql USAGE "
        "image=docker.io/bitnami/postgresql:17 ref=image "
        "default_of=[\"global.imageRegistry\",\"image.repository\",\"image.tag\"]";
    const GpfImageExpected expected[] = {
        {.case_id = "helm-default-parent-image",
         .row = "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE "
                "image=ghcr.io/acme/app:2.0.1 ref=image "
                "default_of=[\"image.registry\",\"global.imageRegistry\",\"image.tag\",\"image."
                "repository\"]",
         .repository = "ghcr.io/acme/app",
         .image = "ghcr.io/acme/app:2.0.1",
         .site = {.file = "charts/app/templates/deploy.yaml",
                  .caller = "charts.app.templates.deploy.yaml::apps/Deployment/-/app",
                  .needle = "{{ include \"common.images.image\" (dict \"imageRoot\" .Values.image "
                            "\"global\" .Values.global \"chart\" .Chart) }}"},
         .declaration_line = 4,
         .declaration_text = "  name: app",
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_rendered_image_source_mapping_missing",
         .why = "The exact chart defaults and active template expression determine this repository "
                "image, but rendered output has no certified raw occurrence mapping.",
         .tried =
             "Preserved the original chart sources and all exact DEPLOYS rows; check independent "
             "expression spans, required literal controls and both production drivers."},
        {.case_id = "helm-default-sidecar-image",
         .row = "Resource:Deployment/app -> Image:docker.io/library/busybox USAGE "
                "image=docker.io/library/busybox:1.36 ref=image "
                "default_of=[\"sidecar.image.repository\",\"sidecar.image.tag\"]",
         .repository = "docker.io/library/busybox",
         .image = "docker.io/library/busybox:1.36",
         .site = {.file = "charts/app/templates/deploy.yaml",
                  .caller = "charts.app.templates.deploy.yaml::apps/Deployment/-/app",
                  .needle = "\"{{ .repository }}:{{ .tag }}\""},
         .declaration_line = 4,
         .declaration_text = "  name: app",
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_rendered_image_source_mapping_missing",
         .why = "The exact chart defaults and active template expression determine this repository "
                "image, but rendered output has no certified raw occurrence mapping.",
         .tried =
             "Preserved the original chart sources and all exact DEPLOYS rows; check independent "
             "expression spans, required literal controls and both production drivers."},
        {.case_id = "helm-default-subchart-active-branch",
         .row = "Resource:StatefulSet/db -> Image:docker.io/bitnami/postgresql USAGE "
                "image=docker.io/bitnami/postgresql:17 ref=image "
                "default_of=[\"global.imageRegistry\",\"image.repository\",\"image.tag\"]",
         .repository = "docker.io/bitnami/postgresql",
         .image = "docker.io/bitnami/postgresql:17",
         .site = {.file = "charts/app/charts/db/templates/sts.yaml",
                  .caller = "charts.app.charts.db.templates.sts.yaml::apps/StatefulSet/-/db",
                  .needle = "\"{{ .Values.global.imageRegistry | default \"docker.io\" }}/{{ "
                            ".Values.image.repository }}:{{ .Values.image.tag | default "
                            ".Chart.AppVersion }}\""},
         .declaration_line = 4,
         .declaration_text = "  name: db",
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_rendered_image_source_mapping_missing",
         .why = "The exact chart defaults and active template expression determine this repository "
                "image, but rendered output has no certified raw occurrence mapping.",
         .tried =
             "Preserved the original chart sources and all exact DEPLOYS rows; check independent "
             "expression spans, required literal controls and both production drivers."}};
    if (!gpf_image_multisite_quality(files, 7, "probe_helm_template_images_render_with_defaults",
                                     want, expected, 3)) {
        FAIL("Helm template images rendered with the chart family's defaults");
    }
    PASS();
}

/* A release renders its chart with its values over the defaults: of a chart
 * in the repository each image that differs is the release's own {via,
 * override_of} -- a Flux HelmRelease's spec.values (a Git source), a
 * kustomize helmCharts entry's valuesFile + valuesInline (chartHome charts/),
 * a Terraform helm_release's values + set. Of an external chart the values
 * name images by convention -- `repository` (+ tag / registry) naming a
 * registry host -- {inferred: values}; a host-less repository names none. */
TEST(probe_helm_release_images) {
    static const GpfFile files[] = {
        {"charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 1.0.0\n"},
        {"charts/app/values.yaml", "image:\n  repository: ghcr.io/acme/app\n  tag: \"1.0\"\n"},
        {"charts/app/templates/deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: app\nspec:\n  template:\n"
         "    spec:\n      containers:\n        - name: app\n"
         "          image: \"{{ .Values.image.repository }}:{{ .Values.image.tag }}\"\n"},
        {"flux/app.yaml",
         "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata:\n  name: app\nspec:\n"
         "  chart:\n    spec:\n      chart: ./charts/app\n      sourceRef:\n"
         "        kind: GitRepository\n        name: repo\n  values:\n    image:\n"
         "      tag: \"9.9\"\n---\n"
         "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata:\n  name: sonarr\n"
         "spec:\n  chart:\n    spec:\n      chart: app-template\n      sourceRef:\n"
         "        kind: HelmRepository\n        name: bjw-s\n  values:\n    controllers:\n"
         "      main:\n        containers:\n          main:\n            image:\n"
         "              repository: ghcr.io/home-operations/sonarr\n              tag: 4.0.15\n"
         "          side:\n            image:\n              repository: busybox\n"
         "              tag: \"1.36\"\n"},
        {"kz/kustomization.yaml",
         "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nhelmCharts:\n"
         "  - name: app\n    releaseName: kapp\n    valuesFile: values-prod.yaml\n"
         "    valuesInline:\n      image:\n        tag: \"5.5\"\n"},
        {"kz/values-prod.yaml", "image:\n  repository: quay.io/acme/app\n  tag: \"4.4\"\n"},
        {"kz/charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 1.0.0\n"},
        {"kz/charts/app/values.yaml", "image:\n  repository: ghcr.io/acme/app\n  tag: \"1.0\"\n"},
        {"kz/charts/app/templates/deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: kapp\nspec:\n  template:\n"
         "    spec:\n      containers:\n        - name: app\n"
         "          image: \"{{ .Values.image.repository }}:{{ .Values.image.tag }}\"\n"},
        {"tf/main.tf", "resource \"helm_release\" \"app\" {\n  name  = \"app\"\n"
                       "  chart = \"../charts/app\"\n  set {\n    name  = \"image.tag\"\n"
                       "    value = \"7.7\"\n  }\n}\n"}};
    static const char want[] =
        "Chart:app -> Image:ghcr.io/acme/app DEPLOYS image=- via=chart ref=image "
        "default_of=[\"image.repository\",\"image.tag\"]\n"
        "Chart:app -> Image:ghcr.io/acme/app DEPLOYS image=- via=chart ref=image "
        "default_of=[\"image.repository\",\"image.tag\"]\n"
        "Class:resource.helm_release.app -> Image:ghcr.io/acme/app DEPLOYS "
        "image=ghcr.io/acme/app:7.7 via=terraform ref=image override_of=[\"image.tag\"]\n"
        "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0 "
        "ref=image default_of=[\"image.repository\",\"image.tag\"]\n"
        "Resource:Deployment/kapp -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0 "
        "ref=image default_of=[\"image.repository\",\"image.tag\"]\n"
        "Resource:HelmRelease/app -> Image:ghcr.io/acme/app DEPLOYS image=ghcr.io/acme/app:9.9 "
        "via=helmrelease ref=image override_of=[\"image.tag\"]\n"
        "Resource:HelmRelease/sonarr -> Image:ghcr.io/home-operations/sonarr DEPLOYS "
        "image=ghcr.io/home-operations/sonarr:4.0.15 via=helmrelease ref=image inferred=values "
        "values_key=controllers.main.containers.main.image\n"
        "Resource:Kustomization -> Image:quay.io/acme/app DEPLOYS image=quay.io/acme/app:5.5 "
        "via=helmcharts ref=image override_of=[\"image.repository\",\"image.tag\"]";
    /* WHY: these authored chart contexts determine all image values, but the
     * resolver cannot yet prove their raw-to-rendered occurrence mapping. Tried
     * the original contexts without changing source bytes or release DEPLOYS
     * contracts; both drivers also require a raw Pod image and an ESM call. */
    const GpfImageExpected expected[] = {
        {.case_id = "probe_helm_release_images/image-1",
         .row =
             "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0 "
             "ref=image default_of=[\"image.repository\",\"image.tag\"]",
         .repository = "ghcr.io/acme/app",
         .image = "ghcr.io/acme/app:1.0",
         .site = {.file = "charts/app/templates/deploy.yaml",
                  .caller = "charts.app.templates.deploy.yaml::apps/Deployment/-/app",
                  .needle = "\"{{ .Values.image.repository }}:{{ .Values.image.tag }}\""},
         .declaration_line = 4,
         .declaration_text = "  name: app",
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_rendered_image_source_mapping_unproven",
         .why = "Explicit chart defaults and template context determine this image, but no "
                "admitted proof maps its rendered scalar to the authored YAML occurrence.",
         .tried = "Kept the original sources and all chart/release DEPLOYS rows; the same "
                  "run requires a raw literal Pod image and an ESM call in both drivers."},
        {.case_id = "probe_helm_release_images/image-2",
         .row =
             "Resource:Deployment/kapp -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0 "
             "ref=image default_of=[\"image.repository\",\"image.tag\"]",
         .repository = "ghcr.io/acme/app",
         .image = "ghcr.io/acme/app:1.0",
         .site = {.file = "kz/charts/app/templates/deploy.yaml",
                  .caller = "kz.charts.app.templates.deploy.yaml::apps/Deployment/-/kapp",
                  .needle = "\"{{ .Values.image.repository }}:{{ .Values.image.tag }}\""},
         .declaration_line = 4,
         .declaration_text = "  name: kapp",
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_rendered_image_source_mapping_unproven",
         .why = "Explicit chart defaults and template context determine this image, but no "
                "admitted proof maps its rendered scalar to the authored YAML occurrence.",
         .tried = "Kept the original sources and all chart/release DEPLOYS rows; the same "
                  "run requires a raw literal Pod image and an ESM call in both drivers."},
    };
    if (!gpf_image_quality(files, 10, "probe_helm_release_images", want, expected, 2)) {
        FAIL("release images: local renders and the external values convention");
    }
    PASS();
}

/* The chart every release test below deploys: an image from two values. */
#define GPF_APP_CHART                                                                           \
    {"charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 1.0.0\n"},                   \
        {"charts/app/values.yaml", "image:\n  repository: ghcr.io/acme/app\n  tag: \"1.0\"\n"}, \
        {"charts/app/templates/deploy.yaml",                                                    \
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: app\nspec:\n  template:\n"  \
         "    spec:\n      containers:\n        - name: app\n"                                  \
         "          image: \"{{ .Values.image.repository }}:{{ .Values.image.tag }}\"\n"}

/* The edges of the chart's defaults (every test below has them). */
#define GPF_APP_DEFAULTS                                                                  \
    "Chart:app -> Image:ghcr.io/acme/app DEPLOYS image=- via=chart ref=image "            \
    "default_of=[\"image.repository\",\"image.tag\"]\n"                                   \
    "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0 " \
    "ref=image default_of=[\"image.repository\",\"image.tag\"]"

/* A Terraform number set on a helm_release reaches the chart as the string
 * Terraform makes of it -- its exact decimal (1.1, 1.10 -> "1.1", 2e3 ->
 * "2000"), never a float's %.17g ("1.1000000000000001"). */
TEST(probe_helm_release_terraform_number_set) {
    static const GpfFile files[] = {
        GPF_APP_CHART,
        {"tf/main.tf", "resource \"helm_release\" \"app\" {\n  name  = \"app\"\n"
                       "  chart = \"../charts/app\"\n  set {\n    name  = \"image.tag\"\n"
                       "    value = 1.10\n  }\n}\n"}};
    static const char want[] =
        "Chart:app -> Image:ghcr.io/acme/app DEPLOYS image=- via=chart ref=image "
        "default_of=[\"image.repository\",\"image.tag\"]\n"
        "Class:resource.helm_release.app -> Image:ghcr.io/acme/app DEPLOYS "
        "image=ghcr.io/acme/app:1.1 via=terraform ref=image override_of=[\"image.tag\"]\n"
        "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0 "
        "ref=image default_of=[\"image.repository\",\"image.tag\"]";
    const GpfImageExpected expected = {
        .case_id = "probe_helm_release_terraform_number_set/default-image",
        .row = "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0 "
               "ref=image default_of=[\"image.repository\",\"image.tag\"]",
        .repository = "ghcr.io/acme/app",
        .image = "ghcr.io/acme/app:1.0",
        .site = {.file = "charts/app/templates/deploy.yaml",
                 .caller = "charts.app.templates.deploy.yaml::apps/Deployment/-/app",
                 .needle = "\"{{ .Values.image.repository }}:{{ .Values.image.tag }}\""},
        .declaration_line = 4,
        .declaration_text = "  name: app",
        .mode = RQ_KNOWN_GAP,
        .gap_id = "helm_rendered_image_source_mapping_unproven",
        .why = "Explicit chart defaults determine this image, but no admitted proof maps its "
               "rendered scalar to the authored YAML occurrence.",
        .tried = "Kept every original fixture and exact chart/release DEPLOYS row, including "
                 "unread-values and postrenderer prohibitions. The same run requires a raw literal "
                 "Pod image and an ESM call in both drivers."};
    if (!gpf_image_quality(files, 4, "probe_helm_release_terraform_number_set", want, &expected,
                           1)) {
        FAIL("a Terraform number set is its exact decimal");
    }
    PASS();
}

/* A Flux HelmRelease whose values also come from a source not read --
 * valuesFrom, the chart's valuesFiles, a postRenderer that may patch the
 * images -- renders no image of its own: its values are not known whole. */
TEST(probe_helm_release_flux_unread_sources) {
    static const GpfFile files[] = {
        GPF_APP_CHART,
        {"flux/app.yaml",
         "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata:\n  name: files\n"
         "spec:\n  chart:\n    spec:\n      chart: ./charts/app\n      valuesFiles:\n"
         "        - charts/app/values-prod.yaml\n      sourceRef:\n        kind: GitRepository\n"
         "        name: repo\n  values:\n    image:\n      tag: \"9.9\"\n---\n"
         "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata:\n  name: from\n"
         "spec:\n  chart:\n    spec:\n      chart: ./charts/app\n      sourceRef:\n"
         "        kind: GitRepository\n        name: repo\n  valuesFrom:\n"
         "    - kind: ConfigMap\n      name: app-values\n  values:\n    image:\n"
         "      tag: \"8.8\"\n---\n"
         "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata:\n  name: post\n"
         "spec:\n  chart:\n    spec:\n      chart: ./charts/app\n      sourceRef:\n"
         "        kind: GitRepository\n        name: repo\n  postRenderers:\n"
         "    - kustomize:\n        images:\n          - name: ghcr.io/acme/app\n"
         "            newName: quay.io/acme/app\n  values:\n    image:\n      tag: \"7.7\"\n"}};
    const GpfImageExpected expected = {
        .case_id = "probe_helm_release_flux_unread_sources/default-image",
        .row = "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0 "
               "ref=image default_of=[\"image.repository\",\"image.tag\"]",
        .repository = "ghcr.io/acme/app",
        .image = "ghcr.io/acme/app:1.0",
        .site = {.file = "charts/app/templates/deploy.yaml",
                 .caller = "charts.app.templates.deploy.yaml::apps/Deployment/-/app",
                 .needle = "\"{{ .Values.image.repository }}:{{ .Values.image.tag }}\""},
        .declaration_line = 4,
        .declaration_text = "  name: app",
        .mode = RQ_KNOWN_GAP,
        .gap_id = "helm_rendered_image_source_mapping_unproven",
        .why = "Explicit chart defaults determine this image, but no admitted proof maps its "
               "rendered scalar to the authored YAML occurrence.",
        .tried = "Kept every original fixture and exact chart/release DEPLOYS row, including "
                 "unread-values and postrenderer prohibitions. The same run requires a raw literal "
                 "Pod image and an ESM call in both drivers."};
    if (!gpf_image_quality(files, 4, "probe_helm_release_flux_unread_sources", GPF_APP_DEFAULTS,
                           &expected, 1)) {
        FAIL("a HelmRelease with a source not read has no image of its own");
    }
    PASS();
}

/* A kustomize helmCharts entry with additionalValuesFiles, or a valuesMerge
 * other than the default override, renders no image of its own. */
TEST(probe_helm_release_kustomize_unread_sources) {
    static const GpfFile files[] = {
        {"kz/kustomization.yaml",
         "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nhelmCharts:\n"
         "  - name: app\n    releaseName: more\n    additionalValuesFiles:\n"
         "      - extra.yaml\n    valuesInline:\n      image:\n        tag: \"5.5\"\n"
         "  - name: app\n    releaseName: merged\n    valuesMerge: merge\n"
         "    valuesInline:\n      image:\n        tag: \"6.6\"\n"},
        {"kz/charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 1.0.0\n"},
        {"kz/charts/app/values.yaml", "image:\n  repository: ghcr.io/acme/app\n  tag: \"1.0\"\n"},
        {"kz/charts/app/templates/deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: app\nspec:\n  template:\n"
         "    spec:\n      containers:\n        - name: app\n"
         "          image: \"{{ .Values.image.repository }}:{{ .Values.image.tag }}\"\n"}};
    const GpfImageExpected expected = {
        .case_id = "probe_helm_release_kustomize_unread_sources/default-image",
        .row = "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0 "
               "ref=image default_of=[\"image.repository\",\"image.tag\"]",
        .repository = "ghcr.io/acme/app",
        .image = "ghcr.io/acme/app:1.0",
        .site = {.file = "kz/charts/app/templates/deploy.yaml",
                 .caller = "kz.charts.app.templates.deploy.yaml::apps/Deployment/-/app",
                 .needle = "\"{{ .Values.image.repository }}:{{ .Values.image.tag }}\""},
        .declaration_line = 4,
        .declaration_text = "  name: app",
        .mode = RQ_KNOWN_GAP,
        .gap_id = "helm_rendered_image_source_mapping_unproven",
        .why = "Explicit chart defaults determine this image, but no admitted proof maps its "
               "rendered scalar to the authored YAML occurrence.",
        .tried = "Kept every original fixture and exact chart/release DEPLOYS row, including "
                 "unread-values and postrenderer prohibitions. The same run requires a raw literal "
                 "Pod image and an ESM call in both drivers."};
    if (!gpf_image_quality(files, 4, "probe_helm_release_kustomize_unread_sources",
                           GPF_APP_DEFAULTS, &expected, 1)) {
        FAIL("a helmCharts entry with a part not read has no image of its own");
    }
    PASS();
}

/* A Terraform helm_release with a `dynamic "set"` block (entries made by a
 * for_each) renders no image of its own, whatever its literal sets say. */
TEST(probe_helm_release_terraform_dynamic_set) {
    static const GpfFile files[] = {
        GPF_APP_CHART,
        {"tf/main.tf", "resource \"helm_release\" \"app\" {\n  name  = \"app\"\n"
                       "  chart = \"../charts/app\"\n  dynamic \"set\" {\n"
                       "    for_each = var.sets\n    content {\n      name  = set.key\n"
                       "      value = set.value\n    }\n  }\n  set {\n    name  = \"image.tag\"\n"
                       "    value = \"3.3\"\n  }\n}\n"}};
    const GpfImageExpected expected = {
        .case_id = "probe_helm_release_terraform_dynamic_set/default-image",
        .row = "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0 "
               "ref=image default_of=[\"image.repository\",\"image.tag\"]",
        .repository = "ghcr.io/acme/app",
        .image = "ghcr.io/acme/app:1.0",
        .site = {.file = "charts/app/templates/deploy.yaml",
                 .caller = "charts.app.templates.deploy.yaml::apps/Deployment/-/app",
                 .needle = "\"{{ .Values.image.repository }}:{{ .Values.image.tag }}\""},
        .declaration_line = 4,
        .declaration_text = "  name: app",
        .mode = RQ_KNOWN_GAP,
        .gap_id = "helm_rendered_image_source_mapping_unproven",
        .why = "Explicit chart defaults determine this image, but no admitted proof maps its "
               "rendered scalar to the authored YAML occurrence.",
        .tried = "Kept every original fixture and exact chart/release DEPLOYS row, including "
                 "unread-values and postrenderer prohibitions. The same run requires a raw literal "
                 "Pod image and an ESM call in both drivers."};
    if (!gpf_image_quality(files, 4, "probe_helm_release_terraform_dynamic_set", GPF_APP_DEFAULTS,
                           &expected, 1)) {
        FAIL("a helm_release with a dynamic set has no image of its own");
    }
    PASS();
}

/* A set_sensitive's value is never read: any set_sensitive leaves the
 * release without an image of its own, whatever its name -- image.tag or
 * db.password (which kept its edge in round 3). A postrender may rewrite
 * what a release deploys: no image of its own either. */
TEST(probe_helm_release_terraform_sensitive_and_postrender) {
    static const GpfFile files[] = {
        GPF_APP_CHART,
        {"tf/main.tf",
         "resource \"helm_release\" \"tagged\" {\n  name  = \"tagged\"\n"
         "  chart = \"../charts/app\"\n  set {\n    name  = \"image.repository\"\n"
         "    value = \"quay.io/acme/app\"\n  }\n  set_sensitive {\n    name  = \"image.tag\"\n"
         "    value = var.tag\n  }\n}\n"
         "resource \"helm_release\" \"pw\" {\n  name  = \"pw\"\n"
         "  chart = \"../charts/app\"\n  set {\n    name  = \"image.repository\"\n"
         "    value = \"quay.io/acme/pw\"\n  }\n  set_sensitive {\n    name  = \"db.password\"\n"
         "    value = var.pw\n  }\n}\n"
         "resource \"helm_release\" \"post\" {\n  name  = \"post\"\n"
         "  chart = \"../charts/app\"\n  set {\n    name  = \"image.tag\"\n"
         "    value = \"3.3\"\n  }\n  postrender {\n    binary_path = \"./kustomize.sh\"\n  "
         "}\n}\n"}};
    const GpfImageExpected expected = {
        .case_id = "probe_helm_release_terraform_sensitive_and_postrender/default-image",
        .row = "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0 "
               "ref=image default_of=[\"image.repository\",\"image.tag\"]",
        .repository = "ghcr.io/acme/app",
        .image = "ghcr.io/acme/app:1.0",
        .site = {.file = "charts/app/templates/deploy.yaml",
                 .caller = "charts.app.templates.deploy.yaml::apps/Deployment/-/app",
                 .needle = "\"{{ .Values.image.repository }}:{{ .Values.image.tag }}\""},
        .declaration_line = 4,
        .declaration_text = "  name: app",
        .mode = RQ_KNOWN_GAP,
        .gap_id = "helm_rendered_image_source_mapping_unproven",
        .why = "Explicit chart defaults determine this image, but no admitted proof maps its "
               "rendered scalar to the authored YAML occurrence.",
        .tried = "Kept every original fixture and exact chart/release DEPLOYS row, including "
                 "unread-values and postrenderer prohibitions. The same run requires a raw literal "
                 "Pod image and an ESM call in both drivers."};
    if (!gpf_image_quality(files, 4, "probe_helm_release_terraform_sensitive_and_postrender",
                           GPF_APP_DEFAULTS, &expected, 1)) {
        FAIL("any set_sensitive and a postrender make release images unknown");
    }
    PASS();
}

/* A set_sensitive whose name an image reads on another line ($tag, before
 * the image line): the release still has no image of its own -- never one
 * with the chart's default tag. */
TEST(probe_helm_release_terraform_sensitive_read_earlier) {
    static const GpfFile files[] = {
        {"charts/web/Chart.yaml",
         "apiVersion: v2\nname: web\nversion: 1.0.0\nappVersion: \"1.0\"\n"},
        {"charts/web/values.yaml", "image:\n  repository: ghcr.io/acme/web\n"},
        {"charts/web/templates/deploy.yaml",
         "{{- $tag := .Values.image.tag | default .Chart.AppVersion }}\n"
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\nspec:\n  template:\n"
         "    spec:\n      containers:\n        - name: web\n"
         "          image: \"{{ .Values.image.repository }}:{{ $tag }}\"\n"},
        {"tf/main.tf",
         "resource \"helm_release\" \"web\" {\n  name  = \"web\"\n"
         "  chart = \"../charts/web\"\n  set {\n    name  = \"image.repository\"\n"
         "    value = \"quay.io/acme/web\"\n  }\n  set_sensitive {\n    name  = \"image.tag\"\n"
         "    value = var.tag\n  }\n}\n"}};
    static const char want[] =
        "Chart:web -> Image:ghcr.io/acme/web DEPLOYS image=- via=chart ref=image "
        "default_of=[\"image.repository\"]\n"
        "Resource:Deployment/web -> Image:ghcr.io/acme/web USAGE image=ghcr.io/acme/web:1.0 "
        "ref=image default_of=[\"image.repository\"]";
    const GpfImageExpected expected = {
        .case_id = "probe_helm_release_terraform_sensitive_read_earlier/default-image",
        .row = "Resource:Deployment/web -> Image:ghcr.io/acme/web USAGE image=ghcr.io/acme/web:1.0 "
               "ref=image default_of=[\"image.repository\"]",
        .repository = "ghcr.io/acme/web",
        .image = "ghcr.io/acme/web:1.0",
        .site = {.file = "charts/web/templates/deploy.yaml",
                 .caller = "charts.web.templates.deploy.yaml::apps/Deployment/-/web",
                 .needle = "\"{{ .Values.image.repository }}:{{ $tag }}\""},
        .declaration_line = 5,
        .declaration_text = "  name: web",
        .mode = RQ_KNOWN_GAP,
        .gap_id = "helm_rendered_image_source_mapping_unproven",
        .why = "Explicit chart defaults determine this image, but no admitted proof maps its "
               "rendered scalar to the authored YAML occurrence.",
        .tried = "Kept every original fixture and exact chart/release DEPLOYS row, including "
                 "unread-values and postrenderer prohibitions. The same run requires a raw literal "
                 "Pod image and an ESM call in both drivers."};
    if (!gpf_image_quality(files, 4, "probe_helm_release_terraform_sensitive_read_earlier", want,
                           &expected, 1)) {
        FAIL("a set_sensitive read on another line makes the release images unknown");
    }
    PASS();
}

/* A postrender as a dynamic block or as an attribute (provider v3) may
 * rewrite what a release deploys, as a block does: no image of its own. */
TEST(probe_helm_release_terraform_postrender_forms) {
    static const GpfFile files[] = {
        GPF_APP_CHART,
        {"tf/main.tf",
         "resource \"helm_release\" \"dyn\" {\n  name  = \"dyn\"\n"
         "  chart = \"../charts/app\"\n  set {\n    name  = \"image.tag\"\n"
         "    value = \"3.3\"\n  }\n  dynamic \"postrender\" {\n    for_each = var.post\n"
         "    content {\n      binary_path = postrender.value\n    }\n  }\n}\n"
         "resource \"helm_release\" \"attr\" {\n  name  = \"attr\"\n"
         "  chart = \"../charts/app\"\n  set = [{ name = \"image.tag\", value = \"4.4\" }]\n"
         "  postrender = { binary_path = \"./kustomize.sh\" }\n}\n"}};
    const GpfImageExpected expected = {
        .case_id = "probe_helm_release_terraform_postrender_forms/default-image",
        .row = "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0 "
               "ref=image default_of=[\"image.repository\",\"image.tag\"]",
        .repository = "ghcr.io/acme/app",
        .image = "ghcr.io/acme/app:1.0",
        .site = {.file = "charts/app/templates/deploy.yaml",
                 .caller = "charts.app.templates.deploy.yaml::apps/Deployment/-/app",
                 .needle = "\"{{ .Values.image.repository }}:{{ .Values.image.tag }}\""},
        .declaration_line = 4,
        .declaration_text = "  name: app",
        .mode = RQ_KNOWN_GAP,
        .gap_id = "helm_rendered_image_source_mapping_unproven",
        .why = "Explicit chart defaults determine this image, but no admitted proof maps its "
               "rendered scalar to the authored YAML occurrence.",
        .tried = "Kept every original fixture and exact chart/release DEPLOYS row, including "
                 "unread-values and postrenderer prohibitions. The same run requires a raw literal "
                 "Pod image and an ESM call in both drivers."};
    if (!gpf_image_quality(files, 4, "probe_helm_release_terraform_postrender_forms",
                           GPF_APP_DEFAULTS, &expected, 1)) {
        FAIL("a dynamic or attribute postrender makes release images unknown");
    }
    PASS();
}

/* A subchart included under an alias (redis as `cache`) has .Template.BasePath
 * app/charts/cache/templates, as Helm sets it (an image here shows it). Its
 * checksum of cm.yaml names its own file, which changes no map -- not app's
 * cm.yaml, which sets a value: its image stays known, as does app's (which
 * checksums its Secret).
 *
 * NOT a cross-file guarantee: app's cm.yaml sets .Values.image.tag, and Helm
 * renders a chart's files with one shared .Values (a subchart's is the
 * parent's .Values.<alias>). Each file renders alone here, so app's image is
 * known only because nothing models that set reaching deploy.yaml -- the
 * known limit (review round 4, R4-5), whose exact fix is to run the family's
 * files in Helm's order with one .Values (the run-included-files follow-up). */
TEST(probe_helm_checksum_include_by_base_path) {
    static const GpfFile files[] = {
        {"charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 1.0.0\ndependencies:\n"
                                  "  - name: redis\n    version: 1.0.0\n    alias: cache\n"},
        {"charts/app/values.yaml", "image:\n  repository: ghcr.io/acme/app\n  tag: \"1.0\"\n"},
        {"charts/app/templates/cm.yaml",
         "{{- $_ := set .Values.image \"tag\" \"2.0\" }}\n"
         "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: app\n"},
        {"charts/app/templates/secret.yaml",
         "apiVersion: v1\nkind: Secret\nmetadata:\n  name: app\n"},
        {"charts/app/templates/deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: app\nspec:\n  template:\n"
         "    metadata:\n      annotations:\n        checksum/secret: "
         "{{ include (print $.Template.BasePath \"/secret.yaml\") . | sha256sum }}\n"
         "    spec:\n      containers:\n        - name: app\n"
         "          image: \"{{ .Values.image.repository }}:{{ .Values.image.tag }}\"\n"},
        {"charts/app/charts/redis/Chart.yaml", "apiVersion: v2\nname: redis\nversion: 1.0.0\n"},
        {"charts/app/charts/redis/values.yaml",
         "image:\n  repository: ghcr.io/acme/redis\n  tag: \"7.0\"\n"},
        {"charts/app/charts/redis/templates/cm.yaml",
         "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: redis\n"},
        {"charts/app/charts/redis/templates/sts.yaml",
         "apiVersion: apps/v1\nkind: StatefulSet\nmetadata:\n  name: redis\nspec:\n  template:\n"
         "    metadata:\n      annotations:\n        checksum/config: "
         "{{ include (print $.Template.BasePath \"/cm.yaml\") . | sha256sum }}\n"
         "    spec:\n      containers:\n        - name: redis\n"
         "          image: \"{{ .Values.image.repository }}:{{ .Values.image.tag }}\"\n"
         "        - name: path\n          image: \"ghcr.io/{{ .Template.BasePath }}:1\"\n"}};
    static const char want[] =
        "Chart:app -> Image:ghcr.io/acme/app DEPLOYS image=- via=chart ref=image "
        "default_of=[\"image.repository\",\"image.tag\"]\n"
        "Chart:redis -> Image:ghcr.io/acme/redis DEPLOYS image=- via=chart ref=image "
        "default_of=[\"image.repository\",\"image.tag\"]\n"
        "Chart:redis -> Image:ghcr.io/app/charts/cache/templates DEPLOYS image=- via=chart "
        "ref=image\n"
        "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0 "
        "ref=image default_of=[\"image.repository\",\"image.tag\"]\n"
        "Resource:StatefulSet/redis -> Image:ghcr.io/acme/redis USAGE "
        "image=ghcr.io/acme/redis:7.0 ref=image default_of=[\"image.repository\",\"image.tag\"]\n"
        "Resource:StatefulSet/redis -> Image:ghcr.io/app/charts/cache/templates USAGE "
        "image=ghcr.io/app/charts/cache/templates:1 ref=image";
    /* WHY: these authored chart contexts determine all image values, but the
     * resolver cannot yet prove their raw-to-rendered occurrence mapping. Tried
     * the original contexts without changing source bytes or release DEPLOYS
     * contracts; both drivers also require a raw Pod image and an ESM call. */
    /* Helm 4.3.1 renders these exact sources as app:1.0, redis:7.0 and the
     * aliased BasePath image. The sibling set does not change app:1.0 in
     * Helm's template evaluation order; retain all three genuine obligations. */
    const GpfImageExpected expected[] = {
        {.case_id = "probe_helm_checksum_include_by_base_path/image-1",
         .row =
             "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0 "
             "ref=image default_of=[\"image.repository\",\"image.tag\"]",
         .repository = "ghcr.io/acme/app",
         .image = "ghcr.io/acme/app:1.0",
         .site = {.file = "charts/app/templates/deploy.yaml",
                  .caller = "charts.app.templates.deploy.yaml::apps/Deployment/-/app",
                  .needle = "\"{{ .Values.image.repository }}:{{ .Values.image.tag }}\""},
         .declaration_line = 4,
         .declaration_text = "  name: app",
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_rendered_image_source_mapping_unproven",
         .why = "Explicit chart defaults and template context determine this image, but no "
                "admitted proof maps its rendered scalar to the authored YAML occurrence.",
         .tried = "Kept the original sources and all chart/release DEPLOYS rows; the same "
                  "run requires a raw literal Pod image and an ESM call in both drivers."},
        {.case_id = "probe_helm_checksum_include_by_base_path/image-2",
         .row = "Resource:StatefulSet/redis -> Image:ghcr.io/acme/redis USAGE "
                "image=ghcr.io/acme/redis:7.0 ref=image "
                "default_of=[\"image.repository\",\"image.tag\"]",
         .repository = "ghcr.io/acme/redis",
         .image = "ghcr.io/acme/redis:7.0",
         .site = {.file = "charts/app/charts/redis/templates/sts.yaml",
                  .caller = "charts.app.charts.redis.templates.sts.yaml::apps/StatefulSet/-/redis",
                  .needle = "\"{{ .Values.image.repository }}:{{ .Values.image.tag }}\""},
         .declaration_line = 4,
         .declaration_text = "  name: redis",
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_rendered_image_source_mapping_unproven",
         .why = "Explicit chart defaults and template context determine this image, but no "
                "admitted proof maps its rendered scalar to the authored YAML occurrence.",
         .tried = "Kept the original sources and all chart/release DEPLOYS rows; the same "
                  "run requires a raw literal Pod image and an ESM call in both drivers."},
        {.case_id = "probe_helm_checksum_include_by_base_path/image-3",
         .row = "Resource:StatefulSet/redis -> Image:ghcr.io/app/charts/cache/templates USAGE "
                "image=ghcr.io/app/charts/cache/templates:1 ref=image",
         .repository = "ghcr.io/app/charts/cache/templates",
         .image = "ghcr.io/app/charts/cache/templates:1",
         .site = {.file = "charts/app/charts/redis/templates/sts.yaml",
                  .caller = "charts.app.charts.redis.templates.sts.yaml::apps/StatefulSet/-/redis",
                  .needle = "\"ghcr.io/{{ .Template.BasePath }}:1\""},
         .declaration_line = 4,
         .declaration_text = "  name: redis",
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_rendered_image_source_mapping_unproven",
         .why = "Explicit chart defaults and template context determine this image, but no "
                "admitted proof maps its rendered scalar to the authored YAML occurrence.",
         .tried = "Kept the original sources and all chart/release DEPLOYS rows; the same "
                  "run requires a raw literal Pod image and an ESM call in both drivers."},
    };
    if (!gpf_image_quality(files, 9, "probe_helm_checksum_include_by_base_path", want, expected,
                           3)) {
        FAIL("a checksum include by .Template.BasePath names the chart's own file");
    }
    PASS();
}

/* A kustomization that also transforms what it inflates (images:, patches)
 * has no release image of its own: the transformer may change it. */
TEST(probe_helm_release_kustomize_transformers) {
    static const GpfFile files[] = {
        {"kz/kustomization.yaml",
         "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nhelmCharts:\n"
         "  - name: app\n    releaseName: kapp\n    valuesInline:\n      image:\n"
         "        tag: \"1.5\"\nimages:\n  - name: ghcr.io/acme/app\n    newTag: \"2.0\"\n"},
        {"kz/charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 1.0.0\n"},
        {"kz/charts/app/values.yaml", "image:\n  repository: ghcr.io/acme/app\n  tag: \"1.0\"\n"},
        {"kz/charts/app/templates/deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: app\nspec:\n  template:\n"
         "    spec:\n      containers:\n        - name: app\n"
         "          image: \"{{ .Values.image.repository }}:{{ .Values.image.tag }}\"\n"}};
    const GpfImageExpected expected = {
        .case_id = "probe_helm_release_kustomize_transformers/default-image",
        .row = "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0 "
               "ref=image default_of=[\"image.repository\",\"image.tag\"]",
        .repository = "ghcr.io/acme/app",
        .image = "ghcr.io/acme/app:1.0",
        .site = {.file = "kz/charts/app/templates/deploy.yaml",
                 .caller = "kz.charts.app.templates.deploy.yaml::apps/Deployment/-/app",
                 .needle = "\"{{ .Values.image.repository }}:{{ .Values.image.tag }}\""},
        .declaration_line = 4,
        .declaration_text = "  name: app",
        .mode = RQ_KNOWN_GAP,
        .gap_id = "helm_rendered_image_source_mapping_unproven",
        .why = "Explicit chart defaults determine this image, but no admitted proof maps its "
               "rendered scalar to the authored YAML occurrence.",
        .tried = "Kept every original fixture and exact chart/release DEPLOYS row, including "
                 "unread-values and postrenderer prohibitions. The same run requires a raw literal "
                 "Pod image and an ESM call in both drivers."};
    if (!gpf_image_quality(files, 4, "probe_helm_release_kustomize_transformers", GPF_APP_DEFAULTS,
                           &expected, 1)) {
        FAIL("a kustomization's own transformers make its release images unknown");
    }
    PASS();
}

/* An empty values.yaml is values {} (Helm's), not values not read: the
 * subchart under it still renders its image. */
TEST(probe_helm_empty_values_file_renders) {
    static const GpfFile files[] = {
        {"charts/app/Chart.yaml",
         "apiVersion: v2\nname: app\nversion: 1.0.0\nappVersion: \"1.0\"\n"},
        {"charts/app/values.yaml", ""},
        {"charts/app/templates/deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: app\nspec:\n  template:\n"
         "    spec:\n      containers:\n        - name: app\n"
         "          image: \"ghcr.io/acme/{{ .Chart.Name }}:{{ .Chart.AppVersion }}\"\n"}};
    static const char want[] =
        "Chart:app -> Image:ghcr.io/acme/app DEPLOYS image=- via=chart ref=image\n"
        "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0 "
        "ref=image";
    const GpfImageExpected expected = {
        .case_id = "probe_helm_empty_values_file_renders/default-image",
        .row = "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0 "
               "ref=image",
        .repository = "ghcr.io/acme/app",
        .image = "ghcr.io/acme/app:1.0",
        .site = {.file = "charts/app/templates/deploy.yaml",
                 .caller = "charts.app.templates.deploy.yaml::apps/Deployment/-/app",
                 .needle = "\"ghcr.io/acme/{{ .Chart.Name }}:{{ .Chart.AppVersion }}\""},
        .declaration_line = 4,
        .declaration_text = "  name: app",
        .mode = RQ_KNOWN_GAP,
        .gap_id = "helm_rendered_image_source_mapping_unproven",
        .why = "Explicit chart defaults determine this image, but no admitted proof maps its "
               "rendered scalar to the authored YAML occurrence.",
        .tried = "Kept every original fixture and exact chart/release DEPLOYS row, including "
                 "unread-values and postrenderer prohibitions. The same run requires a raw literal "
                 "Pod image and an ESM call in both drivers."};
    if (!gpf_image_quality(files, 3, "probe_helm_empty_values_file_renders", want, &expected, 1)) {
        FAIL("an empty values.yaml renders with {}");
    }
    PASS();
}

/* A subchart a dependency aliases renders with .Chart.Name its alias (Helm's
 * getAliasDependency), so an image naming the chart names the alias. */
TEST(probe_helm_aliased_subchart_chart_name) {
    static const GpfFile files[] = {
        {"charts/app/Chart.yaml",
         "apiVersion: v2\nname: app\nversion: 1.0.0\ndependencies:\n  - name: worker\n"
         "    version: 0.1.0\n    alias: worker-high\n"},
        {"charts/app/values.yaml", "worker-high: {}\n"},
        {"charts/app/charts/worker/Chart.yaml",
         "apiVersion: v2\nname: worker\nversion: 0.1.0\nappVersion: \"1.0\"\n"},
        {"charts/app/charts/worker/values.yaml", "{}\n"},
        {"charts/app/charts/worker/templates/deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: worker\nspec:\n  template:\n"
         "    spec:\n      containers:\n        - name: worker\n"
         "          image: \"ghcr.io/acme/{{ .Chart.Name }}:{{ .Chart.AppVersion }}\"\n"}};
    static const char want[] =
        "Chart:worker -> Image:ghcr.io/acme/worker-high DEPLOYS image=- via=chart ref=image\n"
        "Resource:Deployment/worker -> Image:ghcr.io/acme/worker-high USAGE "
        "image=ghcr.io/acme/worker-high:1.0 ref=image";
    const GpfImageExpected expected = {
        .case_id = "probe_helm_aliased_subchart_chart_name/default-image",
        .row = "Resource:Deployment/worker -> Image:ghcr.io/acme/worker-high USAGE "
               "image=ghcr.io/acme/worker-high:1.0 ref=image",
        .repository = "ghcr.io/acme/worker-high",
        .image = "ghcr.io/acme/worker-high:1.0",
        .site = {.file = "charts/app/charts/worker/templates/deploy.yaml",
                 .caller =
                     "charts.app.charts.worker.templates.deploy.yaml::apps/Deployment/-/worker",
                 .needle = "\"ghcr.io/acme/{{ .Chart.Name }}:{{ .Chart.AppVersion }}\""},
        .declaration_line = 4,
        .declaration_text = "  name: worker",
        .mode = RQ_KNOWN_GAP,
        .gap_id = "helm_rendered_image_source_mapping_unproven",
        .why = "Explicit chart defaults determine this image, but no admitted proof maps its "
               "rendered scalar to the authored YAML occurrence.",
        .tried = "Kept every original fixture and exact chart/release DEPLOYS row, including "
                 "unread-values and postrenderer prohibitions. The same run requires a raw literal "
                 "Pod image and an ESM call in both drivers."};
    if (!gpf_image_quality(files, 5, "probe_helm_aliased_subchart_chart_name", want, &expected,
                           1)) {
        FAIL("an aliased subchart's .Chart.Name is the alias");
    }
    PASS();
}

/* An image read from a values list keeps its mark on the chart's edge:
 * default_of names the list paths (images[0].repository) whole. */
TEST(probe_helm_list_path_mark_on_chart_edge) {
    static const GpfFile files[] = {
        {"charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 1.0.0\n"},
        {"charts/app/values.yaml", "images:\n  - repository: ghcr.io/acme/app\n    tag: \"1.0\"\n"},
        {"charts/app/templates/deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: app\nspec:\n  template:\n"
         "    spec:\n      containers:\n        - name: app\n"
         "          image: \"{{ (index .Values.images 0).repository }}:"
         "{{ (index .Values.images 0).tag }}\"\n"}};
    static const char want[] =
        "Chart:app -> Image:ghcr.io/acme/app DEPLOYS image=- via=chart ref=image "
        "default_of=[\"images\",\"images[0].repository\",\"images[0].tag\"]\n"
        "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0 "
        "ref=image default_of=[\"images\",\"images[0].repository\",\"images[0].tag\"]";
    const GpfImageExpected expected = {
        .case_id = "probe_helm_list_path_mark_on_chart_edge/default-image",
        .row = "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0 "
               "ref=image default_of=[\"images\",\"images[0].repository\",\"images[0].tag\"]",
        .repository = "ghcr.io/acme/app",
        .image = "ghcr.io/acme/app:1.0",
        .site = {.file = "charts/app/templates/deploy.yaml",
                 .caller = "charts.app.templates.deploy.yaml::apps/Deployment/-/app",
                 .needle = "\"{{ (index .Values.images 0).repository }}:{{ (index .Values.images "
                           "0).tag }}\""},
        .declaration_line = 4,
        .declaration_text = "  name: app",
        .mode = RQ_KNOWN_GAP,
        .gap_id = "helm_rendered_image_source_mapping_unproven",
        .why = "Explicit chart defaults determine this image, but no admitted proof maps its "
               "rendered scalar to the authored YAML occurrence.",
        .tried = "Kept every original fixture and exact chart/release DEPLOYS row, including "
                 "unread-values and postrenderer prohibitions. The same run requires a raw literal "
                 "Pod image and an ESM call in both drivers."};
    if (!gpf_image_quality(files, 3, "probe_helm_list_path_mark_on_chart_edge", want, &expected,
                           1)) {
        FAIL("a list path's mark is kept whole on the chart edge");
    }
    PASS();
}

#ifndef _WIN32
/* gpf_image_edges with symlinks laid in after the files: links[i] names a
 * repository path and the absolute or relative target it points at (POSIX
 * symlinks: the tests using it skip on Windows). */
/* Preserve the original outside-file symlink seam in both production drivers.
 * The external target is not part of the source snapshot or a candidate image. */
static cbm_store_t *gpf_index_linked_quality(GpfProj *lp, const GpfFile *files, size_t nfiles,
                                             const GpfFile *links, size_t nlinks) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_gpf_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir))
        return NULL;
    gpf_to_fwd_slashes(lp->tmpdir);
    for (size_t i = 0; i < nfiles + nlinks; i++) {
        const GpfFile *file = i < nfiles ? files + i : links + i - nfiles;
        char path[700];
        int length = snprintf(path, sizeof(path), "%s/%s", lp->tmpdir, file->name);
        if (length < 0 || (size_t)length >= sizeof(path))
            return NULL;
        char *slash = strrchr(path, '/');
        if (slash && slash > path + strlen(lp->tmpdir)) {
            *slash = '\0';
            cbm_mkdir_p(path, 0755);
            *slash = '/';
        }
        if (i >= nfiles) {
            if (symlink(file->content, path) != 0)
                return NULL;
        } else {
            FILE *fp = cbm_fopen(path, "wb");
            if (!fp)
                return NULL;
            bool written = fputs(file->content, fp) >= 0;
            written &= fclose(fp) == 0;
            if (!written)
                return NULL;
        }
    }
    return gpf_open_indexed(lp);
}

static char *gpf_linked_deployment_rows(cbm_store_t *store, const char *project) {
    sqlite3_stmt *row = NULL;
    const char *sql =
        "SELECT coalesce(group_concat(x, char(10)), '') FROM (SELECT s.label || ':' || s.name "
        "|| ' -> Image:' || t.name || ' ' || e.type || ' image=' || "
        "coalesce(json_extract(e.properties,'$.image'),'-') || "
        "coalesce(' via=' || json_extract(e.properties,'$.via'), '') AS x "
        "FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id "
        "WHERE e.project=?1 AND t.label='Image' AND e.type!='USAGE' ORDER BY x)";
    char *out = NULL;
    if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &row, NULL) == SQLITE_OK &&
        sqlite3_bind_text(row, 1, project, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
        sqlite3_step(row) == SQLITE_ROW)
        out = strdup((const char *)sqlite3_column_text(row, 0));
    sqlite3_finalize(row);
    return out;
}

static bool gpf_linked_image_quality(const GpfFile *files, size_t nfiles, const GpfFile *links,
                                     size_t nlinks, const char *suite, const char *want,
                                     const GpfImageExpected *expected, size_t count) {
    enum { MAX_FILES = 24, MAX_IMAGES = 16, PADDING = 56 };
    if (!expected || !count || count >= MAX_IMAGES || nfiles > MAX_FILES)
        return false;
    char *required = gpf_image_required_rows(want, expected, count);
    if (!required)
        return false;
    GpfFile indexed[MAX_FILES + 2 + PADDING];
    RQ_Source sources[MAX_FILES + 2];
    GpfImageExpected goldens[MAX_IMAGES];
    char names[PADDING][48];
    for (size_t i = 0; i < nfiles; i++) {
        indexed[i] = files[i];
        sources[i] = (RQ_Source){.path = files[i].name, .text = files[i].content};
    }
    indexed[nfiles] = GPF_IMAGE_LITERAL_CONTROL;
    sources[nfiles] = (RQ_Source){.path = GPF_IMAGE_LITERAL_CONTROL.name,
                                  .text = GPF_IMAGE_LITERAL_CONTROL.content};
    indexed[nfiles + 1] = GPF_IMPORT_ECMA_CONTROL;
    sources[nfiles + 1] =
        (RQ_Source){.path = GPF_IMPORT_ECMA_CONTROL.name, .text = GPF_IMPORT_ECMA_CONTROL.content};
    for (size_t i = 0; i < count; i++)
        goldens[i] = expected[i];
    goldens[count] = GPF_IMAGE_LITERAL_REQUIRED;
    bool ok = true;
    for (int driver = 0; driver < 2; driver++) {
        size_t total = nfiles + 2;
        for (int i = 0; driver && i < PADDING; i++) {
            snprintf(names[i], sizeof(names[i]), "quality_padding/linked_%02d.js", i);
            indexed[total++] = (GpfFile){names[i], "export const padding = 1;\n"};
        }
        GpfProj lp;
        cbm_store_t *store = gpf_index_linked_quality(&lp, indexed, total, links, nlinks);
        char label[192];
        snprintf(label, sizeof(label), "%s/%s", suite, driver ? "parallel" : "sequential");
        if (!store) {
            ok = false;
            gpf_cleanup(&lp, store);
            continue;
        }
        char *actual = gpf_linked_deployment_rows(store, lp.project);
        bool rows = actual && !strcmp(actual, required);
        if (!rows)
            fprintf(stderr, "[IMAGE_RQ] %s linked deployment rows got:\n%s\nwant:\n%s\n", label,
                    actual ? actual : "(null)", required);
        free(actual);
        bool images = gpf_image_occurrences(store, lp.project, label, sources, nfiles + 2, goldens,
                                            count + 1);
        RQ_Report control;
        bool calls = rq_check(store, lp.project, label, sources + nfiles + 1, 1,
                              &GPF_IMPORT_ECMA_REQUIRED, 1, NULL, 0, &control, stderr);
        ok &= rows && images && calls;
        gpf_cleanup(&lp, store);
    }
    free(required);
    return ok;
}
#endif

/* A values file a chart, a kustomization or a Terraform file() names by path
 * is read only when it is a file of the repository: a symlink out of it --
 * to a file holding an image -- is never followed. The chart whose values
 * file is such a link renders nothing (its values are unknown, not empty);
 * the releases reading one have no image of their own. */
TEST(probe_helm_values_symlink_not_followed) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX symlink contract");
#else
    char outside[256];
    snprintf(outside, sizeof(outside), "/tmp/cbm_gpf_out_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(outside));
    char leak[300];
    snprintf(leak, sizeof(leak), "%s/values.yaml", outside);
    FILE *lf = fopen(leak, "wb");
    ASSERT_NOT_NULL(lf);
    fputs("image:\n  repository: ghcr.io/evil/leak\n  tag: \"6.6\"\n", lf);
    fclose(lf);
    const GpfFile files[] = {
        GPF_APP_CHART,
        {"linked/Chart.yaml", "apiVersion: v2\nname: linked\nversion: 1.0.0\n"},
        {"linked/templates/deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: linked\nspec:\n  template:\n"
         "    spec:\n      containers:\n        - name: linked\n"
         "          image: \"ghcr.io/acme/linked:{{ .Values.tag | default \"latest\" }}\"\n"},
        {"kz/kustomization.yaml",
         "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nhelmCharts:\n"
         "  - name: app\n    releaseName: kapp\n    valuesFile: prod.yaml\n"},
        {"kz/charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 1.0.0\n"},
        {"kz/charts/app/values.yaml", "image:\n  repository: ghcr.io/acme/app\n  tag: \"1.0\"\n"},
        {"kz/charts/app/templates/deploy.yaml",
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: kapp\nspec:\n  template:\n"
         "    spec:\n      containers:\n        - name: app\n"
         "          image: \"{{ .Values.image.repository }}:{{ .Values.image.tag }}\"\n"}};
    const GpfFile links[] = {{"linked/values.yaml", leak}, {"kz/prod.yaml", leak}};
    static const char want[] =
        "Chart:app -> Image:ghcr.io/acme/app DEPLOYS image=- via=chart\n"
        "Chart:app -> Image:ghcr.io/acme/app DEPLOYS image=- via=chart\n"
        "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0\n"
        "Resource:Deployment/kapp -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0";
    /* WHY: local chart defaults remain genuine even though the unrelated
     * linked values must not be read. Tried the original outside file and links
     * unchanged in both drivers; leaked images and release guesses are forbidden
     * by the closed Image domain, with required literal and ESM controls. */
    const GpfImageExpected expected[] = {
        {.case_id = "probe_helm_values_symlink_not_followed/default-app",
         .row =
             "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0",
         .repository = "ghcr.io/acme/app",
         .image = "ghcr.io/acme/app:1.0",
         .site = {.file = "charts/app/templates/deploy.yaml",
                  .caller = "charts.app.templates.deploy.yaml::apps/Deployment/-/app",
                  .needle = "\"{{ .Values.image.repository }}:{{ .Values.image.tag }}\""},
         .declaration_line = 4,
         .declaration_text = "  name: app",
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_rendered_image_source_mapping_missing",
         .why = "The exact chart defaults and active template expression determine this repository "
                "image, but rendered output has no certified raw occurrence mapping.",
         .tried =
             "Preserved the original chart sources and all exact DEPLOYS rows; check independent "
             "expression spans, required literal controls and both production drivers."},
        {.case_id = "probe_helm_values_symlink_not_followed/default-kapp",
         .row =
             "Resource:Deployment/kapp -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0",
         .repository = "ghcr.io/acme/app",
         .image = "ghcr.io/acme/app:1.0",
         .site = {.file = "kz/charts/app/templates/deploy.yaml",
                  .caller = "kz.charts.app.templates.deploy.yaml::apps/Deployment/-/kapp",
                  .needle = "\"{{ .Values.image.repository }}:{{ .Values.image.tag }}\""},
         .declaration_line = 4,
         .declaration_text = "  name: kapp",
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_rendered_image_source_mapping_missing",
         .why = "The exact chart defaults and active template expression determine this repository "
                "image, but rendered output has no certified raw occurrence mapping.",
         .tried =
             "Preserved the original chart sources and all exact DEPLOYS rows; check independent "
             "expression spans, required literal controls and both production drivers."}};
    bool ok = gpf_linked_image_quality(files, 9, links, 2, "probe_helm_values_symlink_not_followed",
                                       want, expected, 2);
    th_rmtree(outside);
    ASSERT_TRUE(ok);
    PASS();
#endif
}

/* A Terraform helm_release reading its values with file() of a symlink out
 * of the repository has no image of its own: the link is never followed. */
TEST(probe_helm_terraform_values_symlink_not_followed) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX symlink contract");
#else
    char outside[256];
    snprintf(outside, sizeof(outside), "/tmp/cbm_gpf_out_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(outside));
    char leak[300];
    snprintf(leak, sizeof(leak), "%s/vals.yaml", outside);
    FILE *lf = fopen(leak, "wb");
    ASSERT_NOT_NULL(lf);
    fputs("image:\n  repository: ghcr.io/evil/leak\n  tag: \"6.6\"\n", lf);
    fclose(lf);
    const GpfFile files[] = {
        GPF_APP_CHART,
        {"tf/main.tf", "resource \"helm_release\" \"app\" {\n  name   = \"app\"\n"
                       "  chart  = \"../charts/app\"\n  values = [file(\"vals.yaml\")]\n}\n"}};
    const GpfFile links[] = {{"tf/vals.yaml", leak}};
    static const char want[] =
        "Chart:app -> Image:ghcr.io/acme/app DEPLOYS image=- via=chart\n"
        "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0";
    /* WHY: local chart defaults remain genuine even though the unrelated
     * linked values must not be read. Tried the original outside file and links
     * unchanged in both drivers; leaked images and release guesses are forbidden
     * by the closed Image domain, with required literal and ESM controls. */
    const GpfImageExpected expected[] = {
        {.case_id = "probe_helm_terraform_values_symlink_not_followed/default-app",
         .row =
             "Resource:Deployment/app -> Image:ghcr.io/acme/app USAGE image=ghcr.io/acme/app:1.0",
         .repository = "ghcr.io/acme/app",
         .image = "ghcr.io/acme/app:1.0",
         .site = {.file = "charts/app/templates/deploy.yaml",
                  .caller = "charts.app.templates.deploy.yaml::apps/Deployment/-/app",
                  .needle = "\"{{ .Values.image.repository }}:{{ .Values.image.tag }}\""},
         .declaration_line = 4,
         .declaration_text = "  name: app",
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_rendered_image_source_mapping_missing",
         .why = "The exact chart defaults and active template expression determine this repository "
                "image, but rendered output has no certified raw occurrence mapping.",
         .tried =
             "Preserved the original chart sources and all exact DEPLOYS rows; check independent "
             "expression spans, required literal controls and both production drivers."}};
    bool ok = gpf_linked_image_quality(
        files, 4, links, 1, "probe_helm_terraform_values_symlink_not_followed", want, expected, 1);
    th_rmtree(outside);
    ASSERT_TRUE(ok);
    PASS();
#endif
}

/* A build argument holding a secret -- by its name (*_TOKEN) or its value (a
 * GitHub token) -- is never kept on a build edge; the others still choose the
 * base image they set. */
TEST(probe_docker_build_args_keep_no_secret) {
    static const GpfFile files[] = {
        {"web/Dockerfile", "ARG GO=1.22\nFROM golang:${GO}\n"},
        {"api/Dockerfile", "ARG PY=3.12\nFROM python:${PY}-slim\n"},
        {".env", "NPM_TOKEN=npm-secret-value\n"},
        {"compose.yaml", "services:\n  web:\n    image: ghcr.io/acme/web\n    build:\n"
                         "      context: ./web\n      args:\n        NPM_TOKEN: ${NPM_TOKEN}\n"
                         "        GO: \"1.23\"\n"},
        {"scripts/build.sh",
         "#!/bin/sh\ndocker build --build-arg CI_KEY=ghp_abcdefghijklmnopqrstuvwxyz0123456789AB "
         "--build-arg PY=3.13 -t ghcr.io/acme/api:1 api\n"}};
    int secrets = gpf_query_int(files, 5,
                                "SELECT count(*) FROM edges WHERE project=?1 AND "
                                "(properties LIKE '%npm-secret-value%' OR properties LIKE "
                                "'%ghp_abcdefghij%' OR properties LIKE '%NPM_TOKEN%')");
    int overrides = gpf_query_int(files, 5,
                                  "SELECT count(*) FROM edges WHERE project=?1 AND "
                                  "json_extract(properties,'$.override_of') IS NOT NULL");
    ASSERT_EQ(secrets, 0);
    ASSERT_EQ(overrides, 2);
    PASS();
}

/* Build arguments of any length are kept whole on the build edge (never cut
 * to malformed JSON): the one choosing the base still makes its edge. */
TEST(probe_docker_long_build_args_kept_whole) {
    enum { ARGS = 40, VAL = 150, CAP = ARGS * (VAL + 40) + 512 };
    char *script = malloc(CAP);
    ASSERT_NOT_NULL(script);
    int o = snprintf(script, CAP, "#!/bin/sh\ndocker build");
    char val[VAL + 1];
    memset(val, 'v', VAL);
    val[VAL] = '\0';
    for (int i = 0; i < ARGS; i++) {
        o += snprintf(script + o, (size_t)(CAP - o), " --build-arg LABEL%d=%s", i, val);
    }
    snprintf(script + o, (size_t)(CAP - o), " --build-arg PY=3.13 -t ghcr.io/acme/api:1 api\n");
    const GpfFile files[] = {{"api/Dockerfile", "ARG PY=3.12\nFROM python:${PY}-slim\n"},
                             {"scripts/build.sh", script}};
    int overrides =
        gpf_query_int(files, 2,
                      "SELECT count(*) FROM edges e JOIN nodes t ON t.id=e.target_id WHERE "
                      "e.project=?1 AND t.name='python' AND "
                      "json_extract(e.properties,'$.override_of') IS NOT NULL");
    free(script);
    ASSERT_EQ(overrides, 1);
    PASS();
}

/* A template's `include "name"` calls the define of that name in its chart
 * family -- the chart and its subcharts share one template namespace -- never
 * a same-named define of an unrelated chart; a define may also be written in
 * the template itself (deckhouse, gardener). */
TEST(probe_helm_include_resolves_in_the_chart_family) {
    static const GpfFile files[] = {
        {"app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n"},
        {"app/templates/_helpers.tpl", "{{- define \"app.fullname\" -}}app{{- end }}\n"
                                       "{{- define \"app.labels\" -}}tier: web{{- end }}\n"},
        {"app/templates/deployment.yaml",
         "{{- define \"web.resources\" }}\ncpu: 1\n{{- if .Values.big }}\nmemory: 2Gi\n{{- end }}\n"
         "{{- end }}\n---\n"
         "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  labels:\n"
         "    {{- include \"app.labels\" . | nindent 4 }}\n"
         "    owner: {{ include \"app.fullname\" . }}\n"
         "    sub: {{ template \"sub.name\" . }}\n"
         "    dyn: {{ include (printf \"%s.x\" .Chart.Name) . }}\n"
         "spec:\n  resources: {{ include \"web.resources\" . | nindent 4 }}\n"},
        {"app/charts/sub/Chart.yaml", "apiVersion: v2\nname: sub\nversion: 0.1.0\n"},
        {"app/charts/sub/templates/_helpers.tpl", "{{- define \"sub.name\" -}}sub{{- end }}\n"},
        {"other/Chart.yaml", "apiVersion: v2\nname: other\nversion: 0.1.0\n"},
        {"other/templates/_helpers.tpl", "{{- define \"app.fullname\" -}}other{{- end }}\n"}};
    /* WHY: literal include/template dependencies are genuine, but the legacy
     * chart-family pass supplies no immutable raw-site proof. Tried all four exact
     * definitions unchanged, including the local define and nested chart. Dynamic
     * names and unrelated charts cannot borrow those identities. */
    const RQ_Expected expected[] = {
        {.case_id = "helm-labels",
         .site = {.file = "app/templates/deployment.yaml",
                  .caller = "app.templates.deployment.yaml::apps/Deployment/-/web",
                  .start_byte = 174,
                  .end_byte = 196},
         .relation = "CALLS",
         .target = {.file = "app/templates/_helpers.tpl",
                    .suffix = "app.templates._helpers.app.labels",
                    .label = "Function",
                    .declaration_line = 2,
                    .declaration_text = "{{- define \"app.labels\" -}}tier: web{{- end }}"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_named_template_site_proof_missing",
         .why = "The unique literal template definition is in the selected chart family; the "
                "legacy include pass lacks immutable raw occurrence evidence.",
         .tried = "Preserved original source bytes; explicit build membership and exact "
                  "declaration/site goldens run with sequential and parallel drivers. Wrong "
                  "targets and roles remain failures."},
        {.case_id = "helm-fullname",
         .site = {.file = "app/templates/deployment.yaml",
                  .caller = "app.templates.deployment.yaml::apps/Deployment/-/web",
                  .start_byte = 226,
                  .end_byte = 250},
         .relation = "CALLS",
         .target = {.file = "app/templates/_helpers.tpl",
                    .suffix = "app.templates._helpers.app.fullname",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "{{- define \"app.fullname\" -}}app{{- end }}"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_named_template_site_proof_missing",
         .why = "The unique literal template definition is in the selected chart family; the "
                "legacy include pass lacks immutable raw occurrence evidence.",
         .tried = "Preserved original source bytes; explicit build membership and exact "
                  "declaration/site goldens run with sequential and parallel drivers. Wrong "
                  "targets and roles remain failures."},
        {.case_id = "helm-subchart",
         .site = {.file = "app/templates/deployment.yaml",
                  .caller = "app.templates.deployment.yaml::apps/Deployment/-/web",
                  .start_byte = 266,
                  .end_byte = 287},
         .relation = "CALLS",
         .target = {.file = "app/charts/sub/templates/_helpers.tpl",
                    .suffix = "app.charts.sub.templates._helpers.sub.name",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "{{- define \"sub.name\" -}}sub{{- end }}"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_named_template_site_proof_missing",
         .why = "The unique literal template definition is in the selected chart family; the "
                "legacy include pass lacks immutable raw occurrence evidence.",
         .tried = "Preserved original source bytes; explicit build membership and exact "
                  "declaration/site goldens run with sequential and parallel drivers. Wrong "
                  "targets and roles remain failures."},
        {.case_id = "helm-inline-define",
         .site = {.file = "app/templates/deployment.yaml",
                  .caller = "app.templates.deployment.yaml::apps/Deployment/-/web",
                  .start_byte = 366,
                  .end_byte = 391},
         .relation = "CALLS",
         .target = {.file = "app/templates/deployment.yaml",
                    .suffix = "app.templates.deployment.web.resources",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "{{- define \"web.resources\" }}"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "helm_named_template_site_proof_missing",
         .why = "The unique literal template definition is in the selected chart family; the "
                "legacy include pass lacks immutable raw occurrence evidence.",
         .tried = "Preserved original source bytes; explicit build membership and exact "
                  "declaration/site goldens run with sequential and parallel drivers. Wrong "
                  "targets and roles remain failures."}};
    const RQ_Forbidden forbidden[] = {
        {.case_id = "unrelated-chart-template",
         .site = {.file = "app/templates/deployment.yaml",
                  .caller = "app.templates.deployment.yaml::apps/Deployment/-/web",
                  .start_byte = 226,
                  .end_byte = 250},
         .relation = "CALLS",
         .target = {.file = "other/templates/_helpers.tpl",
                    .suffix = "other.templates._helpers.app.fullname",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "{{- define \"app.fullname\" -}}other{{- end }}"}},
        {.case_id = "dynamic-template-name",
         .site = {.file = "app/templates/deployment.yaml",
                  .caller = "app.templates.deployment.yaml::apps/Deployment/-/web",
                  .start_byte = 303,
                  .end_byte = 340},
         .relation = "CALLS",
         .any_target = true}};
    ASSERT_TRUE(gpf_infra_quality(files, 7, "probe_helm_include_resolves_in_the_chart_family",
                                  expected, 4, forbidden, 2));
    PASS();
}

/* Alternative CRD declarations require an explicit selected build. Neither
 * directory proximity nor fallback fan-out identifies the deployed copy. */
TEST(probe_k8s_cr_prefers_the_crd_in_its_bundle) {
    static const char crd[] =
        "apiVersion: apiextensions.k8s.io/v1\nkind: CustomResourceDefinition\n"
        "metadata:\n  name: widgets.example.com\nspec:\n  group: example.com\n"
        "  names:\n    kind: Widget\n    plural: widgets\n  scope: Namespaced\n";
    static const char cr[] = "apiVersion: example.com/v1\nkind: Widget\nmetadata:\n  name: w1\n";
    static const GpfFile files[] = {{"app/crd.yaml", crd},
                                    {"app/widget.yaml", cr},
                                    {"vendor-chart/crds/crd.yaml", crd},
                                    {"other/widget.yaml", cr}};
    /* WHY: folder proximity and fallback fan-out do not select a CRD copy.
     * Tried the original alternatives unchanged, then each explicit selected build.
     * The uniquely selected CRD dependency is genuine but still lacks certified
     * source/schema proof. Unselected objects and competing copies stay forbidden. */
    const RQ_Forbidden unconfigured[] = {
        {.case_id = "unconfigured-cr-app",
         .site = {.file = "app/widget.yaml",
                  .caller = "app.widget.yaml::example.com/Widget/-/w1",
                  .needle = "Widget"},
         .relation = "USAGE",
         .any_target = true},
        {.case_id = "unconfigured-cr-other",
         .site = {.file = "other/widget.yaml",
                  .caller = "other.widget.yaml::example.com/Widget/-/w1",
                  .needle = "Widget"},
         .relation = "USAGE",
         .any_target = true}};
    ASSERT_TRUE(gpf_infra_quality(files, 4, "crd-unconfigured", NULL, 0, unconfigured, 2));
    const GpfFile configured_0[] = {
        files[0],
        files[1],
        files[2],
        files[3],
        {"kustomization.yaml", "resources:\n  - app/crd.yaml\n  - app/widget.yaml\n"}};
    const RQ_Expected expected_0[] = {
        {.case_id = "selected-crd-1",
         .site = {.file = "app/widget.yaml",
                  .caller = "app.widget.yaml::example.com/Widget/-/w1",
                  .needle = "Widget"},
         .relation = "USAGE",
         .target = {.file = "app/crd.yaml",
                    .suffix = "app.crd.yaml::apiextensions.k8s.io/CustomResourceDefinition/-/"
                              "widgets.example.com",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: widgets.example.com"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "k8s_crd_declaration_source_scope_missing",
         .why = "The explicit build selects this unique CRD declaration for the resource "
                "group/kind; no immutable CRD binding adapter exists.",
         .tried =
             "Preserved the original source declarations; exact source/target goldens and required "
             "ESM control run in both drivers. No directory or name-only target authority."}};
    const RQ_Forbidden forbidden_0[] = {
        {.case_id = "competing-crd-1",
         .site = {.file = "app/widget.yaml",
                  .caller = "app.widget.yaml::example.com/Widget/-/w1",
                  .needle = "Widget"},
         .relation = "USAGE",
         .target = {.file = "vendor-chart/crds/crd.yaml",
                    .suffix = "vendor-chart.crds.crd.yaml::apiextensions.k8s.io/"
                              "CustomResourceDefinition/-/widgets.example.com",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: widgets.example.com"}},
        {.case_id = "unselected-cr-1",
         .site = {.file = "other/widget.yaml",
                  .caller = "other.widget.yaml::example.com/Widget/-/w1",
                  .needle = "Widget"},
         .relation = "USAGE",
         .any_target = true}};
    ASSERT_TRUE(
        gpf_infra_quality(configured_0, 5, "crd-selected-1", expected_0, 1, forbidden_0, 2));
    const GpfFile configured_1[] = {
        files[0],
        files[1],
        files[2],
        files[3],
        {"kustomization.yaml",
         "resources:\n  - vendor-chart/crds/crd.yaml\n  - other/widget.yaml\n"}};
    const RQ_Expected expected_1[] = {
        {.case_id = "selected-crd-2",
         .site = {.file = "other/widget.yaml",
                  .caller = "other.widget.yaml::example.com/Widget/-/w1",
                  .needle = "Widget"},
         .relation = "USAGE",
         .target = {.file = "vendor-chart/crds/crd.yaml",
                    .suffix = "vendor-chart.crds.crd.yaml::apiextensions.k8s.io/"
                              "CustomResourceDefinition/-/widgets.example.com",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: widgets.example.com"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "k8s_crd_declaration_source_scope_missing",
         .why = "The explicit build selects this unique CRD declaration for the resource "
                "group/kind; no immutable CRD binding adapter exists.",
         .tried =
             "Preserved the original source declarations; exact source/target goldens and required "
             "ESM control run in both drivers. No directory or name-only target authority."}};
    const RQ_Forbidden forbidden_1[] = {
        {.case_id = "competing-crd-2",
         .site = {.file = "other/widget.yaml",
                  .caller = "other.widget.yaml::example.com/Widget/-/w1",
                  .needle = "Widget"},
         .relation = "USAGE",
         .target = {.file = "app/crd.yaml",
                    .suffix = "app.crd.yaml::apiextensions.k8s.io/CustomResourceDefinition/-/"
                              "widgets.example.com",
                    .label = "Resource",
                    .declaration_line = 4,
                    .declaration_text = "  name: widgets.example.com"}},
        {.case_id = "unselected-cr-2",
         .site = {.file = "app/widget.yaml",
                  .caller = "app.widget.yaml::example.com/Widget/-/w1",
                  .needle = "Widget"},
         .relation = "USAGE",
         .any_target = true}};
    ASSERT_TRUE(
        gpf_infra_quality(configured_1, 5, "crd-selected-2", expected_1, 1, forbidden_1, 2));
    PASS();
}

/* ── Bundles: a Service selects inside what is deployed together ───── */

#define GPF_DEPLOY(name, label)                                                                \
    "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: " name "\nspec:\n  template:\n" \
    "    metadata:\n      labels:\n        app: " label "\n"
#define GPF_SVC(name, label)                                  \
    "apiVersion: v1\nkind: Service\nmetadata:\n  name: " name \
    "\nspec:\n  selector:\n    app: " label "\n"

/* Two copies of one app in two directories (microservices-demo ships
 * kubernetes-manifests/, release/ and kustomize/base/; k8s.io one prow per
 * cluster): each Service selects its own copy's Deployment, not the other. */
TEST(probe_k8s_selector_stays_in_its_directory) {
    static const GpfFile files[] = {{"gke/svc.yaml", GPF_SVC("web", "web")},
                                    {"gke/deploy.yaml", GPF_DEPLOY("web", "web")},
                                    {"eks/deploy.yaml", GPF_DEPLOY("web", "web")}};
    GpfMetrics m = gpf_metrics_files(files, 3);
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.resources, 3);
    ASSERT_EQ(m.infra_maps, 1);
    PASS();
}

/* A kustomize build root deploys what its entries reach, across
 * directories; a same-labelled Deployment outside it is another bundle. */
TEST(probe_k8s_selector_within_kustomize_build) {
    static const GpfFile files[] = {
        {"overlay/kustomization.yaml", "resources:\n  - ../svc\n  - ../app\n"},
        {"svc/kustomization.yaml", "resources:\n  - svc.yaml\n"},
        {"svc/svc.yaml", GPF_SVC("web", "web")},
        {"app/kustomization.yaml", "resources:\n  - deploy.yaml\n"},
        {"app/deploy.yaml", GPF_DEPLOY("web", "web")},
        {"other/deploy.yaml", GPF_DEPLOY("web-copy", "web")}};
    GpfMetrics m = gpf_metrics_files(files, 6);
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.resources, 3);
    ASSERT_EQ(m.infra_maps, 1);
    PASS();
}

/* The build root's `namespace:` is where its objects live: the logical
 * key carries it (kustomize's namespace transformer, outermost wins). */
TEST(probe_k8s_kustomize_namespace_is_effective) {
    static const GpfFile files[] = {
        {"prod/kustomization.yaml", "namespace: prod\nresources:\n  - ../base\n"},
        {"base/kustomization.yaml", "namespace: base-ns\nresources:\n  - deploy.yaml\n"},
        {"base/deploy.yaml", GPF_DEPLOY("web", "web")}};
    char qn[256];
    char props[512];
    ASSERT_TRUE(gpf_resource(files, 3, "Deployment/web", qn, sizeof(qn), props, sizeof(props)));
    ASSERT_STR_EQ(qn, "::apps/Deployment/-/web");
    ASSERT_NOT_NULL(strstr(props, "\"obj_key\":\"apps/Deployment/prod/web\""));
    PASS();
}

/* Flow-style label maps (`labels: { app: kdc }`, deckhouse's kerberos demo)
 * are label maps; a selector given that way selects just the same. */
TEST(probe_k8s_selector_flow_mapping) {
    GpfMetrics m = gpf_metrics("kdc.yaml", "apiVersion: apps/v1\n"
                                           "kind: Deployment\n"
                                           "metadata:\n"
                                           "  name: kdc\n"
                                           "spec:\n"
                                           "  template:\n"
                                           "    metadata:\n"
                                           "      labels: { app: kdc, tier: \"auth\" }\n"
                                           "---\n"
                                           "apiVersion: v1\n"
                                           "kind: Service\n"
                                           "metadata:\n"
                                           "  name: kdc\n"
                                           "spec:\n"
                                           "  selector: { app: kdc }\n"
                                           "---\n"
                                           "apiVersion: v1\n"
                                           "kind: Service\n"
                                           "metadata:\n"
                                           "  name: other\n"
                                           "spec:\n"
                                           "  selector: {app: kdc, tier: web}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.resources, 3);
    ASSERT_EQ(m.infra_maps, 1); /* `other` needs tier=web, which kdc lacks */
    PASS();
}

/* An anchored name (`name: &name app`, onedr0p/home-ops reuses it in the
 * chart values) is still the object's name; so is a tagged one. */
TEST(probe_k8s_anchored_and_tagged_names) {
    GpfMetrics m = gpf_metrics("release.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\n"
                                               "kind: HelmRelease\n"
                                               "metadata:\n"
                                               "  name: &name actions-runner-controller\n"
                                               "spec:\n"
                                               "  releaseName: *name\n"
                                               "---\n"
                                               "apiVersion: v1\n"
                                               "kind: ConfigMap\n"
                                               "metadata:\n"
                                               "  name: !!str settings\n");
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.resources, 2);
    PASS();
}

/* One name in two namespaces is two objects. */
TEST(probe_k8s_same_name_two_namespaces) {
    GpfMetrics m = gpf_metrics("app.yaml", "apiVersion: apps/v1\n"
                                           "kind: Deployment\n"
                                           "metadata:\n"
                                           "  name: web\n"
                                           "  namespace: staging\n"
                                           "---\n"
                                           "apiVersion: apps/v1\n"
                                           "kind: Deployment\n"
                                           "metadata:\n"
                                           "  name: web\n"
                                           "  namespace: prod\n");
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.resources, 2);
    PASS();
}

/* The node QN carries group, kind, declared namespace and name after the
 * file; `obj_key` is the logical identity (effective namespace, `?` when the
 * deploy decides it). */
TEST(probe_k8s_identity_key) {
    static const GpfFile files[] = {{"app.yaml", "apiVersion: apps/v1\n"
                                                 "kind: Deployment\n"
                                                 "metadata:\n"
                                                 "  name: web\n"}};
    char qn[256];
    char props[512];
    ASSERT_TRUE(gpf_resource(files, 1, "Deployment/web", qn, sizeof(qn), props, sizeof(props)));
    ASSERT_STR_EQ(qn, "::apps/Deployment/-/web");
    ASSERT_NOT_NULL(strstr(props, "\"obj_key\":\"apps/Deployment/?/web\""));
    ASSERT_NOT_NULL(strstr(props, "\"kind\":\"Deployment\""));
    ASSERT_NOT_NULL(strstr(props, "\"group\":\"apps\""));
    PASS();
}

/* Core-group objects say `core`; a declared namespace is part of the QN; a
 * cluster-scoped kind's logical key has no namespace (`_`). */
TEST(probe_k8s_identity_core_namespaced_and_cluster_scoped) {
    static const GpfFile svc[] = {{"svc.yaml", "apiVersion: v1\n"
                                               "kind: Service\n"
                                               "metadata:\n"
                                               "  name: web\n"
                                               "  namespace: prod\n"}};
    static const GpfFile ns[] = {{"ns.yaml", "apiVersion: v1\n"
                                             "kind: Namespace\n"
                                             "metadata:\n"
                                             "  name: prod\n"}};
    char qn[256];
    char props[512];
    ASSERT_TRUE(gpf_resource(svc, 1, "Service/web", qn, sizeof(qn), props, sizeof(props)));
    ASSERT_STR_EQ(qn, "::core/Service/prod/web");
    ASSERT_NOT_NULL(strstr(props, "\"obj_key\":\"core/Service/prod/web\""));
    ASSERT_NOT_NULL(strstr(props, "\"namespace\":\"prod\""));
    ASSERT_TRUE(gpf_resource(ns, 1, "Namespace/prod", qn, sizeof(qn), props, sizeof(props)));
    ASSERT_NOT_NULL(strstr(props, "\"obj_key\":\"core/Namespace/_/prod\""));
    PASS();
}

/* The same comment in front of a kustomization hid its resource list:
 * home-ops resolved 1 of 348 entries. */
TEST(probe_kustomize_imports_after_leading_comment) {
    static const GpfFile files[] = {
        {"kustomization.yaml",
         "---\n"
         "# yaml-language-server: $schema=https://json.schemastore.org/kustomization\n"
         "apiVersion: kustomize.config.k8s.io/v1beta1\n"
         "kind: Kustomization\n"
         "resources:\n"
         "  - namespace.yaml\n"},
        {"namespace.yaml", "apiVersion: v1\n"
                           "kind: Namespace\n"
                           "metadata:\n"
                           "  name: network\n"}};
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.imports >= 1);
    PASS();
}

/* A commented-out entry above the first item (kubernetes/k8s.io's
 * kubernetes/apps: `# - argocd.yaml ...`) is a comment ahead of the list;
 * the entries below it are still the list. */
TEST(probe_kustomize_entries_after_commented_out_entry) {
    static const GpfFile files[] = {{"kustomization.yaml",
                                     "apiVersion: kustomize.config.k8s.io/v1beta1\n"
                                     "kind: Kustomization\n"
                                     "resources:\n"
                                     "  # - argocd.yaml applied by hand\n"
                                     "  - atlantis.yaml\n"
                                     "  - datadog.yaml\n"},
                                    {"atlantis.yaml", "apiVersion: v1\n"
                                                      "kind: Namespace\n"
                                                      "metadata:\n"
                                                      "  name: atlantis\n"},
                                    {"datadog.yaml", "apiVersion: v1\n"
                                                     "kind: Namespace\n"
                                                     "metadata:\n"
                                                     "  name: datadog\n"}};
    GpfMetrics m = gpf_metrics_files(files, 3);
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.imports, 2);
    PASS();
}

/* Same with the comment trailing the key line. */
TEST(probe_kustomize_entries_after_key_line_comment) {
    static const GpfFile files[] = {{"kustomization.yaml",
                                     "apiVersion: kustomize.config.k8s.io/v1beta1\n"
                                     "kind: Kustomization\n"
                                     "resources: # applied by Argo CD\n"
                                     "  - atlantis.yaml\n"},
                                    {"atlantis.yaml", "apiVersion: v1\n"
                                                      "kind: Namespace\n"
                                                      "metadata:\n"
                                                      "  name: atlantis\n"}};
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.imports, 1);
    PASS();
}

/* kustomize also reads an extensionless file named `Kustomization`
 * (kubermatic's addons use it); it was never discovered as a kustomization. */
TEST(probe_kustomize_extensionless_file) {
    static const GpfFile files[] = {{"Kustomization",
                                     "apiVersion: kustomize.config.k8s.io/v1beta1\n"
                                     "kind: Kustomization\n"
                                     "resources:\n"
                                     "  - namespace.yaml\n"},
                                    {"namespace.yaml", "apiVersion: v1\n"
                                                       "kind: Namespace\n"
                                                       "metadata:\n"
                                                       "  name: addons\n"}};
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.modules >= 1);
    ASSERT_TRUE(m.imports >= 1);
    PASS();
}

/* A K8s object's top-level keys (apiVersion, kind, metadata, spec, status) are
 * its envelope, carried by its Resource node; as Variables they repeated per
 * manifest (deckhouse: `kind` 1,358, `apiVersion` 1,347, `metadata` 1,336)
 * and fed name-similarity config links. */
TEST(probe_k8s_manifest_mints_no_envelope_variables) {
    GpfMetrics m = gpf_metrics("deploy.yaml", "# a manifest\n"
                                              "apiVersion: apps/v1\n"
                                              "kind: Deployment\n"
                                              "metadata:\n"
                                              "  name: web\n"
                                              "spec:\n"
                                              "  replicas: 2\n"
                                              "status: {}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.resources, 1);
    ASSERT_EQ(m.variables, 0);
    PASS();
}

/* A plain config YAML keeps its top-level keys as Variables. */
TEST(probe_yaml_config_keeps_variables) {
    GpfMetrics m = gpf_metrics("config.yaml", "server:\n"
                                              "  port: 8080\n"
                                              "kind: production\n"
                                              "replicas: 3\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.variables >= 3);
    PASS();
}

/* A `file://` dependency is the chart in that directory of this repository,
 * not a published one: `app` depends on the local `common`, and no external
 * Chart node stands in for it. `common` is read after `app`. */
TEST(probe_helm_file_dependency_links_local_chart) {
    static const GpfFile files[] = {{"charts/app/Chart.yaml", "apiVersion: v2\n"
                                                              "name: app\n"
                                                              "version: 1.0.0\n"
                                                              "dependencies:\n"
                                                              "  - name: common\n"
                                                              "    repository: file://../common\n"
                                                              "    version: 1.x\n"},
                                    {"charts/common/Chart.yaml", "apiVersion: v2\n"
                                                                 "name: common\n"
                                                                 "type: library\n"
                                                                 "version: 1.2.0\n"}};
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.charts, 2);
    ASSERT_EQ(m.depends_on, 1);
    PASS();
}

/* A `file://` path this repository cannot resolve (deckhouse names its build
 * container's `/deckhouse/...`) still declares a dependency: it stays the
 * external Chart node, the repository on its edge, rather than vanishing. */
TEST(probe_helm_unresolved_file_dependency_stays_external) {
    static const GpfFile files[] = {
        {"modules/policy/Chart.yaml",
         "apiVersion: v2\n"
         "name: policy\n"
         "version: 1.0.0\n"
         "dependencies:\n"
         "  - name: constraint-templates\n"
         "    repository: \"file:///deckhouse/modules/policy/charts/constraint_templates\"\n"}};
    GpfMetrics m = gpf_metrics_files(files, 1);
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.charts, 2);
    ASSERT_EQ(m.depends_on, 1);
    PASS();
}

/* Every external Chart as "<name> <QN, the project as <p>> <repository or ->
 * in=<edges into it>", sorted, one per line. NULL on failure; the caller
 * frees. */
static char *gpf_external_charts(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    char *out = NULL;
    if (store) {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT coalesce(group_concat(x, char(10)), '') FROM (SELECT n.name || ' ' || "
            "replace(n.qualified_name, ?1, '<p>') || ' ' || "
            "coalesce(json_extract(n.properties,'$.repository'), '-') || ' in=' || "
            "(SELECT count(*) FROM edges e WHERE e.target_id=n.id) AS x FROM nodes n "
            "WHERE n.project=?1 AND n.label='Chart' AND json_extract(n.properties,'$.external') "
            "ORDER BY x)";
        if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, lp.project, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = strdup((const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    gpf_cleanup(&lp, store);
    return out;
}

/* An external Chart is keyed by its publisher, __chart__<repository>/<name>,
 * the repository's scheme and host lower-cased, a trailing slash and `.git`
 * dropped: a Chart.yaml dependency, a kustomization, a Flux chartRef and a
 * Terraform oci:// reference naming one published chart meet in one node,
 * the same QN in every project. An oci:// repository is never cut, even when
 * it ends in the chart's name (karpenter); a chart reference is cut to its
 * repository. A repository that is no URL (an @alias, no scheme, file://,
 * credentials in the authority, a query or fragment a token may ride in, an
 * unexpanded ${...}) or that the pass does not read (a
 * Terraform `repository =`) keeps the per-project
 * <project>.__helm_dep__.<name>. The external charts a Chart.yaml depends on
 * carry its path, yet a helm_release, a helm command and a Go loader naming
 * that chart directory deploy the chart the directory declares. */
TEST(probe_external_chart_identity_is_its_publisher) {
    static const GpfFile files[] = {
        {"charts/app/Chart.yaml",
         "apiVersion: v2\nname: app\nversion: 1.0.0\ndependencies:\n"
         "  - name: redis\n    repository: https://Charts.Example.com/stable/\n"
         "    version: 1.x\n"
         "  - name: common\n    repository: \"@stable\"\n    version: 1.x\n"
         "  - name: nats\n    repository: oci://registry.example.com/charts\n"
         "    version: 1.x\n"
         "  - name: vault\n    repository: https://user@charts.example.com/private\n"
         "    version: 1.x\n"},
        {"main.tf", "resource \"helm_release\" \"app\" {\n"
                    "  name  = \"app\"\n  chart = \"./charts/app\"\n}\n\n"
                    "resource \"helm_release\" \"queue\" {\n"
                    "  name  = \"queue\"\n"
                    "  chart = \"oci://registry.example.com/charts/nats\"\n}\n\n"
                    "resource \"helm_release\" \"cache\" {\n"
                    "  name       = \"cache\"\n"
                    "  repository = \"https://charts.example.com/stable\"\n"
                    "  chart      = \"redis\"\n}\n"},
        {"deploy.sh", "#!/bin/sh\ndeploy_app() {\n  helm upgrade --install app ./charts/app\n}\n"},
        {"go.mod", "module example.com/app\n\ngo 1.22\n"},
        {"install.go", "package main\n\n"
                       "import \"helm.sh/helm/v3/pkg/chart/loader\"\n\n"
                       "func installApp() {\n\t_, _ = loader.Load(\"charts/app\")\n}\n"},
        {"obs/kustomization.yaml",
         GPF_KUST "helmCharts:\n- name: redis\n  repo: https://charts.example.com/stable.git\n"
                  "- name: karpenter\n  repo: oci://registry.example.com/karpenter\n"
                  "- name: templated\n  repo: https://charts.example.com/${ENV}\n"
                  "- name: local\n  repo: file://../charts\n"
                  "- name: bare\n  repo: charts.example.com/stable\n"
                  "- name: tokened\n  repo: https://charts.example.com/stable?token=x\n"
                  "- name: fragment\n  repo: https://charts.example.com/stable#x\n"},
        {"apps/web/kustomization.yaml",
         GPF_KUST "namespace: web\nresources:\n  - ocirepository.yaml\n  - helmrelease.yaml\n"},
        {"apps/web/ocirepository.yaml",
         "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata:\n"
         "  name: web-chart\nspec:\n  url: oci://registry.example.com/charts/nats\n"},
        {"apps/web/helmrelease.yaml",
         "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata:\n  name: web\n"
         "spec:\n  chartRef:\n    kind: OCIRepository\n    name: web-chart\n"}};
    int n = (int)(sizeof(files) / sizeof(files[0]));
    char *got = gpf_external_charts(files, n);
    static const char want[] =
        "bare <p>.__helm_dep__.bare - in=1\n"
        "common <p>.__helm_dep__.common - in=1\n"
        "fragment <p>.__helm_dep__.fragment - in=1\n"
        "karpenter __chart__oci://registry.example.com/karpenter/karpenter "
        "oci://registry.example.com/karpenter in=1\n"
        "local <p>.__helm_dep__.local - in=1\n"
        "nats __chart__oci://registry.example.com/charts/nats oci://registry.example.com/charts "
        "in=3\n"
        "redis <p>.__helm_dep__.redis - in=1\n"
        "redis __chart__https://charts.example.com/stable/redis https://charts.example.com/stable "
        "in=2\n"
        "templated <p>.__helm_dep__.templated - in=1\n"
        "tokened <p>.__helm_dep__.tokened - in=1\n"
        "vault <p>.__helm_dep__.vault - in=1";
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "external charts got:\n%s\nwant:\n%s\n", got ? got : "(null)", want);
        free(got);
        FAIL("external chart identity");
    }
    free(got);
    GPF_ASSERT_DEPLOYS(files, n,
                       "HelmRelease/web -> chart:nats(ext) (helmrelease)\n"
                       "Kustomization -> chart:bare(ext) (helmcharts)\n"
                       "Kustomization -> chart:fragment(ext) (helmcharts)\n"
                       "Kustomization -> chart:karpenter(ext) (helmcharts)\n"
                       "Kustomization -> chart:local(ext) (helmcharts)\n"
                       "Kustomization -> chart:redis(ext) (helmcharts)\n"
                       "Kustomization -> chart:templated(ext) (helmcharts)\n"
                       "Kustomization -> chart:tokened(ext) (helmcharts)\n"
                       "deploy_app -> chart:app (helm)\n"
                       "installApp -> chart:app (helm-sdk)\n"
                       "resource.helm_release.app -> chart:app (terraform)\n"
                       "resource.helm_release.cache -> chart:redis(ext) (terraform)\n"
                       "resource.helm_release.queue -> chart:nats(ext) (terraform)");
    PASS();
}

/* A remote base is outside the repository. The import resolver's symbol-name
 * guess walked its URL segments and linked kubermatic's gcp addon to the
 * `deploy` function of hack/ci/deploy.sh. A kustomize entry is a path,
 * never a symbol. */
TEST(probe_kustomize_remote_base_names_no_symbol) {
    static const GpfFile files[] = {
        {"kustomization.yaml",
         "apiVersion: kustomize.config.k8s.io/v1beta1\n"
         "kind: Kustomization\n"
         "resources:\n"
         "  - https://github.com/example/csi-driver.git/deploy/kubernetes/base\n"},
        {"hack/deploy.sh", "#!/usr/bin/env bash\n"
                           "deploy() {\n"
                           "  echo deploying\n"
                           "}\n"},
        /* Same name as the URL's last segment: the sibling lookup's basename
         * candidate must not bind a remote base to it either. */
        {"base.yaml", "apiVersion: v1\n"
                      "kind: ConfigMap\n"
                      "metadata:\n"
                      "  name: local\n"}};
    GpfMetrics m = gpf_metrics_files(files, 3);
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.imports, 0);
    PASS();
}

/* The k8s pass minted a second, edgeless Module per kustomization next to
 * the file's own Module: its import lookup took entries as repo-root paths
 * and resolved none of them in any of 17 infra repositories. */
TEST(probe_kustomize_single_module) {
    GpfMetrics m = gpf_metrics("kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1beta1\n"
                                                     "kind: Kustomization\n"
                                                     "resources:\n"
                                                     "  - deploy.yaml\n");
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.modules, 1);
    PASS();
}

/* Entries are relative to the kustomization's own directory; in a nested
 * overlay they resolve through the sibling lookup, not the repo root. */
TEST(probe_kustomize_nested_sibling_entries) {
    static const GpfFile files[] = {
        {"apps/base/podinfo/kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1beta1\n"
                                                 "kind: Kustomization\n"
                                                 "resources:\n"
                                                 "  - namespace.yaml\n"
                                                 "  - release.yaml\n"},
        {"apps/base/podinfo/namespace.yaml", "apiVersion: v1\n"
                                             "kind: Namespace\n"
                                             "metadata:\n"
                                             "  name: podinfo\n"},
        {"apps/base/podinfo/release.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\n"
                                           "kind: HelmRelease\n"
                                           "metadata:\n"
                                           "  name: podinfo\n"}};
    GpfMetrics m = gpf_metrics_files(files, 3);
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.imports, 2);
    PASS();
}

/* The label scan stopped at a leading `---`, so a Service written that way
 * had no selector and linked to nothing. The workload's name differs from the
 * selector value, so only the pod-template label can match. */
TEST(probe_k8s_selector_after_leading_separator) {
    static const GpfFile files[] = {{"service.yaml", "---\n"
                                                     "apiVersion: v1\n"
                                                     "kind: Service\n"
                                                     "metadata:\n"
                                                     "  name: web\n"
                                                     "spec:\n"
                                                     "  selector:\n"
                                                     "    app: web-pods\n"},
                                    {"deploy.yaml", "apiVersion: apps/v1\n"
                                                    "kind: Deployment\n"
                                                    "metadata:\n"
                                                    "  name: web-deploy\n"
                                                    "spec:\n"
                                                    "  template:\n"
                                                    "    metadata:\n"
                                                    "      labels:\n"
                                                    "        app: web-pods\n"}};
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.infra_maps, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 4 — Nix (.nix)
 *
 * Labels histogram: Module:1
 * Nix is a purely functional language; the grammar-only extractor produces
 * a single Module node per file.  Nix `import` expressions (e.g.
 * `import ./other.nix`) are textual — the pipeline does not yet resolve
 * them into IMPORTS graph edges.
 * ══════════════════════════════════════════════════════════════════ */

/* Nix: attribute set with derivation-like attrs → at least 1 node. */
TEST(probe_nix_attrset_node) {
    GpfMetrics m = gpf_metrics("default.nix", "{ pkgs ? import <nixpkgs> {} }:\n"
                                              "\n"
                                              "pkgs.stdenv.mkDerivation {\n"
                                              "  pname = \"mypackage\";\n"
                                              "  version = \"1.0.0\";\n"
                                              "  src = ./src;\n"
                                              "  buildInputs = [ pkgs.gcc pkgs.make ];\n"
                                              "  buildPhase = \"make\";\n"
                                              "  installPhase = \"make install\";\n"
                                              "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: any non-empty Nix file produces at least a Module node. */
    ASSERT_TRUE(m.total_nodes >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Nix: let-binding expression — multiple top-level attrs. */
TEST(probe_nix_let_binding) {
    GpfMetrics m = gpf_metrics("lib.nix", "let\n"
                                          "  double = x: x * 2;\n"
                                          "  square = x: x * x;\n"
                                          "  cube   = x: x * x * x;\n"
                                          "in {\n"
                                          "  inherit double square cube;\n"
                                          "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: file-level Module node created. */
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* The literal Nix dependency is genuine; its missing binding is quality debt,
 * while any guessed import target is a bug. */
TEST(probe_nix_import_edge) {
    static const GpfFile files[] = {{"utils.nix", "{ double = x: x * 2; }\n"},
                                    {"default.nix", "let utils = import ./utils.nix;\n"
                                                    "in { result = utils.double 21; }\n"}};
    const RQ_Source sources[] = {{.path = "utils.nix", .text = files[0].content},
                                 {.path = "default.nix", .text = files[1].content}};
    const RQ_Expected gap = {
        .case_id = "nix-literal-import",
        .site = {.file = "default.nix",
                 .caller = "default.nix.__file__",
                 .needle = "import ./utils.nix"},
        .relation = "IMPORTS",
        .target = {.file = "utils.nix",
                   .suffix = "utils.nix.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "{ double = x: x * 2; }"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "nix_literal_import_binding_unproven",
        .why = "The literal import names the sibling Nix source, but Nix IMPORTS has no audited "
               "language-binding adapter.",
        .tried = "Kept the original import and target unchanged. The capability row explicitly "
                 "abstains; basename and global symbol matches cannot establish the binding."};
    RQ_Report report;
    ASSERT_TRUE(
        gpf_literal_import_quality(files, sources, "probe_nix_import_edge", &gap, NULL, &report));
    ASSERT_EQ(report.genuine, 2);
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 5 — Nickel (.ncl)
 *
 * Labels histogram: Module:1
 * Nickel is a configuration language; the extractor produces a single Module
 * node per file.  Import with `import "path"` is syntactically supported but
 * not yet resolved into IMPORTS edges.
 * ══════════════════════════════════════════════════════════════════ */

/* Nickel: record literal → Module node (no crash). */
TEST(probe_nickel_record_node) {
    GpfMetrics m = gpf_metrics("config.ncl", "{\n"
                                             "  server = {\n"
                                             "    host = \"localhost\",\n"
                                             "    port = 8080,\n"
                                             "  },\n"
                                             "  database = {\n"
                                             "    name = \"mydb\",\n"
                                             "    max_connections = 10,\n"
                                             "  },\n"
                                             "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: any non-empty Nickel file produces a Module node. */
    ASSERT_TRUE(m.total_nodes >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Nickel: let-binding with function value. */
TEST(probe_nickel_let_fn) {
    GpfMetrics m = gpf_metrics("lib.ncl", "let double = fun x => x * 2 in\n"
                                          "let square = fun x => x * x in\n"
                                          "{ double, square }\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: file-level Module node. */
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Count this genuine import as a named quality gap until its source and
 * path-scope proof is implemented. Any guessed target remains a bug. */
TEST(probe_nickel_import_edge) {
    static const GpfFile files[] = {{"utils.ncl", "{ double = fun x => x * 2 }\n"},
                                    {"main.ncl", "let utils = import \"utils.ncl\" in\n"
                                                 "{ result = utils.double 21 }\n"}};

    const RQ_Source sources[] = {
        {.path = "utils.ncl", .text = files[0].content},
        {.path = "main.ncl", .text = files[1].content},
    };
    const RQ_Expected gap = {
        .case_id = "nickel_import_edge",
        .site = {.file = "main.ncl",
                 .caller = "main.ncl.__file__",
                 .needle = "import \"utils.ncl\""},
        .relation = "IMPORTS",
        .target = {.file = "utils.ncl",
                   .suffix = "utils.ncl.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "{ double = fun x => x * 2 }"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "nickel_literal_import_binding_unproven",
        .why =
            "The relative Nickel import has no audited source occurrence and exact File binding.",
        .tried =
            "Kept the complete import expression and sibling source. The Nickel carrier lacks the "
            "audited literal-relative-path kind; a matching basename cannot authorize this edge."};
    RQ_Report report;
    ASSERT_TRUE(gpf_literal_import_quality(files, sources, "probe_nickel_import_edge", &gap, NULL,
                                           &report));
    ASSERT_EQ(report.genuine, 2); /* original import + required control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 6 — Pkl (.pkl)
 *
 * Labels histogram: Module:1
 * Pkl is Apple's configuration language.  `amends`/`import` for cross-file
 * references — not yet resolved by the pipeline.
 * ══════════════════════════════════════════════════════════════════ */

/* Pkl: property assignments → Module node. */
TEST(probe_pkl_properties_node) {
    GpfMetrics m = gpf_metrics("config.pkl", "host = \"localhost\"\n"
                                             "port = 8080\n"
                                             "debug = false\n"
                                             "maxConnections = 10\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: any non-empty Pkl file produces at least a Module node. */
    ASSERT_TRUE(m.total_nodes >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Pkl: class-like object definition. */
TEST(probe_pkl_object_block) {
    GpfMetrics m = gpf_metrics("server.pkl", "server {\n"
                                             "  host = \"0.0.0.0\"\n"
                                             "  port = 443\n"
                                             "  tls {\n"
                                             "    enabled = true\n"
                                             "    cert = \"/etc/ssl/cert.pem\"\n"
                                             "  }\n"
                                             "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: Module node. */
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Count this genuine import as a named quality gap until its source and
 * path-scope proof is implemented. Any guessed target remains a bug. */
TEST(probe_pkl_import_edge) {
    static const GpfFile files[] = {{"base.pkl", "host = \"localhost\"\n"
                                                 "port = 8080\n"},
                                    {"prod.pkl", "amends \"base.pkl\"\n"
                                                 "\n"
                                                 "port = 443\n"}};

    const RQ_Source sources[] = {
        {.path = "base.pkl", .text = files[0].content},
        {.path = "prod.pkl", .text = files[1].content},
    };
    const RQ_Expected gap = {
        .case_id = "pkl_import_edge",
        .site = {.file = "prod.pkl",
                 .caller = "prod.pkl.__file__",
                 .needle = "amends \"base.pkl\""},
        .relation = "IMPORTS",
        .target = {.file = "base.pkl",
                   .suffix = "base.pkl.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "host = \"localhost\""},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "pkl_amends_module_binding_unproven",
        .why = "The relative amends module URI lacks an audited source-to-File binding.",
        .tried = "Kept the amends clause and complete base module. The generic Pkl carrier does "
                 "not certify the URI scope or a raw source span."};
    RQ_Report report;
    ASSERT_TRUE(
        gpf_literal_import_quality(files, sources, "probe_pkl_import_edge", &gap, NULL, &report));
    ASSERT_EQ(report.genuine, 2); /* original import + required control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 7 — RON (.ron)
 *
 * Labels histogram: Module:1
 * RON (Rusty Object Notation) is a data serialization format; the extractor
 * produces a Module node per file.  No import mechanism exists in RON.
 * ══════════════════════════════════════════════════════════════════ */

/* RON: struct-like record → Module node. */
TEST(probe_ron_record_node) {
    GpfMetrics m = gpf_metrics("config.ron", "(\n"
                                             "  host: \"localhost\",\n"
                                             "  port: 8080,\n"
                                             "  debug: false,\n"
                                             "  tags: [\"api\", \"v2\"],\n"
                                             ")\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: RON file produces at least 1 node. */
    ASSERT_TRUE(m.total_nodes >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* RON: named struct. */
TEST(probe_ron_named_struct) {
    GpfMetrics m = gpf_metrics("scene.ron", "Scene(\n"
                                            "  entities: [\n"
                                            "    Entity(id: 1, name: \"player\"),\n"
                                            "    Entity(id: 2, name: \"enemy\"),\n"
                                            "  ],\n"
                                            ")\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: Module node for the RON file. */
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* RON: no import mechanism → 0 IMPORTS edges expected. */
TEST(probe_ron_no_imports) {
    GpfMetrics m = gpf_metrics("data.ron", "[\n"
                                           "  (key: \"a\", value: 1),\n"
                                           "  (key: \"b\", value: 2),\n"
                                           "]\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: RON has no import syntax; pipeline emits 0 IMPORTS edges. */
    ASSERT_TRUE(m.imports == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 8 — Jsonnet (.jsonnet, .libsonnet)
 *
 * Labels histogram: Module:1
 * Jsonnet is a data templating language.  `import`/`importstr` for cross-file
 * references — not yet resolved by the pipeline.
 * ══════════════════════════════════════════════════════════════════ */

/* Jsonnet: object literal → Module node. */
TEST(probe_jsonnet_object_node) {
    GpfMetrics m = gpf_metrics("config.jsonnet", "{\n"
                                                 "  local env = \"prod\",\n"
                                                 "  host: \"api.example.com\",\n"
                                                 "  port: 443,\n"
                                                 "  debug: env == \"dev\",\n"
                                                 "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: any non-empty Jsonnet file produces at least a Module node. */
    ASSERT_TRUE(m.total_nodes >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Jsonnet: function definition in object. */
TEST(probe_jsonnet_function_in_object) {
    GpfMetrics m = gpf_metrics("lib.jsonnet", "{\n"
                                              "  double(x):: x * 2,\n"
                                              "  square(x):: x * x,\n"
                                              "  greet(name):: \"Hello, \" + name,\n"
                                              "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: Module node. */
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Count this genuine import as a named quality gap until its source and
 * path-scope proof is implemented. Any guessed target remains a bug. */
TEST(probe_jsonnet_import_edge) {
    static const GpfFile files[] = {{"utils.libsonnet", "{\n"
                                                        "  double(x):: x * 2,\n"
                                                        "}\n"},
                                    {"main.jsonnet", "local utils = import 'utils.libsonnet';\n"
                                                     "{\n"
                                                     "  result: utils.double(21),\n"
                                                     "}\n"}};

    const RQ_Source sources[] = {
        {.path = "utils.libsonnet", .text = files[0].content},
        {.path = "main.jsonnet", .text = files[1].content},
    };
    const RQ_Expected gap = {
        .case_id = "jsonnet_import_edge",
        .site = {.file = "main.jsonnet",
                 .caller = "main.jsonnet.__file__",
                 .needle = "import 'utils.libsonnet'"},
        .relation = "IMPORTS",
        .target = {.file = "utils.libsonnet",
                   .suffix = "utils.libsonnet.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "{"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "jsonnet_literal_import_binding_unproven",
        .why = "The literal Jsonnet import has no audited importer-relative source binding.",
        .tried = "Kept the imported library and exact import expression. The Jsonnet path carrier "
                 "lacks the audited literal-relative-path kind; name matching is insufficient."};
    RQ_Report report;
    ASSERT_TRUE(gpf_literal_import_quality(files, sources, "probe_jsonnet_import_edge", &gap, NULL,
                                           &report));
    ASSERT_EQ(report.genuine, 2); /* original import + required control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 9 — KDL (.kdl)
 *
 * Labels histogram: Module:1
 * KDL is a document language.  No import mechanism; single Module per file.
 * ══════════════════════════════════════════════════════════════════ */

/* KDL: node with children → Module node. */
TEST(probe_kdl_node_block) {
    GpfMetrics m = gpf_metrics("config.kdl", "server {\n"
                                             "  host \"localhost\"\n"
                                             "  port 8080\n"
                                             "}\n"
                                             "\n"
                                             "database {\n"
                                             "  host \"db.internal\"\n"
                                             "  port 5432\n"
                                             "  name \"mydb\"\n"
                                             "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: KDL file produces at least a Module node. */
    ASSERT_TRUE(m.total_nodes >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* KDL: flat node list (key-value style). */
TEST(probe_kdl_flat_nodes) {
    GpfMetrics m =
        gpf_metrics("manifest.kdl", "package \"my-package\" version=\"1.0.0\"\n"
                                    "description \"A test package\"\n"
                                    "author \"Test Author\" email=\"test@example.com\"\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: Module node. */
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* KDL: no import mechanism → 0 IMPORTS edges. */
TEST(probe_kdl_no_imports) {
    GpfMetrics m = gpf_metrics("data.kdl", "items {\n"
                                           "  item id=1 name=\"alpha\"\n"
                                           "  item id=2 name=\"beta\"\n"
                                           "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: KDL has no import syntax; 0 IMPORTS edges expected. */
    ASSERT_TRUE(m.imports == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 10 — Hyprlang (.conf, .hl, hyprland.conf)
 *
 * Labels histogram: Module:1
 * Hyprlang is the Hyprland compositor config language.  `source` directive
 * for including other files — not resolved by the pipeline.
 * ══════════════════════════════════════════════════════════════════ */

/* Hyprlang: section block → Module node. */
TEST(probe_hyprlang_section_node) {
    GpfMetrics m = gpf_metrics("hyprland.conf", "general {\n"
                                                "  gaps_in = 5\n"
                                                "  gaps_out = 10\n"
                                                "  border_size = 2\n"
                                                "  col.active_border = rgba(33ccffee)\n"
                                                "}\n"
                                                "\n"
                                                "decoration {\n"
                                                "  rounding = 10\n"
                                                "  blur {\n"
                                                "    enabled = true\n"
                                                "    size = 3\n"
                                                "  }\n"
                                                "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: any non-empty Hyprlang file produces at least a Module node. */
    ASSERT_TRUE(m.total_nodes >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Hyprlang: keybind and monitor directives. */
TEST(probe_hyprlang_directives) {
    GpfMetrics m = gpf_metrics("binds.hl", "bind = $mainMod, Q, killactive\n"
                                           "bind = $mainMod, M, exit\n"
                                           "bind = $mainMod, F, fullscreen\n"
                                           "monitor = DP-1, 1920x1080@144, 0x0, 1\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: Module node for any non-empty .hl file. */
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Hyprlang: HOME-qualified source must not bind to a repository basename. */
TEST(probe_hyprlang_source_import_edge) {
    static const GpfFile files[] = {{"colors.conf", "col.active_border = rgba(33ccffee)\n"
                                                    "col.inactive_border = rgba(595959aa)\n"},
                                    {"hyprland.conf", "source = ~/.config/hypr/colors.conf\n"
                                                      "\n"
                                                      "general {\n"
                                                      "  gaps_in = 5\n"
                                                      "}\n"}};

    const RQ_Source sources[] = {
        {.path = "colors.conf", .text = files[0].content},
        {.path = "hyprland.conf", .text = files[1].content},
    };
    /* The original HOME-qualified source is outside this project.
     * The same-basename local file is a forbidden target, not quality debt. */
    const RQ_Forbidden forbidden = {
        .case_id = "hyprlang-home-source-not-repo-basename",
        .site = {.file = "hyprland.conf",
                 .caller = "hyprland.conf.__file__",
                 .needle = "source = ~/.config/hypr/colors.conf"},
        .relation = "IMPORTS",
        .target = {.file = "colors.conf",
                   .suffix = "colors.conf.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "col.active_border = rgba(33ccffee)"}};
    RQ_Report report;
    ASSERT_TRUE(gpf_literal_import_quality(files, sources, "probe_hyprlang_source_import_edge",
                                           NULL, &forbidden, &report));
    ASSERT_EQ(report.genuine, 1); /* required control; HOME target is outside the project */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 11 — DeviceTree (.dts, .dtsi, .overlay)
 *
 * Labels histogram: Module:1
 * DeviceTree Source describes hardware topology; node blocks like `/ { ... }`
 * or named nodes like `cpu@0 { ... }`.  `#include` is preprocessor-level;
 * not modeled as IMPORTS in the pipeline.
 * ══════════════════════════════════════════════════════════════════ */

/* DeviceTree: minimal DTS with root node → Module node. */
TEST(probe_devicetree_root_node) {
    GpfMetrics m = gpf_metrics("board.dts", "/dts-v1/;\n"
                                            "\n"
                                            "/ {\n"
                                            "  compatible = \"vendor,board\";\n"
                                            "  model = \"Vendor Board v1\";\n"
                                            "  #address-cells = <1>;\n"
                                            "  #size-cells = <1>;\n"
                                            "};\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: any non-empty DTS file produces at least a Module node. */
    ASSERT_TRUE(m.total_nodes >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* DeviceTree: named child nodes (cpu, memory). */
TEST(probe_devicetree_child_nodes) {
    GpfMetrics m = gpf_metrics("soc.dts", "/dts-v1/;\n"
                                          "\n"
                                          "/ {\n"
                                          "  cpus {\n"
                                          "    #address-cells = <1>;\n"
                                          "    #size-cells = <0>;\n"
                                          "    cpu@0 {\n"
                                          "      compatible = \"arm,cortex-a53\";\n"
                                          "      reg = <0>;\n"
                                          "    };\n"
                                          "    cpu@1 {\n"
                                          "      compatible = \"arm,cortex-a53\";\n"
                                          "      reg = <1>;\n"
                                          "    };\n"
                                          "  };\n"
                                          "  memory@80000000 {\n"
                                          "    reg = <0x80000000 0x40000000>;\n"
                                          "  };\n"
                                          "};\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: Module node (child nodes not individually extracted in grammar-only path). */
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Count this genuine import as a named quality gap until its source and
 * path-scope proof is implemented. Any guessed target remains a bug. */
TEST(probe_devicetree_include_edge) {
    static const GpfFile files[] = {{"soc.dtsi", "/dts-v1/;\n"
                                                 "/ {\n"
                                                 "  soc: soc {\n"
                                                 "    compatible = \"simple-bus\";\n"
                                                 "  };\n"
                                                 "};\n"},
                                    {"board.dts", "/dts-v1/;\n"
                                                  "#include \"soc.dtsi\"\n"
                                                  "\n"
                                                  "/ {\n"
                                                  "  model = \"My Board\";\n"
                                                  "};\n"}};

    const RQ_Source sources[] = {
        {.path = "soc.dtsi", .text = files[0].content},
        {.path = "board.dts", .text = files[1].content},
    };
    const RQ_Expected gap = {
        .case_id = "devicetree_include_edge",
        .site = {.file = "board.dts",
                 .caller = "board.dts.__file__",
                 .needle = "#include \"soc.dtsi\""},
        .relation = "IMPORTS",
        .target = {.file = "soc.dtsi",
                   .suffix = "soc.dtsi.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "/dts-v1/;"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "devicetree_preprocessor_include_binding_unproven",
        .why = "The intended quoted preprocessor include needs an audited include context and "
               "exact File identity.",
        .tried =
            "Kept the board directive and sibling fragment. DeviceTree has no dedicated audited "
            "import dispatch, and best-effort preprocessing cannot certify this include edge."};
    RQ_Report report;
    ASSERT_TRUE(gpf_literal_import_quality(files, sources, "probe_devicetree_include_edge", &gap,
                                           NULL, &report));
    ASSERT_EQ(report.genuine, 2); /* original import + required control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 12 — CMake (CMakeLists.txt, .cmake)
 *
 * Labels histogram: Function:1, Module:1
 * CMake's grammar extractor produces a Function node per defined function/macro
 * plus a Module for the file.  `include()` and `add_subdirectory()` are
 * candidates for IMPORTS edges but not yet resolved.
 * ══════════════════════════════════════════════════════════════════ */

/* CMake: function definition → Function node. */
TEST(probe_cmake_function_node) {
    GpfMetrics m = gpf_metrics("CMakeLists.txt", "cmake_minimum_required(VERSION 3.16)\n"
                                                 "project(MyProject)\n"
                                                 "\n"
                                                 "function(add_test_target name)\n"
                                                 "  add_executable(${name} ${name}.cpp)\n"
                                                 "  target_link_libraries(${name} PRIVATE gtest)\n"
                                                 "endfunction()\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: CMake function() definition must produce a Function node. */
    ASSERT_TRUE(m.functions >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* CMake: macro definition → Function node (macros map to Function label). */
TEST(probe_cmake_macro_node) {
    GpfMetrics m = gpf_metrics("helpers.cmake", "macro(print_var varname)\n"
                                                "  message(STATUS \"${varname} = ${${varname}}\")\n"
                                                "endmacro()\n"
                                                "\n"
                                                "function(setup_target name)\n"
                                                "  add_library(${name} STATIC)\n"
                                                "endfunction()\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both function() and macro() produce Function nodes. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Count this genuine import as a named quality gap until its source and
 * path-scope proof is implemented. Any guessed target remains a bug. */
TEST(probe_cmake_include_edge) {
    static const GpfFile files[] = {{"helpers.cmake",
                                     "function(check_compiler)\n"
                                     "  message(STATUS \"Compiler: ${CMAKE_CXX_COMPILER_ID}\")\n"
                                     "endfunction()\n"},
                                    {"CMakeLists.txt", "cmake_minimum_required(VERSION 3.16)\n"
                                                       "project(Foo)\n"
                                                       "include(helpers.cmake)\n"
                                                       "check_compiler()\n"}};

    const RQ_Source sources[] = {
        {.path = "helpers.cmake", .text = files[0].content},
        {.path = "CMakeLists.txt", .text = files[1].content},
    };
    const RQ_Expected gap = {
        .case_id = "cmake_include_edge",
        .site = {.file = "CMakeLists.txt",
                 .caller = "CMakeLists.txt.__file__",
                 .needle = "include(helpers.cmake)"},
        .relation = "IMPORTS",
        .target = {.file = "helpers.cmake",
                   .suffix = "helpers.cmake.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "function(check_compiler)"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "cmake_literal_include_source_scope_unproven",
        .why = "The explicit include file needs a certified CMake source-directory binding.",
        .tried = "Kept the complete include command, project setup, and helper file. The generic "
                 "command carrier does not prove source scope or the exact raw import occurrence."};
    RQ_Report report;
    ASSERT_TRUE(gpf_literal_import_quality(files, sources, "probe_cmake_include_edge", &gap, NULL,
                                           &report));
    ASSERT_EQ(report.genuine, 2); /* original import + required control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* Count this genuine import as a named quality gap until its source and
 * path-scope proof is implemented. Any guessed target remains a bug. */
TEST(probe_cmake_subdirectory_edge) {
    static const GpfFile files[] = {{"CMakeLists.txt", "cmake_minimum_required(VERSION 3.16)\n"
                                                       "project(Root)\n"
                                                       "add_subdirectory(lib)\n"},
                                    {"lib/CMakeLists.txt",
                                     "add_library(mylib STATIC mylib.cpp)\n"
                                     "\n"
                                     "function(mylib_configure target)\n"
                                     "  target_link_libraries(${target} PRIVATE mylib)\n"
                                     "endfunction()\n"}};

    const RQ_Source sources[] = {
        {.path = "CMakeLists.txt", .text = files[0].content},
        {.path = "lib/CMakeLists.txt", .text = files[1].content},
    };
    const RQ_Expected gap = {.case_id = "cmake_subdirectory_edge",
                             .site = {.file = "CMakeLists.txt",
                                      .caller = "CMakeLists.txt.__file__",
                                      .needle = "add_subdirectory(lib)"},
                             .relation = "IMPORTS",
                             .target = {.file = "lib/CMakeLists.txt",
                                        .suffix = "lib.CMakeLists.txt.__file__",
                                        .label = "File",
                                        .declaration_line = 1,
                                        .declaration_text = "add_library(mylib STATIC mylib.cpp)"},
                             .mode = RQ_KNOWN_GAP,
                             .gap_id = "cmake_subdirectory_entry_file_unproven",
                             .why = "The subdirectory command must bind to the exact child "
                                    "CMakeLists.txt under the configured source directory.",
                             .tried = "Kept both original build files and the lib argument. The "
                                      "command carrier has no certified directory-to-entry-file "
                                      "rule; a Folder or same-name Module is not the target."};
    RQ_Report report;
    ASSERT_TRUE(gpf_literal_import_quality(files, sources, "probe_cmake_subdirectory_edge", &gap,
                                           NULL, &report));
    ASSERT_EQ(report.genuine, 2); /* original import + required control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 13 — Makefile (Makefile, .mk, GNUmakefile)
 *
 * Labels histogram: Function:1, Module:1
 * The Makefile extractor produces a Function node for targets and a Module for
 * the file.  `include` directive is a candidate for IMPORTS but not resolved.
 * ══════════════════════════════════════════════════════════════════ */

/* Makefile: phony targets → Function nodes. */
TEST(probe_makefile_target_nodes) {
    GpfMetrics m = gpf_metrics("Makefile", ".PHONY: all build test clean\n"
                                           "\n"
                                           "all: build\n"
                                           "\n"
                                           "build:\n"
                                           "\tgcc -o myapp main.c utils.c\n"
                                           "\n"
                                           "test:\n"
                                           "\t./run_tests.sh\n"
                                           "\n"
                                           "clean:\n"
                                           "\trm -f myapp *.o\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: at least the first target must produce a Function node. */
    ASSERT_TRUE(m.functions >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Makefile: pattern rules and variables. */
TEST(probe_makefile_pattern_rule) {
    GpfMetrics m = gpf_metrics("rules.mk", "CC = gcc\n"
                                           "CFLAGS = -Wall -Wextra -O2\n"
                                           "\n"
                                           "%.o: %.c\n"
                                           "\t$(CC) $(CFLAGS) -c $< -o $@\n"
                                           "\n"
                                           "install:\n"
                                           "\tcp myapp /usr/local/bin/\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: Module node for any non-empty Makefile. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* Count this genuine import as a named quality gap until its source and
 * path-scope proof is implemented. Any guessed target remains a bug. */
TEST(probe_makefile_include_edge) {
    static const GpfFile files[] = {{"common.mk", "CC = gcc\n"
                                                  "CFLAGS = -Wall -O2\n"
                                                  "\n"
                                                  "clean:\n"
                                                  "\trm -f *.o\n"},
                                    {"Makefile", "include common.mk\n"
                                                 "\n"
                                                 "all:\n"
                                                 "\t$(CC) $(CFLAGS) -o myapp main.c\n"}};

    const RQ_Source sources[] = {
        {.path = "common.mk", .text = files[0].content},
        {.path = "Makefile", .text = files[1].content},
    };
    const RQ_Expected gap = {
        .case_id = "makefile_include_edge",
        .site = {.file = "Makefile", .caller = "Makefile.__file__", .needle = "include common.mk"},
        .relation = "IMPORTS",
        .target = {.file = "common.mk",
                   .suffix = "common.mk.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "CC = gcc"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "make_include_invocation_scope_unproven",
        .why = "The intended root-invoked Make include needs working-directory and include-search "
               "context.",
        .tried = "Kept the literal include, shared definitions, and recipes. The include carrier "
                 "does not certify the invocation root or search path, so co-location alone cannot "
                 "authorize the edge."};
    RQ_Report report;
    ASSERT_TRUE(gpf_literal_import_quality(files, sources, "probe_makefile_include_edge", &gap,
                                           NULL, &report));
    ASSERT_EQ(report.genuine, 2); /* original import + required control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 14 — Meson (meson.build, meson.options)
 *
 * Labels histogram: Module:1
 * Meson's grammar-only extractor produces a Module node per file.  Individual
 * build targets (executable(), library()) are not extracted as distinct nodes.
 * `subdir()` maps to IMPORTS conceptually but is not resolved.
 * ══════════════════════════════════════════════════════════════════ */

/* Meson: project + executable → Module node. */
TEST(probe_meson_project_node) {
    GpfMetrics m = gpf_metrics("meson.build", "project('myapp', 'c',\n"
                                              "  version: '1.0.0',\n"
                                              "  default_options: ['warning_level=2'])\n"
                                              "\n"
                                              "executable('myapp',\n"
                                              "  sources: ['main.c', 'utils.c'],\n"
                                              "  install: true)\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: any non-empty meson.build produces at least a Module node. */
    ASSERT_TRUE(m.total_nodes >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Meson: options file → Module node. */
TEST(probe_meson_options_node) {
    GpfMetrics m = gpf_metrics(
        "meson.options", "option('enable_tests', type: 'boolean', value: true,\n"
                         "  description: 'Build and run tests')\n"
                         "option('build_type', type: 'combo', choices: ['debug', 'release'],\n"
                         "  value: 'release')\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: meson.options also produces a Module node. */
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* A complete first project() declaration preserves the unconditional subdir.
 * Require the physical File target and persisted outcome/edge agreement. */
TEST(probe_meson_subdir_edge) {
    static const GpfFile files[] = {{"meson.build", "project('root', 'c')\n"
                                                    "subdir('lib')\n"},
                                    {"lib/meson.build", "static_library('mylib', 'mylib.c')\n"}};
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, 2);
    bool ok = store != NULL;
    sqlite3_stmt *row = NULL;
    /* The carrier records the literal's exact occurrence. */
    const char *literal = strstr(files[0].content, "'lib'");
    const char *sql = "SELECT count(*) FROM resolver_occurrences r JOIN nodes s ON "
                      "s.project=r.project AND s.qualified_name=r.scope JOIN nodes t ON "
                      "t.project=r.project AND t.qualified_name=r.target JOIN edges e ON "
                      "e.source_id=s.id AND e.target_id=t.id AND e.type=r.relation "
                      "WHERE r.project=?1 AND r.relation='IMPORTS' AND r.emitted=1 "
                      "AND r.start_byte=?2 AND r.end_byte=?3 AND s.file_path='meson.build' "
                      "AND s.label='File' AND t.file_path='lib/meson.build' AND t.label='File'";
    if (ok) {
        ok = sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &row, NULL) == SQLITE_OK &&
             sqlite3_bind_text(row, 1, lp.project, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
             sqlite3_bind_int(row, 2, (int)(literal - files[0].content)) == SQLITE_OK &&
             sqlite3_bind_int(row, 3, (int)(literal - files[0].content) + 5) == SQLITE_OK &&
             sqlite3_step(row) == SQLITE_ROW && sqlite3_column_int(row, 0) == 1 &&
             cbm_store_count_edges_by_type(store, lp.project, "IMPORTS") == 1;
    }
    sqlite3_finalize(row);
    gpf_cleanup(&lp, store);
    ASSERT_TRUE(ok);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 15 — GN (.gn, .gni)
 *
 * Labels histogram: Module:1
 * GN (Generate Ninja) build system for Chromium-like projects.  The extractor
 * produces a Module node per file.  `import()` for shared .gni files is a
 * candidate for IMPORTS but not yet resolved.
 * ══════════════════════════════════════════════════════════════════ */

/* GN: executable target → Module node (individual targets not extracted). */
TEST(probe_gn_executable_node) {
    GpfMetrics m = gpf_metrics("BUILD.gn", "executable(\"myapp\") {\n"
                                           "  sources = [\n"
                                           "    \"main.cc\",\n"
                                           "    \"utils.cc\",\n"
                                           "  ]\n"
                                           "  deps = [ \"//third_party/base\" ]\n"
                                           "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: any non-empty .gn file produces at least a Module node. */
    ASSERT_TRUE(m.total_nodes >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* GN: shared_library and static_library targets. */
TEST(probe_gn_library_targets) {
    GpfMetrics m = gpf_metrics("lib.gn", "static_library(\"crypto\") {\n"
                                         "  sources = [ \"crypto.cc\", \"sha256.cc\" ]\n"
                                         "}\n"
                                         "\n"
                                         "shared_library(\"net\") {\n"
                                         "  sources = [ \"net.cc\", \"socket.cc\" ]\n"
                                         "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: Module node. */
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Count this genuine import as a named quality gap until its source and
 * path-scope proof is implemented. Any guessed target remains a bug. */
TEST(probe_gn_import_edge) {
    static const GpfFile files[] = {{"//build/common.gni",
                                     "template(\"cc_binary\") {\n"
                                     "  executable(target_name) {\n"
                                     "    forward_variables_from(invoker, \"*\")\n"
                                     "  }\n"
                                     "}\n"},
                                    {"BUILD.gn", "import(\"//build/common.gni\")\n"
                                                 "\n"
                                                 "cc_binary(\"myapp\") {\n"
                                                 "  sources = [ \"main.cc\" ]\n"
                                                 "}\n"}};

    const RQ_Source sources[] = {
        {.path = "build/common.gni", .text = files[0].content},
        {.path = "BUILD.gn", .text = files[1].content},
    };
    const RQ_Expected gap = {.case_id = "gn_import_edge",
                             .site = {.file = "BUILD.gn",
                                      .caller = "BUILD.gn.__file__",
                                      .needle = "import(\"//build/common.gni\")"},
                             .relation = "IMPORTS",
                             .target = {.file = "build/common.gni",
                                        .suffix = "build.common.gni.__file__",
                                        .label = "File",
                                        .declaration_line = 1,
                                        .declaration_text = "template(\"cc_binary\") {"},
                             .mode = RQ_KNOWN_GAP,
                             .gap_id = "gn_source_root_import_binding_unproven",
                             .why = "The GN source-root path needs an explicit source-root binding "
                                    "and audited import occurrence.",
                             .tried =
                                 "Preserved the original //build/common.gni fixture spelling; the "
                                 "writer places those bytes at build/common.gni under the test "
                                 "root. No .gn root witness or audited GN import kind exists."};
    RQ_Report report;
    ASSERT_TRUE(
        gpf_literal_import_quality(files, sources, "probe_gn_import_edge", &gap, NULL, &report));
    ASSERT_EQ(report.genuine, 2); /* original import + required control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 16 — Just (justfile, Justfile, .justfile)
 *
 * Labels histogram: Module:1
 * Just is a command runner.  Recipes are the build units; the grammar-only
 * extractor currently produces only a Module node per file (recipes not
 * individually extracted as Function nodes per the histogram).
 * ══════════════════════════════════════════════════════════════════ */

/* Just: recipe definitions → at least Module node. */
TEST(probe_just_recipe_nodes) {
    GpfMetrics m = gpf_metrics("justfile", "build:\n"
                                           "  cargo build --release\n"
                                           "\n"
                                           "test:\n"
                                           "  cargo test\n"
                                           "\n"
                                           "clean:\n"
                                           "  cargo clean\n"
                                           "\n"
                                           "fmt:\n"
                                           "  cargo fmt\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: any non-empty justfile produces at least a Module node. */
    ASSERT_TRUE(m.total_nodes >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Just: recipe nodes as Function nodes.
 * RED: histogram shows Module:1 only — individual recipes are not extracted
 *      as Function nodes by the grammar-only Just extractor. */
TEST(probe_just_recipe_function_nodes) {
    GpfMetrics m = gpf_metrics("Justfile", "deploy env:\n"
                                           "  ./scripts/deploy.sh {{env}}\n"
                                           "\n"
                                           "lint:\n"
                                           "  golangci-lint run ./...\n"
                                           "\n"
                                           "generate:\n"
                                           "  go generate ./...\n");
    ASSERT_TRUE(m.ok);
    /* RED: Just recipes not extracted as Function nodes (only Module:1 in histogram). */
    ASSERT_TRUE(m.functions >= 1); /* expected RED — no Just recipe → Function extraction */
    PASS();
}

/* Just: `import` directive (Just >=1.5) → IMPORTS edge.
 * RED: Just `import` not resolved into IMPORTS edges. */
TEST(probe_just_import_edge) {
    static const GpfFile files[] = {{"common.just", "fmt:\n"
                                                    "  cargo fmt\n"
                                                    "\n"
                                                    "lint:\n"
                                                    "  cargo clippy\n"},
                                    {"justfile", "import 'common.just'\n"
                                                 "\n"
                                                 "build:\n"
                                                 "  cargo build\n"}};
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: Just `import` not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no Just import resolver */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 17 — BitBake (.bb, .bbappend, .bbclass)
 *
 * Labels histogram: Module:1
 * BitBake is the build system used by Yocto/OpenEmbedded.  Recipes define
 * tasks (do_compile, do_install, etc.) and variables.  The extractor produces
 * a Module node per file; individual tasks not extracted as Function nodes.
 * `inherit` and `require`/`include` are cross-file mechanisms.
 * ══════════════════════════════════════════════════════════════════ */

/* BitBake: recipe with task and variable → Module node. */
TEST(probe_bitbake_recipe_node) {
    GpfMetrics m =
        gpf_metrics("mypackage_1.0.bb", "DESCRIPTION = \"A simple test package\"\n"
                                        "HOMEPAGE = \"https://example.com\"\n"
                                        "LICENSE = \"MIT\"\n"
                                        "LIC_FILES_CHKSUM = \"file://COPYING;md5=abc123\"\n"
                                        "\n"
                                        "SRC_URI = \"https://example.com/mypackage-1.0.tar.gz\"\n"
                                        "\n"
                                        "do_compile() {\n"
                                        "  make\n"
                                        "}\n"
                                        "\n"
                                        "do_install() {\n"
                                        "  install -d ${D}${bindir}\n"
                                        "  install -m 0755 myapp ${D}${bindir}/\n"
                                        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: any non-empty .bb file produces at least a Module node. */
    ASSERT_TRUE(m.total_nodes >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* BitBake: bbclass file → Module node. */
TEST(probe_bitbake_bbclass_node) {
    GpfMetrics m = gpf_metrics("cmake.bbclass",
                               "# CMake class for BitBake\n"
                               "EXTRA_OECMAKE ?= \"\"\n"
                               "\n"
                               "cmake_do_configure() {\n"
                               "  cmake -DCMAKE_INSTALL_PREFIX=${prefix} ${EXTRA_OECMAKE} ${S}\n"
                               "}\n"
                               "\n"
                               "cmake_do_compile() {\n"
                               "  make ${EXTRA_OEMAKE}\n"
                               "}\n"
                               "\n"
                               "EXPORT_FUNCTIONS do_configure do_compile\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: .bbclass produces a Module node. */
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Count this genuine import as a named quality gap until its source and
 * path-scope proof is implemented. Any guessed target remains a bug. */
TEST(probe_bitbake_require_edge) {
    static const GpfFile files[] = {{"mypackage.inc",
                                     "DESCRIPTION = \"Shared package metadata\"\n"
                                     "LICENSE = \"MIT\"\n"
                                     "SRC_URI = \"https://example.com/pkg-${PV}.tar.gz\"\n"},
                                    {"mypackage_1.0.bb", "require mypackage.inc\n"
                                                         "\n"
                                                         "PV = \"1.0\"\n"
                                                         "\n"
                                                         "do_compile() {\n"
                                                         "  make\n"
                                                         "}\n"}};

    const RQ_Source sources[] = {
        {.path = "mypackage.inc", .text = files[0].content},
        {.path = "mypackage_1.0.bb", .text = files[1].content},
    };
    const RQ_Expected gap = {
        .case_id = "bitbake_require_edge",
        .site = {.file = "mypackage_1.0.bb",
                 .caller = "mypackage_1.0.bb.__file__",
                 .needle = "require mypackage.inc"},
        .relation = "IMPORTS",
        .target = {.file = "mypackage.inc",
                   .suffix = "mypackage.inc.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "DESCRIPTION = \"Shared package metadata\""},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "bitbake_require_search_scope_unproven",
        .why = "The intended require target needs a certified recipe/include search context.",
        .tried = "Kept the literal directive and complete include metadata. The BitBake carrier "
                 "does not prove the active include-search binding or exact source occurrence; a "
                 "basename match is insufficient."};
    RQ_Report report;
    ASSERT_TRUE(gpf_literal_import_quality(files, sources, "probe_bitbake_require_edge", &gap, NULL,
                                           &report));
    ASSERT_EQ(report.genuine, 2); /* original import + required control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 18 — Kconfig (Kconfig)
 *
 * Labels histogram: Class:1, Module:1
 * Kconfig describes Linux kernel configuration options.  The extractor
 * produces a Module + a Class node (one per Kconfig file, matching histogram).
 * `source` directive includes other Kconfig files — not resolved as IMPORTS.
 * ══════════════════════════════════════════════════════════════════ */

/* Kconfig: config entries → Module and Class nodes. */
TEST(probe_kconfig_config_entries) {
    GpfMetrics m = gpf_metrics("Kconfig", "config MYDRV\n"
                                          "  tristate \"My Driver\"\n"
                                          "  depends on PCI\n"
                                          "  help\n"
                                          "    Enable this driver for My Device.\n"
                                          "\n"
                                          "config MYDRV_DEBUG\n"
                                          "  bool \"Enable debug output\"\n"
                                          "  depends on MYDRV\n"
                                          "  default n\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: histogram confirms Class:1,Module:1 for any Kconfig file. */
    ASSERT_TRUE(m.modules >= 1);
    ASSERT_TRUE(m.classes >= 1);
    PASS();
}

/* Kconfig: menu block with multiple entries. */
TEST(probe_kconfig_menu_block) {
    GpfMetrics m = gpf_metrics("Kconfig", "menu \"Network Support\"\n"
                                          "\n"
                                          "config NET\n"
                                          "  bool \"Networking support\"\n"
                                          "\n"
                                          "config INET\n"
                                          "  bool \"TCP/IP networking\"\n"
                                          "  depends on NET\n"
                                          "\n"
                                          "endmenu\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: Module + Class nodes for the file. */
    ASSERT_TRUE(m.total_nodes >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Count this genuine import as a named quality gap until its source and
 * path-scope proof is implemented. Any guessed target remains a bug. */
TEST(probe_kconfig_source_edge) {
    static const GpfFile files[] = {{"drivers/Kconfig", "config DRV_USB\n"
                                                        "  tristate \"USB Driver\"\n"
                                                        "  depends on USB\n"},
                                    {"Kconfig", "mainmenu \"Linux Kernel Configuration\"\n"
                                                "\n"
                                                "source \"drivers/Kconfig\"\n"}};

    const RQ_Source sources[] = {
        {.path = "drivers/Kconfig", .text = files[0].content},
        {.path = "Kconfig", .text = files[1].content},
    };
    const RQ_Expected gap = {
        .case_id = "kconfig_source_edge",
        .site = {.file = "Kconfig",
                 .caller = "Kconfig.__file__",
                 .needle = "source \"drivers/Kconfig\""},
        .relation = "IMPORTS",
        .target = {.file = "drivers/Kconfig",
                   .suffix = "drivers.Kconfig.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "config DRV_USB"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "kconfig_source_tree_binding_unproven",
        .why = "The intended child Kconfig source needs a certified source-tree root and exact "
               "File binding.",
        .tried = "Kept the top-level source directive and driver definition. The generic path "
                 "carrier does not prove source-tree configuration or the raw source occurrence."};
    RQ_Report report;
    ASSERT_TRUE(gpf_literal_import_quality(files, sources, "probe_kconfig_source_edge", &gap, NULL,
                                           &report));
    ASSERT_EQ(report.genuine, 2); /* original import + required control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 19 — Go Mod (go.mod)
 *
 * Labels histogram: Module:1
 * go.mod is parsed by pass_pkgmap.c which extracts the module path as a
 * pkgmap entry used for import resolution, not for producing a DEPENDS_ON
 * edge per `require` directive.  A Module node is created per file.
 * DEPENDS_ON edges are produced for Helm chart dependencies (pass_k8s.c),
 * not for go.mod `require` blocks in the grammar-only pipeline.
 * ══════════════════════════════════════════════════════════════════ */

/* Go mod: module directive → Module node. */
TEST(probe_gomod_module_node) {
    GpfMetrics m = gpf_metrics("go.mod", "module example.com/myapp\n"
                                         "\n"
                                         "go 1.21\n"
                                         "\n"
                                         "require (\n"
                                         "  github.com/gin-gonic/gin v1.9.1\n"
                                         "  github.com/stretchr/testify v1.8.4\n"
                                         ")\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: go.mod produces at least a Module node. */
    ASSERT_TRUE(m.total_nodes >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Go mod: simple module with go directive only. */
TEST(probe_gomod_simple) {
    GpfMetrics m = gpf_metrics("go.mod", "module example.com/simple\n"
                                         "\n"
                                         "go 1.21\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: even a minimal go.mod with no requires produces a Module node. */
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Go mod: `require` blocks → DEPENDS_ON edges per dependency.
 * RED: go.mod `require` directives are not resolved into DEPENDS_ON edges;
 *      pass_pkgmap only uses go.mod for intra-repo module path mapping. */
TEST(probe_gomod_depends_on_edges) {
    GpfMetrics m = gpf_metrics("go.mod", "module example.com/server\n"
                                         "\n"
                                         "go 1.21\n"
                                         "\n"
                                         "require (\n"
                                         "  github.com/gorilla/mux v1.8.1\n"
                                         "  github.com/sirupsen/logrus v1.9.3\n"
                                         "  github.com/prometheus/client_golang v1.17.0\n"
                                         ")\n");
    ASSERT_TRUE(m.ok);
    /* RED: go.mod require → DEPENDS_ON not implemented (pkgmap handles path mapping only). */
    ASSERT_TRUE(m.depends_on >= 1); /* expected RED — no go.mod DEPENDS_ON producer */
    PASS();
}

/* Go mod: go.mod used in a multi-file project to enable cross-package IMPORTS.
 * GREEN: the presence of go.mod enables the pkgmap resolver to link
 *        cross-package imports in Go source files. */
TEST(probe_gomod_enables_go_imports) {
    static const GpfFile files[] = {{"go.mod", "module example.com/demo\n"
                                               "\n"
                                               "go 1.21\n"},
                                    {"pkg/calc/calc.go",
                                     "package calc\n"
                                     "\n"
                                     "func Add(a, b int) int { return a + b }\n"},
                                    {"main.go", "package main\n"
                                                "\n"
                                                "import \"example.com/demo/pkg/calc\"\n"
                                                "\n"
                                                "func main() { _ = calc.Add(1, 2) }\n"}};
    GpfMetrics m = gpf_metrics_files(files, 3);
    ASSERT_TRUE(m.ok);
    /* GREEN: go.mod enables the pkgmap resolver; main.go → calc IMPORTS edge. */
    ASSERT_TRUE(m.imports >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * SUITE wiring
 * ══════════════════════════════════════════════════════════════════ */

SUITE(grammar_probe_f) {
    /* HCL / Terraform */
    RUN_TEST(probe_hcl_resource_nodes);
    RUN_TEST(probe_hcl_class_node);
    RUN_TEST(probe_hcl_module_block_node);
    RUN_TEST(probe_hcl_infra_maps_edge);
    RUN_TEST(probe_hcl_no_spurious_imports);
    RUN_TEST(probe_hcl_nested_blocks_are_not_classes);
    RUN_TEST(probe_tofu_file_indexed);
    RUN_TEST(probe_tfvars_file_indexed);

    /* Kubernetes manifests */
    RUN_TEST(probe_k8s_pod_resource_node);
    RUN_TEST(probe_k8s_deployment_resource_node);
    RUN_TEST(probe_k8s_two_manifests);
    RUN_TEST(probe_k8s_infra_maps_edge);
    RUN_TEST(probe_k8s_selector_requires_all_pairs);
    RUN_TEST(probe_k8s_selector_ignores_workload_metadata_labels);
    RUN_TEST(probe_k8s_selector_no_name_guess);
    RUN_TEST(probe_k8s_selector_key_matters);
    RUN_TEST(probe_k8s_selector_namespace_scoped);
    RUN_TEST(probe_k8s_selector_beyond_512_manifests);

    /* Kustomize */
    RUN_TEST(probe_kustomize_module_node);
    RUN_TEST(probe_kustomize_imports_edges);
    RUN_TEST(probe_kustomize_with_patches);
    RUN_TEST(probe_k8s_manifest_after_leading_comment);
    RUN_TEST(probe_kustomize_imports_after_leading_comment);
    RUN_TEST(probe_k8s_selector_after_leading_separator);
    RUN_TEST(probe_kustomize_extensionless_file);
    RUN_TEST(probe_kustomize_remote_base_names_no_symbol);
    RUN_TEST(probe_helm_file_dependency_links_local_chart);
    RUN_TEST(probe_k8s_manifest_mints_no_envelope_variables);
    RUN_TEST(probe_yaml_config_keeps_variables);
    RUN_TEST(probe_helm_unresolved_file_dependency_stays_external);
    RUN_TEST(probe_external_chart_identity_is_its_publisher);
    RUN_TEST(probe_kustomize_nested_sibling_entries);
    RUN_TEST(probe_kustomize_single_module);
    RUN_TEST(probe_kustomize_entries_after_commented_out_entry);
    RUN_TEST(probe_kustomize_entries_after_key_line_comment);
    RUN_TEST(probe_k8s_manifest_metadata_after_key_line_comment);
    RUN_TEST(probe_k8s_every_document_is_an_object);
    RUN_TEST(probe_k8s_selector_across_documents);
    RUN_TEST(probe_k8s_same_name_two_namespaces);
    RUN_TEST(probe_k8s_identity_key);
    RUN_TEST(probe_k8s_identity_core_namespaced_and_cluster_scoped);
    RUN_TEST(probe_k8s_anchored_and_tagged_names);
    RUN_TEST(probe_k8s_selector_flow_mapping);
    RUN_TEST(probe_secret_stringdata_mints_no_route);
    RUN_TEST(probe_endpoint_url_loses_credentials);
    RUN_TEST(probe_tfvars_values_mint_no_route);
    RUN_TEST(probe_k8s_pod_spec_references);
    RUN_TEST(probe_k8s_rbac_ingress_hpa_webhook_references);
    RUN_TEST(probe_k8s_cr_uses_its_crd_and_unresolved_draws_nothing);
    RUN_TEST(probe_k8s_reference_stays_in_bundle);
    RUN_TEST(probe_k8s_reference_prefers_its_own_file);
    RUN_TEST(probe_k8s_cr_prefers_the_crd_in_its_bundle);
    RUN_TEST(probe_go_controller_handles_its_kinds);
    RUN_TEST(probe_go_crd_type_and_embed);
    RUN_TEST(probe_go_controller_kind_is_the_imported_type);
    RUN_TEST(probe_go_controller_import_follows_go_mod);
    RUN_TEST(probe_go_controller_chain_with_comment_lines);
    RUN_TEST(probe_go_controller_methods_sharing_a_node);
    RUN_TEST(probe_flux_refs_meet_in_the_deploy_scope);
    RUN_TEST(probe_capi_refs_follow_the_api_group);
    RUN_TEST(probe_cert_manager_and_kkp_refs);
    RUN_TEST(probe_argo_application_and_rollout_refs);
    RUN_TEST(probe_k8s_pdb_and_networkpolicy_select_workloads);
    RUN_TEST(probe_k8s_selectors_pick_their_own_version);
    RUN_TEST(probe_k8s_ingress_and_httproute_expose_routes);
    RUN_TEST(probe_mcp_search_filters_kind_and_namespace);
    RUN_TEST(probe_mcp_trace_topology);
    RUN_TEST(probe_mcp_trace_topology_follows_deploys);
    RUN_TEST(probe_mcp_trace_run);
    RUN_TEST(probe_mcp_trace_deploy);
    RUN_TEST(probe_dockerfile_bases_are_images);
    RUN_TEST(probe_image_without_registry_host_stays_unknown);
    RUN_TEST(probe_dockerfile_variants_are_dockerfiles);
    RUN_TEST(probe_dockerfile_entrypoint_runs);
    RUN_TEST(probe_k8s_container_images_are_images);
    RUN_TEST(probe_build_configs_build_images_from_dockerfiles);
    RUN_TEST(probe_compose_services_are_resources);
    RUN_TEST(probe_shell_docker_builds_build_images);
    RUN_TEST(probe_container_env_sets_scoped_envvars);
    RUN_TEST(probe_container_env_joins_its_own_image_readers);
    RUN_TEST(probe_env_address_connects_to_its_service);
    RUN_TEST(probe_script_http_request_calls_its_route);
    RUN_TEST(probe_kustomize_patches_override_their_base);
    RUN_TEST(probe_kustomize_helm_charts_deploy_their_chart);
    RUN_TEST(probe_terraform_refs_resolve_in_their_module);
    RUN_TEST(probe_terraform_module_files_extract_no_generic_refs);
    RUN_TEST(probe_terraform_module_calls_import_their_source);
    RUN_TEST(probe_terraform_tfvars_override_their_variables);
    RUN_TEST(probe_terragrunt_units_compose_their_files);
    RUN_TEST(probe_ansible_files_import_what_they_name);
    RUN_TEST(probe_terraform_kubernetes_objects_are_resources);
    RUN_TEST(probe_kustomize_name_prefix_makes_variants);
    RUN_TEST(probe_ansible_tasks_are_functions);
    RUN_TEST(probe_ansible_kube_tasks_deploy_their_templates);
    RUN_TEST(probe_image_hub_splits_by_component);
    RUN_TEST(probe_gitops_objects_deploy_their_units);
    RUN_TEST(probe_shell_kubectl_deploys_what_it_applies);
    RUN_TEST(probe_shell_helm_deploys_its_chart);
    RUN_TEST(probe_shell_paths_follow_the_script);
    RUN_TEST(probe_make_recipes_deploy);
    RUN_TEST(probe_shell_programs_by_path_spawn);
    RUN_TEST(probe_make_programs_by_path_spawn);
    RUN_TEST(probe_ci_and_scripts_run_scripts);
    RUN_TEST(probe_make_targets_spawn_their_rules);
    RUN_TEST(probe_make_rules_have_a_node_per_target);
    RUN_TEST(probe_helmrelease_chartref_deploys_its_oci_chart);
    RUN_TEST(probe_helmrelease_chartref_requires_unique_explicit_source);
    RUN_TEST(probe_go_installer_deploys_charts);
    RUN_TEST(probe_terraform_helm_release_deploys_its_chart);
    RUN_TEST(probe_helm_templated_objects_are_resources);
    RUN_TEST(probe_helm_chart_deploys_its_objects);
    RUN_TEST(probe_helm_chart_literal_images_follow_rendered_condition);
    RUN_TEST(probe_helm_chart_crds_and_werf_chart);
    RUN_TEST(probe_docker_default_images_marked_and_overrides);
    RUN_TEST(probe_helm_template_images_render_with_defaults);
    RUN_TEST(probe_helm_release_images);
    RUN_TEST(probe_helm_release_terraform_number_set);
    RUN_TEST(probe_helm_release_flux_unread_sources);
    RUN_TEST(probe_helm_release_kustomize_unread_sources);
    RUN_TEST(probe_helm_release_terraform_dynamic_set);
    RUN_TEST(probe_helm_release_terraform_sensitive_and_postrender);
    RUN_TEST(probe_helm_release_terraform_sensitive_read_earlier);
    RUN_TEST(probe_helm_release_terraform_postrender_forms);
    RUN_TEST(probe_helm_checksum_include_by_base_path);
    RUN_TEST(probe_helm_release_kustomize_transformers);
    RUN_TEST(probe_helm_empty_values_file_renders);
    RUN_TEST(probe_helm_aliased_subchart_chart_name);
    RUN_TEST(probe_helm_list_path_mark_on_chart_edge);
    RUN_TEST(probe_helm_values_symlink_not_followed);
    RUN_TEST(probe_helm_terraform_values_symlink_not_followed);
    RUN_TEST(probe_docker_build_args_keep_no_secret);
    RUN_TEST(probe_docker_long_build_args_kept_whole);
    RUN_TEST(probe_helm_include_resolves_in_the_chart_family);
    RUN_TEST(probe_k8s_selector_stays_in_its_directory);
    RUN_TEST(probe_k8s_selector_within_kustomize_build);
    RUN_TEST(probe_k8s_kustomize_namespace_is_effective);

    /* Nix */
    RUN_TEST(probe_nix_attrset_node);
    RUN_TEST(probe_nix_let_binding);
    RUN_TEST(probe_nix_import_edge);

    /* Nickel */
    RUN_TEST(probe_nickel_record_node);
    RUN_TEST(probe_nickel_let_fn);
    RUN_TEST(probe_nickel_import_edge);

    /* Pkl */
    RUN_TEST(probe_pkl_properties_node);
    RUN_TEST(probe_pkl_object_block);
    RUN_TEST(probe_pkl_import_edge);

    /* RON */
    RUN_TEST(probe_ron_record_node);
    RUN_TEST(probe_ron_named_struct);
    RUN_TEST(probe_ron_no_imports);

    /* Jsonnet */
    RUN_TEST(probe_jsonnet_object_node);
    RUN_TEST(probe_jsonnet_function_in_object);
    RUN_TEST(probe_jsonnet_import_edge);

    /* KDL */
    RUN_TEST(probe_kdl_node_block);
    RUN_TEST(probe_kdl_flat_nodes);
    RUN_TEST(probe_kdl_no_imports);

    /* Hyprlang */
    RUN_TEST(probe_hyprlang_section_node);
    RUN_TEST(probe_hyprlang_directives);
    RUN_TEST(probe_hyprlang_source_import_edge);

    /* DeviceTree */
    RUN_TEST(probe_devicetree_root_node);
    RUN_TEST(probe_devicetree_child_nodes);
    RUN_TEST(probe_devicetree_include_edge);

    /* CMake */
    RUN_TEST(probe_cmake_function_node);
    RUN_TEST(probe_cmake_macro_node);
    RUN_TEST(probe_cmake_include_edge);
    RUN_TEST(probe_cmake_subdirectory_edge);

    /* Makefile */
    RUN_TEST(probe_makefile_target_nodes);
    RUN_TEST(probe_makefile_pattern_rule);
    RUN_TEST(probe_makefile_include_edge);

    /* Meson */
    RUN_TEST(probe_meson_project_node);
    RUN_TEST(probe_meson_options_node);
    RUN_TEST(probe_meson_subdir_edge);

    /* GN */
    RUN_TEST(probe_gn_executable_node);
    RUN_TEST(probe_gn_library_targets);
    RUN_TEST(probe_gn_import_edge);

    /* Just */
    RUN_TEST(probe_just_recipe_nodes);
    RUN_TEST(probe_just_recipe_function_nodes);
    RUN_TEST(probe_just_import_edge);

    /* BitBake */
    RUN_TEST(probe_bitbake_recipe_node);
    RUN_TEST(probe_bitbake_bbclass_node);
    RUN_TEST(probe_bitbake_require_edge);

    /* Kconfig */
    RUN_TEST(probe_kconfig_config_entries);
    RUN_TEST(probe_kconfig_menu_block);
    RUN_TEST(probe_kconfig_source_edge);

    /* Go Mod */
    RUN_TEST(probe_gomod_module_node);
    RUN_TEST(probe_gomod_simple);
    RUN_TEST(probe_gomod_depends_on_edges);
    RUN_TEST(probe_gomod_enables_go_imports);
}
