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
#include "cbm.h"
#include <mcp/mcp.h>
#include <store/store.h>
#include <pipeline/pipeline.h>
#include <foundation/log.h>

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
        if (*p == '\\') *p = '/';
    }
}

static cbm_store_t *gpf_open_indexed(GpfProj *lp) {
    lp->project = cbm_project_name_from_path(lp->tmpdir);
    if (!lp->project) return NULL;
    const char *home = getenv("HOME");
    if (!home) home = "/tmp";
    char cache_dir[512];
    snprintf(cache_dir, sizeof(cache_dir), "%s/.cache/codebase-memory-mcp", home);
    cbm_mkdir(cache_dir);
    snprintf(lp->dbpath, sizeof(lp->dbpath), "%s/%s.db", cache_dir, lp->project);
    unlink(lp->dbpath);
    lp->srv = cbm_mcp_server_new(NULL);
    if (!lp->srv) return NULL;
    char args[700];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", lp->tmpdir);
    char *resp = cbm_mcp_handle_tool(lp->srv, "index_repository", args);
    if (resp) free(resp);
    return cbm_store_open_path(lp->dbpath);
}

static cbm_store_t *gpf_index_files(GpfProj *lp, const GpfFile *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_gpf_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir)) return NULL;
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
        if (!f) return NULL;
        fputs(files[i].content, f);
        fclose(f);
    }
    return gpf_open_indexed(lp);
}

static void gpf_cleanup(GpfProj *lp, cbm_store_t *store) {
    if (store) cbm_store_close(store);
    if (lp->srv) { cbm_mcp_server_free(lp->srv); lp->srv = NULL; }
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
    int functions;   /* Function label */
    int modules;     /* Module label */
    int classes;     /* Class label */
    int resources;   /* Resource label (k8s) */
    int imports;     /* IMPORTS edges */
    int depends_on;  /* DEPENDS_ON edges */
    int infra_maps;  /* INFRA_MAPS edges */
} GpfMetrics;

static GpfMetrics gpf_metrics_files(const GpfFile *files, int nfiles) {
    GpfProj lp;
    cbm_store_t *store = gpf_index_files(&lp, files, nfiles);
    GpfMetrics m = {0};
    if (store) {
        m.ok          = 1;
        m.total_nodes = cbm_store_count_nodes(store, lp.project);
        m.functions   = gpf_count_label(store, lp.project, "Function");
        m.modules     = gpf_count_label(store, lp.project, "Module");
        m.classes     = gpf_count_label(store, lp.project, "Class");
        m.resources   = gpf_count_label(store, lp.project, "Resource");
        m.imports     = cbm_store_count_edges_by_type(store, lp.project, "IMPORTS");
        m.depends_on  = cbm_store_count_edges_by_type(store, lp.project, "DEPENDS_ON");
        m.infra_maps  = cbm_store_count_edges_by_type(store, lp.project, "INFRA_MAPS");
    }
    gpf_cleanup(&lp, store);
    return m;
}

static GpfMetrics gpf_metrics(const char *filename, const char *content) {
    GpfFile f = {filename, content};
    return gpf_metrics_files(&f, 1);
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
    GpfMetrics m = gpf_metrics("main.tf",
        "resource \"aws_s3_bucket\" \"my_bucket\" {\n"
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
    GpfMetrics m = gpf_metrics("infra.hcl",
        "resource \"aws_instance\" \"web\" {\n"
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
    GpfMetrics m = gpf_metrics("modules.tf",
        "module \"vpc\" {\n"
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
    GpfMetrics m = gpf_metrics("sched.tf",
        "resource \"google_cloud_scheduler_job\" \"cron\" {\n"
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
    static const GpfFile files[] = {
        {"vars.hcl",
         "variable \"region\" {\n"
         "  default = \"us-east-1\"\n"
         "}\n"},
        {"main.tf",
         "resource \"aws_s3_bucket\" \"b\" {\n"
         "  bucket = \"test\"\n"
         "}\n"}
    };
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* GREEN: HCL has no import syntax — pipeline should emit 0 IMPORTS edges. */
    ASSERT_TRUE(m.imports == 0);
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
    GpfMetrics m = gpf_metrics("pod.yaml",
        "apiVersion: v1\n"
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
    GpfMetrics m = gpf_metrics("deploy.yaml",
        "apiVersion: apps/v1\n"
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
    static const GpfFile files[] = {
        {"service.yaml",
         "apiVersion: v1\n"
         "kind: Service\n"
         "metadata:\n"
         "  name: my-svc\n"
         "spec:\n"
         "  selector:\n"
         "    app: my-app\n"
         "  ports:\n"
         "  - port: 80\n"},
        {"configmap.yaml",
         "apiVersion: v1\n"
         "kind: ConfigMap\n"
         "metadata:\n"
         "  name: app-config\n"
         "data:\n"
         "  key: value\n"}
    };
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* GREEN: each manifest file produces its own Resource node. */
    ASSERT_TRUE(m.resources >= 2);
    PASS();
}

/* K8s: cross-manifest reference via INFRA_MAPS.
 * RED: the single-path pipeline does not emit INFRA_MAPS edges for k8s refs;
 *      pass_k8s only emits Resource nodes (no ref-resolution between manifests). */
TEST(probe_k8s_infra_maps_edge) {
    static const GpfFile files[] = {
        {"deploy.yaml",
         "apiVersion: apps/v1\n"
         "kind: Deployment\n"
         "metadata:\n"
         "  name: frontend\n"
         "spec:\n"
         "  template:\n"
         "    spec:\n"
         "      containers:\n"
         "      - name: frontend\n"
         "        image: frontend:latest\n"},
        {"service.yaml",
         "apiVersion: v1\n"
         "kind: Service\n"
         "metadata:\n"
         "  name: frontend-svc\n"
         "spec:\n"
         "  selector:\n"
         "    app: frontend\n"}
    };
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: inter-manifest INFRA_MAPS edges not modeled by the current k8s pass. */
    ASSERT_TRUE(m.infra_maps >= 1); /* expected RED — no k8s selector → INFRA_MAPS */
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
    GpfMetrics m = gpf_metrics("kustomization.yaml",
        "apiVersion: kustomize.config.k8s.io/v1beta1\n"
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
    static const GpfFile files[] = {
        {"kustomization.yaml",
         "apiVersion: kustomize.config.k8s.io/v1beta1\n"
         "kind: Kustomization\n"
         "resources:\n"
         "  - deploy.yaml\n"
         "  - service.yaml\n"},
        {"deploy.yaml",
         "apiVersion: apps/v1\n"
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
        {"service.yaml",
         "apiVersion: v1\n"
         "kind: Service\n"
         "metadata:\n"
         "  name: myapp-svc\n"
         "spec:\n"
         "  selector:\n"
         "    app: myapp\n"
         "  ports:\n"
         "  - port: 80\n"}
    };
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
    GpfMetrics m = gpf_metrics("kustomization.yaml",
        "apiVersion: kustomize.config.k8s.io/v1beta1\n"
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
    GpfMetrics m = gpf_metrics("default.nix",
        "{ pkgs ? import <nixpkgs> {} }:\n"
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
    GpfMetrics m = gpf_metrics("lib.nix",
        "let\n"
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

/* Nix: `import` in two-file fixture → IMPORTS edge.
 * RED: Nix import expressions are not resolved into IMPORTS graph edges. */
TEST(probe_nix_import_edge) {
    static const GpfFile files[] = {
        {"utils.nix",
         "{ double = x: x * 2; }\n"},
        {"default.nix",
         "let utils = import ./utils.nix;\n"
         "in { result = utils.double 21; }\n"}
    };
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: Nix `import` not resolved into IMPORTS edges by the grammar-only pipeline. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no Nix import resolver */
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
    GpfMetrics m = gpf_metrics("config.ncl",
        "{\n"
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
    GpfMetrics m = gpf_metrics("lib.ncl",
        "let double = fun x => x * 2 in\n"
        "let square = fun x => x * x in\n"
        "{ double, square }\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: file-level Module node. */
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Nickel: `import` in two-file fixture → IMPORTS edge.
 * RED: Nickel import expressions not resolved by the pipeline. */
TEST(probe_nickel_import_edge) {
    static const GpfFile files[] = {
        {"utils.ncl", "{ double = fun x => x * 2 }\n"},
        {"main.ncl",
         "let utils = import \"utils.ncl\" in\n"
         "{ result = utils.double 21 }\n"}
    };
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: Nickel `import` not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no Nickel import resolver */
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
    GpfMetrics m = gpf_metrics("config.pkl",
        "host = \"localhost\"\n"
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
    GpfMetrics m = gpf_metrics("server.pkl",
        "server {\n"
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

/* Pkl: `import` in two-file fixture → IMPORTS edge.
 * RED: Pkl import/amends not resolved into IMPORTS edges. */
TEST(probe_pkl_import_edge) {
    static const GpfFile files[] = {
        {"base.pkl",
         "host = \"localhost\"\n"
         "port = 8080\n"},
        {"prod.pkl",
         "amends \"base.pkl\"\n"
         "\n"
         "port = 443\n"}
    };
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: Pkl `amends`/`import` not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no Pkl import resolver */
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
    GpfMetrics m = gpf_metrics("config.ron",
        "(\n"
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
    GpfMetrics m = gpf_metrics("scene.ron",
        "Scene(\n"
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
    GpfMetrics m = gpf_metrics("data.ron",
        "[\n"
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
    GpfMetrics m = gpf_metrics("config.jsonnet",
        "{\n"
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
    GpfMetrics m = gpf_metrics("lib.jsonnet",
        "{\n"
        "  double(x):: x * 2,\n"
        "  square(x):: x * x,\n"
        "  greet(name):: \"Hello, \" + name,\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: Module node. */
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Jsonnet: `import` in two-file fixture → IMPORTS edge.
 * RED: Jsonnet import not resolved into IMPORTS edges. */
TEST(probe_jsonnet_import_edge) {
    static const GpfFile files[] = {
        {"utils.libsonnet",
         "{\n"
         "  double(x):: x * 2,\n"
         "}\n"},
        {"main.jsonnet",
         "local utils = import 'utils.libsonnet';\n"
         "{\n"
         "  result: utils.double(21),\n"
         "}\n"}
    };
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: Jsonnet `import` not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no Jsonnet import resolver */
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
    GpfMetrics m = gpf_metrics("config.kdl",
        "server {\n"
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
    GpfMetrics m = gpf_metrics("manifest.kdl",
        "package \"my-package\" version=\"1.0.0\"\n"
        "description \"A test package\"\n"
        "author \"Test Author\" email=\"test@example.com\"\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: Module node. */
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* KDL: no import mechanism → 0 IMPORTS edges. */
TEST(probe_kdl_no_imports) {
    GpfMetrics m = gpf_metrics("data.kdl",
        "items {\n"
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
    GpfMetrics m = gpf_metrics("hyprland.conf",
        "general {\n"
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
    GpfMetrics m = gpf_metrics("binds.hl",
        "bind = $mainMod, Q, killactive\n"
        "bind = $mainMod, M, exit\n"
        "bind = $mainMod, F, fullscreen\n"
        "monitor = DP-1, 1920x1080@144, 0x0, 1\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: Module node for any non-empty .hl file. */
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Hyprlang: `source` in two-file fixture → IMPORTS edge.
 * RED: Hyprlang `source` directive not resolved into IMPORTS edges. */
TEST(probe_hyprlang_source_import_edge) {
    static const GpfFile files[] = {
        {"colors.conf",
         "col.active_border = rgba(33ccffee)\n"
         "col.inactive_border = rgba(595959aa)\n"},
        {"hyprland.conf",
         "source = ~/.config/hypr/colors.conf\n"
         "\n"
         "general {\n"
         "  gaps_in = 5\n"
         "}\n"}
    };
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: Hyprlang `source` not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no Hyprlang source resolver */
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
    GpfMetrics m = gpf_metrics("board.dts",
        "/dts-v1/;\n"
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
    GpfMetrics m = gpf_metrics("soc.dts",
        "/dts-v1/;\n"
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

/* DeviceTree: .dtsi include fragment → IMPORTS edge.
 * RED: DeviceTree `#include` is handled at preprocessor level, not by the pipeline
 *      as an IMPORTS graph edge. */
TEST(probe_devicetree_include_edge) {
    static const GpfFile files[] = {
        {"soc.dtsi",
         "/dts-v1/;\n"
         "/ {\n"
         "  soc: soc {\n"
         "    compatible = \"simple-bus\";\n"
         "  };\n"
         "};\n"},
        {"board.dts",
         "/dts-v1/;\n"
         "#include \"soc.dtsi\"\n"
         "\n"
         "/ {\n"
         "  model = \"My Board\";\n"
         "};\n"}
    };
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: DTS `#include` not resolved into IMPORTS edges (preprocessor-level). */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no DTS include resolver */
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
    GpfMetrics m = gpf_metrics("CMakeLists.txt",
        "cmake_minimum_required(VERSION 3.16)\n"
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
    GpfMetrics m = gpf_metrics("helpers.cmake",
        "macro(print_var varname)\n"
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

/* CMake: `include()` in two-file fixture → IMPORTS edge.
 * RED: CMake include() not resolved into IMPORTS edges. */
TEST(probe_cmake_include_edge) {
    static const GpfFile files[] = {
        {"helpers.cmake",
         "function(check_compiler)\n"
         "  message(STATUS \"Compiler: ${CMAKE_CXX_COMPILER_ID}\")\n"
         "endfunction()\n"},
        {"CMakeLists.txt",
         "cmake_minimum_required(VERSION 3.16)\n"
         "project(Foo)\n"
         "include(helpers.cmake)\n"
         "check_compiler()\n"}
    };
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: CMake `include()` not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no CMake include resolver */
    PASS();
}

/* CMake: add_subdirectory → child CMakeLists.txt; IMPORTS edge expected.
 * RED: add_subdirectory not resolved into IMPORTS edges. */
TEST(probe_cmake_subdirectory_edge) {
    static const GpfFile files[] = {
        {"CMakeLists.txt",
         "cmake_minimum_required(VERSION 3.16)\n"
         "project(Root)\n"
         "add_subdirectory(lib)\n"},
        {"lib/CMakeLists.txt",
         "add_library(mylib STATIC mylib.cpp)\n"
         "\n"
         "function(mylib_configure target)\n"
         "  target_link_libraries(${target} PRIVATE mylib)\n"
         "endfunction()\n"}
    };
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: CMake add_subdirectory() not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no add_subdirectory resolver */
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
    GpfMetrics m = gpf_metrics("Makefile",
        ".PHONY: all build test clean\n"
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
    GpfMetrics m = gpf_metrics("rules.mk",
        "CC = gcc\n"
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

/* Makefile: `include` in two-file fixture → IMPORTS edge.
 * RED: Makefile `include` directive not resolved into IMPORTS edges. */
TEST(probe_makefile_include_edge) {
    static const GpfFile files[] = {
        {"common.mk",
         "CC = gcc\n"
         "CFLAGS = -Wall -O2\n"
         "\n"
         "clean:\n"
         "\trm -f *.o\n"},
        {"Makefile",
         "include common.mk\n"
         "\n"
         "all:\n"
         "\t$(CC) $(CFLAGS) -o myapp main.c\n"}
    };
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: Makefile `include` not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no Makefile include resolver */
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
    GpfMetrics m = gpf_metrics("meson.build",
        "project('myapp', 'c',\n"
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
    GpfMetrics m = gpf_metrics("meson.options",
        "option('enable_tests', type: 'boolean', value: true,\n"
        "  description: 'Build and run tests')\n"
        "option('build_type', type: 'combo', choices: ['debug', 'release'],\n"
        "  value: 'release')\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: meson.options also produces a Module node. */
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Meson: subdir() across two files → IMPORTS edge.
 * RED: Meson subdir() not resolved into IMPORTS edges. */
TEST(probe_meson_subdir_edge) {
    static const GpfFile files[] = {
        {"meson.build",
         "project('root', 'c')\n"
         "subdir('lib')\n"},
        {"lib/meson.build",
         "static_library('mylib', 'mylib.c')\n"}
    };
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: Meson subdir() not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no Meson subdir resolver */
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
    GpfMetrics m = gpf_metrics("BUILD.gn",
        "executable(\"myapp\") {\n"
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
    GpfMetrics m = gpf_metrics("lib.gn",
        "static_library(\"crypto\") {\n"
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

/* GN: import() of a .gni file → IMPORTS edge.
 * RED: GN import() not resolved into IMPORTS edges. */
TEST(probe_gn_import_edge) {
    static const GpfFile files[] = {
        {"//build/common.gni",
         "template(\"cc_binary\") {\n"
         "  executable(target_name) {\n"
         "    forward_variables_from(invoker, \"*\")\n"
         "  }\n"
         "}\n"},
        {"BUILD.gn",
         "import(\"//build/common.gni\")\n"
         "\n"
         "cc_binary(\"myapp\") {\n"
         "  sources = [ \"main.cc\" ]\n"
         "}\n"}
    };
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: GN import() not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no GN import resolver */
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
    GpfMetrics m = gpf_metrics("justfile",
        "build:\n"
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
    GpfMetrics m = gpf_metrics("Justfile",
        "deploy env:\n"
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
    static const GpfFile files[] = {
        {"common.just",
         "fmt:\n"
         "  cargo fmt\n"
         "\n"
         "lint:\n"
         "  cargo clippy\n"},
        {"justfile",
         "import 'common.just'\n"
         "\n"
         "build:\n"
         "  cargo build\n"}
    };
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
    GpfMetrics m = gpf_metrics("mypackage_1.0.bb",
        "DESCRIPTION = \"A simple test package\"\n"
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

/* BitBake: `require` in two-file fixture → IMPORTS edge.
 * RED: BitBake require/include not resolved into IMPORTS edges. */
TEST(probe_bitbake_require_edge) {
    static const GpfFile files[] = {
        {"mypackage.inc",
         "DESCRIPTION = \"Shared package metadata\"\n"
         "LICENSE = \"MIT\"\n"
         "SRC_URI = \"https://example.com/pkg-${PV}.tar.gz\"\n"},
        {"mypackage_1.0.bb",
         "require mypackage.inc\n"
         "\n"
         "PV = \"1.0\"\n"
         "\n"
         "do_compile() {\n"
         "  make\n"
         "}\n"}
    };
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: BitBake `require` not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no BitBake require resolver */
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
    GpfMetrics m = gpf_metrics("Kconfig",
        "config MYDRV\n"
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
    GpfMetrics m = gpf_metrics("Kconfig",
        "menu \"Network Support\"\n"
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

/* Kconfig: `source` in two-file fixture → IMPORTS edge.
 * RED: Kconfig `source` directive not resolved into IMPORTS edges. */
TEST(probe_kconfig_source_edge) {
    static const GpfFile files[] = {
        {"drivers/Kconfig",
         "config DRV_USB\n"
         "  tristate \"USB Driver\"\n"
         "  depends on USB\n"},
        {"Kconfig",
         "mainmenu \"Linux Kernel Configuration\"\n"
         "\n"
         "source \"drivers/Kconfig\"\n"}
    };
    GpfMetrics m = gpf_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: Kconfig `source` not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no Kconfig source resolver */
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
    GpfMetrics m = gpf_metrics("go.mod",
        "module example.com/myapp\n"
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
    GpfMetrics m = gpf_metrics("go.mod",
        "module example.com/simple\n"
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
    GpfMetrics m = gpf_metrics("go.mod",
        "module example.com/server\n"
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
    static const GpfFile files[] = {
        {"go.mod",
         "module example.com/demo\n"
         "\n"
         "go 1.21\n"},
        {"pkg/calc/calc.go",
         "package calc\n"
         "\n"
         "func Add(a, b int) int { return a + b }\n"},
        {"main.go",
         "package main\n"
         "\n"
         "import \"example.com/demo/pkg/calc\"\n"
         "\n"
         "func main() { _ = calc.Add(1, 2) }\n"}
    };
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

    /* Kubernetes manifests */
    RUN_TEST(probe_k8s_pod_resource_node);
    RUN_TEST(probe_k8s_deployment_resource_node);
    RUN_TEST(probe_k8s_two_manifests);
    RUN_TEST(probe_k8s_infra_maps_edge);

    /* Kustomize */
    RUN_TEST(probe_kustomize_module_node);
    RUN_TEST(probe_kustomize_imports_edges);
    RUN_TEST(probe_kustomize_with_patches);

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
