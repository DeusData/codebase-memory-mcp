/*
 * test_complexity.c — Complexity guard: superlinearity detection at tiny input
 *                     sizes, gated on DETERMINISTIC work counters — never time.
 *
 * Why this suite exists: the v0.9.0→v0.10.x indexing regression was a
 * files×corpus_defs coupling in cross-file LSP (#1669). Finding it took an
 * 11-corpus A/B across two release binaries, because nothing in CI could see
 * an O(n^2) forming. This suite makes that class of bug fail a unit test.
 *
 * Method — replicated independent modules:
 *   Build a synthetic corpus of k INDEPENDENT module copies per language
 *   (module i never references module j). Then every extensive quantity —
 *   nodes, edges, and Σ per-file work — MUST grow linearly in k. Run the full
 *   in-process pipeline at k and 2k and assert counter RATIOS:
 *
 *       linear pipeline    ratio ≈ 2      (gate: within [lo, hi])
 *       files×corpus bug   ratio ≈ 4      (per-file work itself grows with k)
 *
 *   Ratios expose the exponent at TINY sizes (dozens of files, seconds of
 *   runtime): a counter that counts the quadratic term directly doubles its
 *   growth per doubling regardless of absolute scale. No large corpora needed.
 *
 * Determinism doctrine (O9): a verdict must be a pure function of
 * (code, input). Work counters are sums over per-file work and do not depend
 * on scheduling; wall time does. Therefore ONLY counters gate. Throughput
 * (nodes/s, edges/s) is measured and written to a LOCAL report under private/
 * as information for trend comparison — it never gates, and the report step is
 * skipped entirely under CBM_SKIP_PERF (starved fidelity legs would record
 * meaningless rates).
 *
 * Dynamic coverage: languages come from two providers, iterated over the full
 * CBM_LANG_COUNT enum —
 *   1. embedded templates below (the LSP-hybrid languages, where cross-file
 *      machinery — and therefore files×corpus coupling risk — lives);
 *   2. auto-discovered fixture dirs tests/fixtures/complexity/<lang-name>/
 *      (drop files there when adding a language; no test edit needed).
 * Languages with neither provider are recorded in the report as skipped with
 * the reason "no complexity template": their extractors are per-file by
 * construction (grammar-only, no cross-file resolution), so the coupling this
 * suite hunts cannot arise from them; the shared passes they feed (registry,
 * similarity, semantic) are exercised by the template corpus.
 */
#include "lsp/type_registry.h"
#include "test_framework.h"
#include "test_helpers.h"

#include "../src/foundation/compat.h"
#include "../src/foundation/compat_fs.h"
#include "../src/foundation/compat_thread.h"
#include "../src/foundation/log.h"
#include "../src/foundation/profile.h"
#include "cbm.h"
#include "discover/discover.h"
#include "lang_specs.h"
#include "lsp/c_lsp.h"
#include "lsp/kotlin_lsp.h"
#include "lsp/py_lsp.h"
#include "lsp/rust_proof.h"
#include "tree_sitter/api.h"
#include "pipeline/pass_lsp_cross.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "store/store.h"
#include "resolve/stats.h"
#include "foundation/sha256.h"
#include <sqlite3.h>
#include "graph_buffer/graph_buffer.h"
#include "resolve/binding.h"
#include "resolve/rust_names.h"
#include "resolve/source_adapter.h"
#include "lsp/lsp_work.h"
#include "lsp/lsp_name_rows.h"
#define CBM_SYNTAX_INDEX_IMPLEMENTATION /* the seam only: this file asks tree-sitter directly */
#include "resolve/syntax_index.h"
#include "yyjson/yyjson.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Tail-match scan counters (defined in pass_parallel.c, declared in
 * lsp_resolve.h — re-declared here to avoid pulling that header's statics). */
extern _Atomic uint64_t g_lsp_tail_lookups;
extern _Atomic uint64_t g_lsp_tail_candidates;
/* Infra resolver candidate counters (pass_k8s.c, pass_operator.c,
 * pass_shell.c, pass_terraform.c). */
extern _Atomic uint64_t g_k8s_candidates;
extern _Atomic uint64_t g_op_candidates;
extern _Atomic uint64_t g_sh_candidates;
extern _Atomic uint64_t g_tf_candidates;
extern _Atomic uint64_t g_dk_hub_edges;

/* ── Corpus scale ──────────────────────────────────────────────────────
 * Small on purpose: the gate reads exponents from ratios, not magnitudes.
 * K_BASE modules vs 2*K_BASE modules, CX_FILES_PER_MOD files each, per
 * provider language. Total runtime target for the whole suite: seconds. */
enum {
    CX_K_BASE = 3,
    CX_FILES_PER_MOD = 5,
    /* Gate only when the base run produced enough work for the ratio to be
     * meaningful — a near-zero denominator would make the gate noise. The
     * false-guard audit rule applies: a separate assertion below proves the
     * counter is genuinely nonzero so this gate can never pass vacuously. */
    CX_MIN_BASE_WORK = 40,
    /* The pipeline resolves sequentially below MIN_FILES_FOR_PARALLEL (50
     * files, pipeline.c) and that path builds no shared registries — so both
     * legs of every corpus pair must exceed it or the gates measure the wrong
     * code path. The sequential path's own per-file registry work is bounded
     * by the 50-file ceiling and is deliberately out of scope here. */
    CX_BIGPKG_FILES_PER_K = 20,
    CX_PYEXT_FILES_PER_MOD = 20,
    CX_PYEXT_DEFS_PER_FILE = 120,
    CX_PYEXT_PROBES_PER_FILE = 3,
};

/* Linear growth bounds for a 2x input doubling. Fixed structural overhead
 * (Project/root nodes) pulls node ratios slightly under 2; template boundary
 * effects push edge ratios slightly around 2. A files×corpus coupling lands
 * at ~4 — far outside. */
#define CX_RATIO_LO 1.45
#define CX_RATIO_HI 2.75

/* ── Language templates ────────────────────────────────────────────────
 * Each emitter writes file j of a module. Files reference file j-1 of the
 * SAME module (cross-file resolution work); modules never reference each
 * other (independence — the property the linearity gate rests on). */

static void cx_emit_java(FILE *f, int m, int j) {
    fprintf(f, "package mod%d;\n\npublic class C%d {\n", m, j);
    fprintf(f, "    public int val%d(int x) {\n        return x + %d;\n    }\n", j, j);
    if (j > 0) {
        fprintf(f, "    public int chain() {\n");
        fprintf(f, "        C%d prev = new C%d();\n", j - 1, j - 1);
        fprintf(f, "        return prev.val%d(1) + val%d(2);\n    }\n", j - 1, j);
    } else {
        fprintf(f, "    public int chain() {\n        return val0(2);\n    }\n");
    }
    fprintf(f, "}\n");
}

static void cx_emit_python(FILE *f, int m, int j) {
    (void)m;
    if (j > 0) {
        fprintf(f, "from .f%d import val%d\n\n", j - 1, j - 1);
    }
    fprintf(f, "def val%d(x):\n    return x + %d\n\n", j, j);
    if (j > 0) {
        fprintf(f, "def chain%d():\n    return val%d(1) + val%d(2)\n", j, j - 1, j);
    } else {
        fprintf(f, "def chain0():\n    return val0(2)\n");
    }
    /* Unknown call effects must invalidate a real receiver binding. The two
     * member occurrences exercise both positive proof and stale-type refusal. */
    fprintf(f,
            "\nclass Box%d:\n    def value(self):\n        return %d\n\n"
            "def mutate%d(effect):\n    obj = Box%d()\n    obj.value()\n"
            "    effect()\n    obj.value()\n",
            j, j, j, j);
}

static void cx_emit_go(FILE *f, int m, int j) {
    fprintf(f, "package mod%d\n\n", m);
    fprintf(f, "func Val%d(x int) int {\n\treturn x + %d\n}\n\n", j, j);
    if (j > 0) {
        fprintf(f, "func Chain%d() int {\n\treturn Val%d(1) + Val%d(2)\n}\n", j, j - 1, j);
    } else {
        fprintf(f, "func Chain0() int {\n\treturn Val0(2)\n}\n");
    }
}

static void cx_emit_ts(FILE *f, int m, int j) {
    (void)m;
    if (j > 0) {
        fprintf(f, "import { val%d } from \"./f%d\";\n\n", j - 1, j - 1);
    }
    fprintf(f, "export function val%d(x: number): number {\n    return x + %d;\n}\n\n", j, j);
    if (j > 0) {
        fprintf(f, "export function chain%d(): number {\n    return val%d(1) + val%d(2);\n}\n", j,
                j - 1, j);
    } else {
        fprintf(f, "export function chain0(): number {\n    return val0(2);\n}\n");
    }
}

typedef struct {
    CBMLanguage lang;
    const char *dirname; /* corpus subdir, doubles as the per-lang scope */
    const char *file_prefix;
    const char *ext;
    void (*emit)(FILE *f, int m, int j);
} CxTemplate;

static const CxTemplate CX_TEMPLATES[] = {
    {CBM_LANG_JAVA, "javasrc", "C", ".java", cx_emit_java},
    {CBM_LANG_PYTHON, "pysrc", "f", ".py", cx_emit_python},
    {CBM_LANG_GO, "gosrc", "f", ".go", cx_emit_go},
    {CBM_LANG_TYPESCRIPT, "tssrc", "f", ".ts", cx_emit_ts},
};
enum { CX_TEMPLATE_COUNT = sizeof(CX_TEMPLATES) / sizeof(CX_TEMPLATES[0]) };

/* Fixture-dir provider: tests/fixtures/complexity/<lang-name>/ — every file
 * and directory in it (dot-names aside) is copied verbatim into each module
 * dir. Lets a new language join the
 * guard by dropping fixtures, with no edit to this suite. */
static bool cx_fixture_dir_for(CBMLanguage lang, char *out, size_t cap) {
    const char *name = cbm_language_name(lang);
    if (!name || !name[0]) {
        return false;
    }
    snprintf(out, cap, "tests/fixtures/complexity/%s", name);
    cbm_dir_t *d = cbm_opendir(out);
    if (!d) {
        return false;
    }
    cbm_closedir(d);
    return true;
}

static int cx_copy_file(const char *src, const char *dst) {
    FILE *in = fopen(src, "rb");
    if (!in) {
        return -1;
    }
    FILE *out = fopen(dst, "wb");
    if (!out) {
        fclose(in);
        return -1;
    }
    char buf[4096];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), in)) > 0) {
        fwrite(buf, 1, n, out);
    }
    fclose(in);
    fclose(out);
    return 0;
}

/* A fixture entry into `dst`: a file, or a directory with its contents. */
static int cx_copy_entry(const char *src, const char *dst, bool is_dir) {
    if (!is_dir) {
        return cx_copy_file(src, dst);
    }
    if (th_mkdir_p(dst) != 0) {
        return -1;
    }
    cbm_dir_t *d = cbm_opendir(src);
    if (!d) {
        return -1;
    }
    int rc = 0;
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(d)) != NULL) {
        if (entry->name[0] == '.') {
            continue;
        }
        char from[1024];
        char to[1024];
        snprintf(from, sizeof(from), "%s/%s", src, entry->name);
        snprintf(to, sizeof(to), "%s/%s", dst, entry->name);
        if (cx_copy_entry(from, to, entry->is_dir) != 0) {
            rc = -1;
        }
    }
    cbm_closedir(d);
    return rc;
}

/* ── Corpus builder ──────────────────────────────────────────────────── */

static int cx_build_corpus(const char *root, int k_modules) {
    char dir[1024];
    char path[1200];
    for (int t = 0; t < CX_TEMPLATE_COUNT; t++) {
        const CxTemplate *tp = &CX_TEMPLATES[t];
        for (int m = 0; m < k_modules; m++) {
            snprintf(dir, sizeof(dir), "%s/%s/mod%d", root, tp->dirname, m);
            if (th_mkdir_p(dir) != 0) {
                return -1;
            }
            for (int j = 0; j < CX_FILES_PER_MOD; j++) {
                snprintf(path, sizeof(path), "%s/%s%d%s", dir, tp->file_prefix, j, tp->ext);
                FILE *f = fopen(path, "w");
                if (!f) {
                    return -1;
                }
                tp->emit(f, m, j);
                fclose(f);
            }
        }
    }
    /* Fixture-dir providers: replicate each discovered language dir into
     * k module copies. Distinct paths make distinct modules/QNs, which is all
     * the independence argument needs. */
    for (int lang = 0; lang < CBM_LANG_COUNT; lang++) {
        /* a templated language may add fixtures too (Go: the operator
         * corpus); they land in their own fx_<lang> root */
        char fixdir[512];
        if (!cx_fixture_dir_for((CBMLanguage)lang, fixdir, sizeof(fixdir))) {
            continue;
        }
        cbm_dir_t *d = cbm_opendir(fixdir);
        if (!d) {
            continue;
        }
        cbm_dirent_t *entry;
        while ((entry = cbm_readdir(d)) != NULL) {
            if (entry->name[0] == '.') {
                continue;
            }
            for (int m = 0; m < k_modules; m++) {
                snprintf(dir, sizeof(dir), "%s/fx_%s/mod%d", root,
                         cbm_language_name((CBMLanguage)lang), m);
                if (th_mkdir_p(dir) != 0) {
                    continue;
                }
                char src[1024];
                snprintf(src, sizeof(src), "%s/%s", fixdir, entry->name);
                snprintf(path, sizeof(path), "%s/%s", dir, entry->name);
                (void)cx_copy_entry(src, path, entry->is_dir);
            }
        }
        cbm_closedir(d);
    }
    return 0;
}

/* Corpus shape #2 — the growing shared package. Real monorepos concentrate
 * files in a few large packages (org.<org>.common, …), and the JVM filter
 * branch includes every def sharing the file's namespace — so per-file work
 * tracks PACKAGE size. A package whose file count scales with the corpus is
 * therefore the honest reproducer for the #1669 growth pattern that fully
 * independent modules cannot show: here every extensive quantity must STILL
 * be linear in k, while a namespace/module-scoped per-file registry build
 * goes quadratic. */
static int cx_build_bigpkg(const char *root, int k) {
    char dir[1024];
    char path[1200];
    snprintf(dir, sizeof(dir), "%s/bigsrc/bigpkg", root);
    if (th_mkdir_p(dir) != 0) {
        return -1;
    }
    int files = k * CX_BIGPKG_FILES_PER_K;
    for (int j = 0; j < files; j++) {
        snprintf(path, sizeof(path), "%s/B%d.java", dir, j);
        FILE *f = fopen(path, "w");
        if (!f) {
            return -1;
        }
        fprintf(f, "package bigpkg;\n\npublic class B%d {\n", j);
        fprintf(f, "    public int val%d(int x) {\n        return x + %d;\n    }\n", j, j);
        if (j > 0) {
            fprintf(f, "    public int chain() {\n");
            fprintf(f, "        B%d prev = new B%d();\n", j - 1, j - 1);
            fprintf(f, "        return prev.val%d(1) + val%d(2);\n    }\n", j - 1, j);
        } else {
            fprintf(f, "    public int chain() {\n        return val0(2);\n    }\n");
        }
        fprintf(f, "}\n");
        fclose(f);
    }
    /* One growing Python package exercises export/type propagation and
     * invalidation alongside the JVM namespace registry. Each import has a
     * bounded direct predecessor, so adding files must not scan the package. */
    snprintf(dir, sizeof(dir), "%s/resolverpkg", root);
    if (th_mkdir_p(dir) != 0)
        return -1;
    snprintf(path, sizeof(path), "%s/__init__.py", dir);
    FILE *package = cbm_fopen(path, "wb");
    if (!package || fclose(package) != 0)
        return -1;
    for (int j = 0; j < files; j++) {
        snprintf(path, sizeof(path), "%s/f%d.py", dir, j);
        FILE *f = cbm_fopen(path, "wb");
        if (!f)
            return -1;
        cx_emit_python(f, 0, j);
        bool failed = ferror(f) != 0;
        if (fclose(f) != 0 || failed)
            return -1;
    }
    return 0;
}

/* Every file probes absent external submodules and retains a real local-call
 * control. Independent module copies grow the shared registry without adding
 * cross-copy dependencies. Both sizes exceed the parallel-driver threshold. */
static int cx_build_pyext(const char *root, int k) {
    char dir[1024];
    char path[1200];
    for (int m = 0; m < k; m++) {
        int n = snprintf(dir, sizeof(dir), "%s/pyext/mod%d", root, m);
        if (n < 0 || (size_t)n >= sizeof(dir) || th_mkdir_p(dir) != 0)
            return -1;
        for (int j = 0; j < CX_PYEXT_FILES_PER_MOD; j++) {
            n = snprintf(path, sizeof(path), "%s/p%d.py", dir, j);
            if (n < 0 || (size_t)n >= sizeof(path))
                return -1;
            FILE *f = fopen(path, "w");
            if (!f)
                return -1;
            bool written = fprintf(f, "import extlib\n\n") >= 0;
            for (int d = 0; written && d < CX_PYEXT_DEFS_PER_FILE; d++) {
                written =
                    fprintf(f, "def cx_pyext_definition_%d(x):\n    return x + %d\n\n", d, d) >= 0;
            }
            if (written)
                written =
                    fprintf(f, "def cx_pyext_use():\n    return (cx_pyext_definition_0(0)") >= 0;
            for (int probe = 0; written && probe < CX_PYEXT_PROBES_PER_FILE; probe++) {
                written = fprintf(f, " + extlib.sub%d.helper(%d)", probe, probe) >= 0;
            }
            if (written)
                written = fprintf(f, ")\n") >= 0;
            int closed = fclose(f);
            if (!written || closed != 0)
                return -1;
        }
    }
    return 0;
}

/* ── Metrics ─────────────────────────────────────────────────────────── */

/* Per-pass timing capture: a TEE log sink parses `pass.timing` lines during a
 * run. Information for the report only — pass timings are wall-clock and never
 * gate (O9). */
enum { CX_MAX_PASSES = 48 };
typedef struct {
    char name[64];
    long ms;
} CxPassMs;
static CxPassMs g_cx_passes[CX_MAX_PASSES];
static _Atomic int g_cx_pass_count = 0;

static void cx_pass_sink(const char *line) {
    if (!line || !strstr(line, "pass.timing")) {
        return;
    }
    const char *pp = strstr(line, "pass=");
    const char *ee = strstr(line, "elapsed_ms=");
    if (!pp || !ee) {
        return;
    }
    int n = atomic_fetch_add_explicit(&g_cx_pass_count, 1, memory_order_relaxed);
    if (n >= CX_MAX_PASSES) {
        return;
    }
    size_t i = 0;
    pp += 5;
    while (pp[i] && pp[i] != ' ' && i < sizeof(g_cx_passes[n].name) - 1) {
        g_cx_passes[n].name[i] = pp[i];
        i++;
    }
    g_cx_passes[n].name[i] = '\0';
    g_cx_passes[n].ms = atol(ee + 11);
}

/* Fixture-provided infra corpora (tests/fixtures/complexity/<name>/), scoped
 * by their copy root: Kubernetes manifests with a kustomization (and a Flux
 * HelmRelease whose chartRef names an OCIRepository), a Go operator
 * (controller, API types with +groupName, an embedded CRD, a chart loader), a
 * shell deploy chain (composite CI action -> make -> scripts -> kubectl / helm
 * of the copy's own manifests and chart), and Terraform helm_releases (one of
 * a chart dir of the module, one of an external chart). An API
 * group/kind is one identity across a repository: copies declaring the same
 * one ARE joined (a CRD links to every package marking its group), so the
 * fixtures' CRDs use groups no copy's Go code declares -- the guard measures
 * accidental growth, not that by-design join. The container layer (Docker)
 * has Dockerfiles with stage ENV, an ENTRYPOINT script and a compose project
 * building two components of one repository; an image repository is one
 * identity too, so every copy's builds of it join one Image -- that join
 * must stay linear (the component split examines its edges). */
static const char *const CX_INFRA_DIRS[] = {"fx_Kubernetes", "fx_Go", "fx_Bash", "fx_HCL",
                                            "fx_Dockerfile"};
enum { CX_INFRA_COUNT = sizeof(CX_INFRA_DIRS) / sizeof(CX_INFRA_DIRS[0]) };

typedef struct {
    int nodes;
    int edges;
    int lang_nodes[CX_TEMPLATE_COUNT];
    int lang_edges[CX_TEMPLATE_COUNT];
    CxPassMs passes[CX_MAX_PASSES];
    int pass_count;
    uint64_t perfile_defs; /* Σ defs registered by per-file/overlay registry builds */
    uint64_t build_files;
    uint64_t filter_failed;
    uint64_t tail_lookups;
    uint64_t tail_candidates;
    uint64_t fallback_rows;
    uint64_t imp_nodes;       /* symbols scored by the importance pass */
    uint64_t imp_name_visits; /* same-name-group members visited by that pass */
    uint64_t k8s_candidates;  /* objects the K8s resolvers examined */
    uint64_t op_candidates;   /* Go types / dirs the operator pass examined */
    uint64_t sh_candidates;   /* paths / make rules the shell layer examined */
    uint64_t tf_candidates;   /* blocks the Terraform layer examined */
    uint64_t dk_hub_edges;    /* Image edges the component split examined */
    int infra_nodes[CX_INFRA_COUNT];
    int infra_edges[CX_INFRA_COUNT];
    uint64_t py_probes;
    uint64_t py_probe_visits;
    int pyext_files;
    int pyext_defs;
    int pyext_controls;
    uint64_t py_class_binds;
    uint64_t py_class_visits;
    uint64_t py_wildcards;
    uint64_t py_wildcard_visits;
    int pyrange_files;
    int pyrange_defs;
    int pyrange_classes;
    int pyrange_peer_classes;
    int pyrange_controls;
    int errors;
    double wall_s;
    cbm_resolve_stats_snapshot_t resolver;
} CxMetrics;

/* Count the intended corpus, then prove each use() kept its exact local LSP
 * call. Query errors are failures, never indistinguishable from zero rows. */
static bool cx_pyext_control(cbm_store_t *store, const char *project, const cbm_node_t *caller) {
    if (!caller->qualified_name || !caller->file_path)
        return false;
    const char *dot = strrchr(caller->qualified_name, '.');
    if (!dot || dot - caller->qualified_name > 1000)
        return false;
    char target_qn[1200];
    int n = snprintf(target_qn, sizeof(target_qn), "%.*s.cx_pyext_definition_0",
                     (int)(dot - caller->qualified_name), caller->qualified_name);
    if (n < 0 || (size_t)n >= sizeof(target_qn))
        return false;
    cbm_node_t target = {0};
    cbm_edge_t *edges = NULL;
    int count = 0;
    bool found = false;
    if (cbm_store_find_node_by_qn(store, project, target_qn, &target) == CBM_STORE_OK &&
        target.label && strcmp(target.label, "Function") == 0 && target.file_path &&
        strcmp(target.file_path, caller->file_path) == 0 &&
        cbm_store_find_edges_by_source_type(store, caller->id, "CALLS", &edges, &count) ==
            CBM_STORE_OK) {
        for (int i = 0; i < count; i++) {
            if (edges[i].target_id != target.id || !edges[i].properties_json)
                continue;
            /* Every admitted semantic edge carries the shared resolver proof rule. */
            yyjson_doc *doc =
                yyjson_read(edges[i].properties_json, strlen(edges[i].properties_json), 0);
            const char *strategy =
                doc ? yyjson_get_str(yyjson_obj_get(yyjson_doc_get_root(doc), "strategy")) : NULL;
            found = found ||
                    (strategy && !strcmp(strategy, "shared_resolver") &&
                     yyjson_get_int(yyjson_obj_get(yyjson_doc_get_root(doc), "resolver_rule")) > 0);
            yyjson_doc_free(doc);
        }
    }
    cbm_store_free_edges(edges, count);
    cbm_node_free_fields(&target);
    return found;
}

static bool cx_pyext_population(cbm_store_t *store, const char *project, CxMetrics *out) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    bool ok = cbm_store_find_nodes_by_label(store, project, "File", &nodes, &count) == CBM_STORE_OK;
    for (int i = 0; ok && i < count; i++) {
        if (nodes[i].file_path && strncmp(nodes[i].file_path, "pyext/", 6) == 0)
            out->pyext_files++;
    }
    cbm_store_free_nodes(nodes, count);
    nodes = NULL;
    count = 0;
    ok = ok &&
         cbm_store_find_nodes_by_label(store, project, "Function", &nodes, &count) == CBM_STORE_OK;
    for (int i = 0; ok && i < count; i++) {
        const cbm_node_t *node = &nodes[i];
        if (!node->file_path || strncmp(node->file_path, "pyext/", 6) != 0 || !node->name)
            continue;
        static const char definition_prefix[] = "cx_pyext_definition_";
        if (strncmp(node->name, definition_prefix, sizeof(definition_prefix) - 1) == 0)
            out->pyext_defs++;
        if (strcmp(node->name, "cx_pyext_use") == 0 && cx_pyext_control(store, project, node))
            out->pyext_controls++;
    }
    cbm_store_free_nodes(nodes, count);
    return ok;
}

/* Each package has many registered direct members; other packages are decoys.
 * Wildcards precede definitions so the fixture retains explicit LSP positives.
 * Exact invalidation-order negatives live in test_py_lsp.c. */
enum { CX_PYRANGE_FILES_PER_K = 20, CX_PYRANGE_DEFS = 40, CX_PYRANGE_WILDCARDS = 4 };

static int cx_build_pyrange(const char *root, int k) {
    for (int m = 0; m < k; m++) {
        for (int j = 0; j < CX_PYRANGE_FILES_PER_K; j++) {
            char dir[1024], path[1200];
            int dn = snprintf(dir, sizeof(dir), "%s/pyrange/mod%d/p%d", root, m, j);
            if (dn < 0 || (size_t)dn >= sizeof(dir) || th_mkdir_p(dir) != 0)
                return -1;
            int pn = snprintf(path, sizeof(path), "%s/__init__.py", dir);
            if (pn < 0 || (size_t)pn >= sizeof(path))
                return -1;
            FILE *f = fopen(path, "w");
            if (!f)
                return -1;
            bool written = true;
            for (int w = 0; written && w < CX_PYRANGE_WILDCARDS; w++)
                written = fprintf(f, "from range_vendor_%d import *\n", w) >= 0;
            int peer = (j + 1) % CX_PYRANGE_FILES_PER_K;
            if (written)
                written = fprintf(f, "from pyrange.mod%d.p%d import Peer_%d_%d\n\n", m, peer, m,
                                  peer) >= 0;
            for (int d = 0; written && d < CX_PYRANGE_DEFS; d++) {
                written = fprintf(f,
                                  "def cx_pyrange_definition_%d(x):\n    return x + %d\n\n"
                                  "class RangeClass_%d:\n    def value(self):\n"
                                  "        return %d\n\n",
                                  d, d, d, d) >= 0;
            }
            if (written)
                written = fprintf(f,
                                  "class Peer_%d_%d:\n    def remote_%d_%d(self):\n"
                                  "        return %d\n\n"
                                  "def cx_pyrange_use():\n    obj = RangeClass_0()\n"
                                  "    peer = Peer_%d_%d()\n"
                                  "    return (cx_pyrange_definition_0(1) + obj.value()\n"
                                  "            + peer.remote_%d_%d())\n",
                                  m, j, m, j, j, m, peer, m, peer) >= 0;
            int closed = fclose(f);
            if (!written || closed != 0)
                return -1;
        }
    }
    return 0;
}

static bool cx_pyrange_exact_control(cbm_store_t *store, const char *project,
                                     const cbm_node_t *caller, const char *qn,
                                     const char *target_file, const char *label) {
    if (!caller->qualified_name || !caller->file_path || !caller->label ||
        strcmp(caller->label, "Function") != 0)
        return false;
    cbm_node_t target = {0};
    cbm_edge_t *edges = NULL;
    int count = 0, exact = 0;
    bool ok = cbm_store_find_node_by_qn(store, project, qn, &target) == CBM_STORE_OK &&
              target.qualified_name && strcmp(target.qualified_name, qn) == 0 && target.label &&
              strcmp(target.label, label) == 0 && target.file_path &&
              strcmp(target.file_path, target_file) == 0 &&
              cbm_store_find_edges_by_source_type(store, caller->id, "CALLS", &edges, &count) ==
                  CBM_STORE_OK;
    for (int i = 0; ok && i < count; i++) {
        if (edges[i].target_id != target.id)
            continue;
        yyjson_doc *doc =
            edges[i].properties_json
                ? yyjson_read(edges[i].properties_json, strlen(edges[i].properties_json), 0)
                : NULL;
        /* Every admitted semantic edge carries the shared resolver proof rule. */
        const char *strategy =
            doc ? yyjson_get_str(yyjson_obj_get(yyjson_doc_get_root(doc), "strategy")) : NULL;
        if (strategy && !strcmp(strategy, "shared_resolver") &&
            yyjson_get_int(yyjson_obj_get(yyjson_doc_get_root(doc), "resolver_rule")) > 0)
            exact++;
        yyjson_doc_free(doc);
    }
    cbm_store_free_edges(edges, count);
    cbm_node_free_fields(&target);
    return ok && exact == 1;
}

/* A remote method has no declaration in the importer/raw file registry. Its
 * source=lsp edge proves that cross-file registry evidence reached resolution. */
static bool cx_pyrange_controls(cbm_store_t *store, const char *project, const cbm_node_t *caller) {
    if (!caller->qualified_name || !caller->file_path)
        return false;
    const char *dot = strrchr(caller->qualified_name, '.');
    if (!dot || dot - caller->qualified_name > 1000)
        return false;
    char module[1200], qn[1400];
    int n = snprintf(module, sizeof(module), "%.*s", (int)(dot - caller->qualified_name),
                     caller->qualified_name);
    if (n < 0 || (size_t)n >= sizeof(module))
        return false;
    n = snprintf(qn, sizeof(qn), "%s.cx_pyrange_definition_0", module);
    bool function =
        n >= 0 && (size_t)n < sizeof(qn) &&
        cx_pyrange_exact_control(store, project, caller, qn, caller->file_path, "Function");
    n = snprintf(qn, sizeof(qn), "%s.RangeClass_0.value", module);
    bool method = n >= 0 && (size_t)n < sizeof(qn) &&
                  cx_pyrange_exact_control(store, project, caller, qn, caller->file_path, "Method");
    int m = -1, j = -1;
    if (sscanf(caller->file_path, "pyrange/mod%d/p%d/__init__.py", &m, &j) != 2 || m < 0 || j < 0 ||
        j >= CX_PYRANGE_FILES_PER_K)
        return false;
    char path[1200], slot_name[64];
    n = snprintf(path, sizeof(path), "pyrange/mod%d/p%d/__init__.py", m, j);
    if (n < 0 || (size_t)n >= sizeof(path) || strcmp(path, caller->file_path) != 0)
        return false;
    n = snprintf(slot_name, sizeof(slot_name), "p%d", j);
    char *slot = strrchr(module, '.');
    if (n < 0 || (size_t)n >= sizeof(slot_name) || !slot || strcmp(slot + 1, slot_name) != 0)
        return false;
    *slot = '\0';
    int peer = (j + 1) % CX_PYRANGE_FILES_PER_K;
    n = snprintf(qn, sizeof(qn), "%s.p%d.Peer_%d_%d.remote_%d_%d", module, peer, m, peer, m, peer);
    int pn = snprintf(path, sizeof(path), "pyrange/mod%d/p%d/__init__.py", m, peer);
    bool remote = n >= 0 && (size_t)n < sizeof(qn) && pn >= 0 && (size_t)pn < sizeof(path) &&
                  strcmp(path, caller->file_path) != 0 &&
                  cx_pyrange_exact_control(store, project, caller, qn, path, "Method");
    return function && method && remote;
}

static bool cx_pyrange_population(cbm_store_t *store, const char *project, CxMetrics *out) {
    static const char *const labels[] = {"File", "Function", "Class"};
    bool ok = true;
    for (int l = 0; l < 3; l++) {
        cbm_node_t *nodes = NULL;
        int count = 0;
        bool queried = cbm_store_find_nodes_by_label(store, project, labels[l], &nodes, &count) ==
                       CBM_STORE_OK;
        ok = queried && ok;
        for (int i = 0; queried && i < count; i++) {
            const cbm_node_t *node = &nodes[i];
            if (!node->file_path || strncmp(node->file_path, "pyrange/", 8) != 0)
                continue;
            if (l == 0)
                out->pyrange_files++;
            if (l == 1 && node->name) {
                static const char def_prefix[] = "cx_pyrange_definition_";
                if (strncmp(node->name, def_prefix, sizeof(def_prefix) - 1) == 0)
                    out->pyrange_defs++;
                if (strcmp(node->name, "cx_pyrange_use") == 0 &&
                    cx_pyrange_controls(store, project, node))
                    out->pyrange_controls++;
            }
            if (l == 2 && node->name && strncmp(node->name, "RangeClass_", 11) == 0)
                out->pyrange_classes++;
            if (l == 2 && node->name && strncmp(node->name, "Peer_", 5) == 0)
                out->pyrange_peer_classes++;
        }
        cbm_store_free_nodes(nodes, count);
    }
    return ok;
}

static double cx_now_s(void) {
    struct timespec ts;
    cbm_profile_now(&ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static int cx_run(const char *root, const char *db_path, CxMetrics *out, bool pyext, bool pyrange) {
    memset(out, 0, sizeof(*out));

    uint64_t d0;
    uint64_t b0;
    uint64_t f0;
    uint64_t x0;
    cbm_pxc_filter_stats(&d0, &b0, &f0, &x0);
    uint64_t tl0 = atomic_load_explicit(&g_lsp_tail_lookups, memory_order_relaxed);
    uint64_t tc0 = atomic_load_explicit(&g_lsp_tail_candidates, memory_order_relaxed);
    uint64_t fb0 = cbm_pp_lsp_linear_fallback_rows();
    uint64_t in0 = atomic_load_explicit(&g_importance_nodes, memory_order_relaxed);
    uint64_t iv0 = atomic_load_explicit(&g_importance_name_visits, memory_order_relaxed);
    uint64_t kc0 = atomic_load_explicit(&g_k8s_candidates, memory_order_relaxed);
    uint64_t oc0 = atomic_load_explicit(&g_op_candidates, memory_order_relaxed);
    uint64_t sc0 = atomic_load_explicit(&g_sh_candidates, memory_order_relaxed);
    uint64_t fc0 = atomic_load_explicit(&g_tf_candidates, memory_order_relaxed);
    uint64_t dh0 = atomic_load_explicit(&g_dk_hub_edges, memory_order_relaxed);
    uint64_t pp0 = atomic_load_explicit(&cbm_py_submodule_probes, memory_order_relaxed);
    uint64_t pv0 = atomic_load_explicit(&cbm_py_submodule_probe_visits, memory_order_relaxed);
    uint64_t pc0 = atomic_load_explicit(&cbm_py_module_class_binds, memory_order_relaxed);
    uint64_t pcv0 = atomic_load_explicit(&cbm_py_module_class_bind_visits, memory_order_relaxed);
    uint64_t pw0 = atomic_load_explicit(&cbm_py_wildcard_invalidations, memory_order_relaxed);
    uint64_t pwv0 =
        atomic_load_explicit(&cbm_py_wildcard_invalidation_visits, memory_order_relaxed);

    atomic_store_explicit(&g_cx_pass_count, 0, memory_order_relaxed);
    cbm_log_set_sink_ex(cx_pass_sink, CBM_LOG_SINK_TEE);

    double t0 = cx_now_s();
    cbm_pipeline_t *p = cbm_pipeline_new(root, db_path, CBM_MODE_FULL);
    if (!p) {
        return -1;
    }
    int rc = cbm_pipeline_run(p);
    out->resolver = cbm_pipeline_resolver_stats(p);
    out->wall_s = cx_now_s() - t0;
    cbm_pipeline_get_file_errors(p, NULL, &out->errors);
    cbm_log_set_sink(NULL);
    int captured = atomic_load_explicit(&g_cx_pass_count, memory_order_relaxed);
    out->pass_count = captured < CX_MAX_PASSES ? captured : CX_MAX_PASSES;
    memcpy(out->passes, g_cx_passes, (size_t)out->pass_count * sizeof(CxPassMs));

    char project[512];
    snprintf(project, sizeof(project), "%s", cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    if (rc != 0) {
        return rc;
    }

    uint64_t d1;
    uint64_t b1;
    uint64_t f1;
    uint64_t x1;
    cbm_pxc_filter_stats(&d1, &b1, &f1, &x1);
    out->perfile_defs = d1 - d0;
    out->build_files = b1 - b0;
    out->filter_failed = x1 - x0;
    out->tail_lookups = atomic_load_explicit(&g_lsp_tail_lookups, memory_order_relaxed) - tl0;
    out->tail_candidates = atomic_load_explicit(&g_lsp_tail_candidates, memory_order_relaxed) - tc0;
    out->fallback_rows = cbm_pp_lsp_linear_fallback_rows() - fb0;
    out->imp_nodes = atomic_load_explicit(&g_importance_nodes, memory_order_relaxed) - in0;
    out->imp_name_visits =
        atomic_load_explicit(&g_importance_name_visits, memory_order_relaxed) - iv0;
    out->k8s_candidates = atomic_load_explicit(&g_k8s_candidates, memory_order_relaxed) - kc0;
    out->op_candidates = atomic_load_explicit(&g_op_candidates, memory_order_relaxed) - oc0;
    out->sh_candidates = atomic_load_explicit(&g_sh_candidates, memory_order_relaxed) - sc0;
    out->tf_candidates = atomic_load_explicit(&g_tf_candidates, memory_order_relaxed) - fc0;
    out->dk_hub_edges = atomic_load_explicit(&g_dk_hub_edges, memory_order_relaxed) - dh0;

    out->py_probes = atomic_load_explicit(&cbm_py_submodule_probes, memory_order_relaxed) - pp0;
    out->py_probe_visits =
        atomic_load_explicit(&cbm_py_submodule_probe_visits, memory_order_relaxed) - pv0;

    cbm_store_t *s = cbm_store_open_path(db_path);
    if (!s) {
        return -1;
    }
    out->nodes = cbm_store_count_nodes(s, project);
    out->edges = cbm_store_count_edges(s, project);
    for (int t = 0; t < CX_TEMPLATE_COUNT; t++) {
        out->lang_nodes[t] = cbm_store_count_nodes_scoped(s, project, CX_TEMPLATES[t].dirname);
        out->lang_edges[t] = cbm_store_count_edges_scoped(s, project, CX_TEMPLATES[t].dirname);
    }
    for (int i = 0; i < CX_INFRA_COUNT; i++) {
        out->infra_nodes[i] = cbm_store_count_nodes_scoped(s, project, CX_INFRA_DIRS[i]);
        out->infra_edges[i] = cbm_store_count_edges_scoped(s, project, CX_INFRA_DIRS[i]);
    }
    out->py_class_binds =
        atomic_load_explicit(&cbm_py_module_class_binds, memory_order_relaxed) - pc0;
    out->py_class_visits =
        atomic_load_explicit(&cbm_py_module_class_bind_visits, memory_order_relaxed) - pcv0;
    out->py_wildcards =
        atomic_load_explicit(&cbm_py_wildcard_invalidations, memory_order_relaxed) - pw0;
    out->py_wildcard_visits =
        atomic_load_explicit(&cbm_py_wildcard_invalidation_visits, memory_order_relaxed) - pwv0;
    bool extra_ok = (!pyext || cx_pyext_population(s, project, out)) &&
                    (!pyrange || cx_pyrange_population(s, project, out));
    cbm_store_close(s);
    return extra_ok ? 0 : -1;
}

static double cx_ratio(double num, double den) {
    return den > 0.0 ? num / den : 0.0;
}

typedef int (*CxCorpusBuilder)(const char *root, int k);

static int cx_remove_corpus(const char *root) {
    int rc = th_rmtree(root);
    if (rc != 0) {
        fprintf(stderr, "  [complexity] WARN: could not remove temp corpus %s\n", root);
    }
    return rc;
}

/* Build one corpus shape at k and 2k in two fresh temp dirs, run the pipeline
 * on both, and remove both dirs before returning — on success and on every
 * failure path. Only the metrics outlive the call; the corpora used to stay
 * behind in the temp root, four per suite run. Only dirs this call created
 * are removed (the temp root is shared with concurrent runs). */
static int cx_measure(CxCorpusBuilder build, const char *prefix_a, const char *prefix_b,
                      CxMetrics *out_a, CxMetrics *out_b) {
    const char *tmp = cbm_tmpdir();
    char root_a[512];
    char root_b[512];
    int na = snprintf(root_a, sizeof(root_a), "%s/%s_XXXXXX", tmp, prefix_a);
    int nb = snprintf(root_b, sizeof(root_b), "%s/%s_XXXXXX", tmp, prefix_b);
    if (na < 0 || (size_t)na >= sizeof(root_a) || nb < 0 || (size_t)nb >= sizeof(root_b))
        return -1;
    bool made_a = cbm_mkdtemp(root_a) != NULL;
    bool made_b = made_a && cbm_mkdtemp(root_b) != NULL;
    int rc = -1;
    if (made_b && build(root_a, CX_K_BASE) == 0 && build(root_b, CX_K_BASE * 2) == 0) {
        char db_a[600];
        char db_b[600];
        int da = snprintf(db_a, sizeof(db_a), "%s/cx.db", root_a);
        int db = snprintf(db_b, sizeof(db_b), "%s/cx.db", root_b);
        bool paths = da >= 0 && (size_t)da < sizeof(db_a) && db >= 0 && (size_t)db < sizeof(db_b);
        bool pyext = build == cx_build_pyext;
        bool pyrange = build == cx_build_pyrange;
        if (paths && cx_run(root_a, db_a, out_a, pyext, pyrange) == 0 &&
            cx_run(root_b, db_b, out_b, pyext, pyrange) == 0) {
            rc = 0;
        }
    }
    if (made_b && cx_remove_corpus(root_b) != 0 && build == cx_build_pyrange)
        rc = -1;
    if (made_a && cx_remove_corpus(root_a) != 0 && build == cx_build_pyrange)
        rc = -1;
    return rc;
}

/* Shared across the suite so the report test reuses the measured pair instead
 * of paying two more pipeline runs. */
static CxMetrics g_cx_base;
static CxMetrics g_cx_doubled;
static bool g_cx_measured = false;

static int cx_measure_pair(void) {
    if (g_cx_measured) {
        return 0;
    }
    if (cx_measure(cx_build_corpus, "cbm_cx_base", "cbm_cx_dbl", &g_cx_base, &g_cx_doubled) != 0) {
        return -1;
    }
    g_cx_measured = true;
    return 0;
}

static bool cx_resolver_linear(const CxMetrics *a, const CxMetrics *b) {
    bool ok = true;
    for (unsigned i = 0; i < CBM_RESOLVE_STAT_COUNT; i++) {
        /* Only languages with root-level semantic facts walk syntax here; the
         * flat-file test below gates this counter with a corpus that does. */
        if (i == CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS || i == CBM_RESOLVE_STAT_SYNTAX_INDEX_NODES)
            continue;
        uint64_t first = a->resolver.values[i], second = b->resolver.values[i];
        double ratio = cx_ratio((double)second, (double)first);
        printf("    resolver_%s %llu -> %llu (ratio %.2f)\n",
               cbm_resolve_stat_name((cbm_resolve_stat_t)i), (unsigned long long)first,
               (unsigned long long)second, ratio);
        /* Every category is exercised by source, including unknown-call
         * invalidations. Removing a hook cannot produce a vacuous green. */
        ok = ok && first > 0 && second > 0 && ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI;
    }
    return ok;
}

typedef struct {
    _Atomic unsigned ready;
    _Atomic bool release;
} CxResolverGate;

typedef struct {
    cbm_resolve_stats_t stats;
    uint64_t amount;
    CxResolverGate *gate;
} CxResolverThread;

static void *cx_resolver_count_thread(void *opaque) {
    CxResolverThread *thread = opaque;
    cbm_resolve_stats_t *previous = cbm_resolve_stats_bind(&thread->stats);
    atomic_fetch_add_explicit(&thread->gate->ready, 1, memory_order_release);
    while (!atomic_load_explicit(&thread->gate->release, memory_order_acquire)) {
        /* Both sinks must be installed before either thread records work. */
    }
    for (uint64_t i = 0; i < thread->amount; i++)
        cbm_resolve_stats_add(CBM_RESOLVE_STAT_LOOKUP_PROBES, 1);
    cbm_resolve_stats_bind(previous);
    return NULL;
}

/* ── Tests ───────────────────────────────────────────────────────────── */

TEST(complexity_resolver_counters_are_run_scoped) {
    cbm_resolve_stats_t owner = {0}, nested = {0};
    CxResolverGate gate = {0};
    CxResolverThread first = {.amount = 1000, .gate = &gate};
    CxResolverThread second = {.amount = 2000, .gate = &gate};
    cbm_thread_t a, b;
    cbm_resolve_stats_t *previous = cbm_resolve_stats_bind(&owner);
    cbm_resolve_stats_add(CBM_RESOLVE_STAT_LOOKUP_PROBES, 3);
    cbm_resolve_stats_t *outer = cbm_resolve_stats_bind(&nested);
    cbm_resolve_stats_add(CBM_RESOLVE_STAT_LOOKUP_PROBES, 11);
    cbm_resolve_stats_bind(outer);
    int created_a = cbm_thread_create(&a, 0, cx_resolver_count_thread, &first);
    int created_b = cbm_thread_create(&b, 0, cx_resolver_count_thread, &second);
    unsigned created = (unsigned)(created_a == 0) + (unsigned)(created_b == 0);
    while (atomic_load_explicit(&gate.ready, memory_order_acquire) < created) {
        /* A held gate makes isolation deterministic, independent of scheduling. */
    }
    cbm_resolve_stats_add(CBM_RESOLVE_STAT_LOOKUP_PROBES, 4);
    atomic_store_explicit(&gate.release, true, memory_order_release);
    int joined_a = created_a == 0 ? cbm_thread_join(&a) : -1;
    int joined_b = created_b == 0 ? cbm_thread_join(&b) : -1;
    cbm_resolve_stats_bind(previous);
    ASSERT_EQ(created_a, 0);
    ASSERT_EQ(created_b, 0);
    ASSERT_EQ(joined_a, 0);
    ASSERT_EQ(joined_b, 0);
    ASSERT_EQ(cbm_resolve_stats_snapshot(&owner).values[CBM_RESOLVE_STAT_LOOKUP_PROBES], 7);
    ASSERT_EQ(cbm_resolve_stats_snapshot(&nested).values[CBM_RESOLVE_STAT_LOOKUP_PROBES], 11);
    ASSERT_EQ(cbm_resolve_stats_snapshot(&first.stats).values[CBM_RESOLVE_STAT_LOOKUP_PROBES],
              1000);
    ASSERT_EQ(cbm_resolve_stats_snapshot(&second.stats).values[CBM_RESOLVE_STAT_LOOKUP_PROBES],
              2000);
    cbm_resolve_stats_reset(&first.stats);
    ASSERT_EQ(cbm_resolve_stats_snapshot(&first.stats).values[CBM_RESOLVE_STAT_LOOKUP_PROBES], 0);
    ASSERT_EQ(cbm_resolve_stats_snapshot(&second.stats).values[CBM_RESOLVE_STAT_LOOKUP_PROBES],
              2000);
    PASS();
}

/* The flagship gate. Independent module copies ⇒ every extensive quantity must
 * scale linearly in the copy count. A files×corpus coupling shows up as a
 * ratio near 4 on the counter that sums per-file work. */
TEST(complexity_replicated_modules_scale_linearly) {
    if (cx_measure_pair() != 0) {
        FAIL("failed to build/run the complexity corpus pair");
    }
    const CxMetrics *a = &g_cx_base;
    const CxMetrics *b = &g_cx_doubled;
    ASSERT_TRUE(cx_resolver_linear(a, b));

    /* Sanity: the corpus is real. */
    ASSERT_GT(a->nodes, 50);
    ASSERT_GT(a->edges, 20);

    double node_r = cx_ratio(b->nodes, a->nodes);
    double edge_r = cx_ratio(b->edges, a->edges);
    printf("    nodes %d -> %d (ratio %.2f)  edges %d -> %d (ratio %.2f)\n", a->nodes, b->nodes,
           node_r, a->edges, b->edges, edge_r);
    ASSERT_TRUE(node_r >= CX_RATIO_LO && node_r <= CX_RATIO_HI);
    ASSERT_TRUE(edge_r >= CX_RATIO_LO && edge_r <= CX_RATIO_HI);

    /* Per-language linearity, from the same run pair (scoped by subtree). If
     * scoped counting returns 0 for the base run the scope semantics changed —
     * surface that rather than silently skipping. */
    for (int t = 0; t < CX_TEMPLATE_COUNT; t++) {
        ASSERT_GT(a->lang_nodes[t], 0);
        double lr = cx_ratio(b->lang_nodes[t], a->lang_nodes[t]);
        printf("    %-8s nodes %d -> %d (ratio %.2f)\n", CX_TEMPLATES[t].dirname, a->lang_nodes[t],
               b->lang_nodes[t], lr);
        ASSERT_TRUE(lr >= CX_RATIO_LO && lr <= CX_RATIO_HI);
    }
    PASS();
}

/* The infra resolvers (K8s references / selectors / CRDs / chartRefs, the Go
 * operator's controller kinds, CRD types and loaded charts, the shell layer's
 * manifest dirs and make rules, Terraform's helm_release blocks) examine
 * candidates by name. Independent
 * copies share every name, so a candidate set that is not scoped to the
 * bundle / package / module grows with the copy count: the examined count
 * goes ~4x per doubling while the edges stay ~2x. Both are gated, on the
 * fixture corpora's own subtrees, and neither may be vacuous. */
TEST(complexity_infra_resolution_is_linear) {
    if (cx_measure_pair() != 0) {
        FAIL("failed to build/run the complexity corpus pair");
    }
    const CxMetrics *a = &g_cx_base;
    const CxMetrics *b = &g_cx_doubled;
    printf("    k8s_candidates %llu -> %llu  op_candidates %llu -> %llu\n",
           (unsigned long long)a->k8s_candidates, (unsigned long long)b->k8s_candidates,
           (unsigned long long)a->op_candidates, (unsigned long long)b->op_candidates);
    ASSERT_GT((long long)a->k8s_candidates, 0);
    ASSERT_GT((long long)a->op_candidates, 0);
    double kr = cx_ratio((double)b->k8s_candidates, (double)a->k8s_candidates);
    double orr = cx_ratio((double)b->op_candidates, (double)a->op_candidates);
    printf("    k8s_candidates ratio %.2f  op_candidates ratio %.2f\n", kr, orr);
    ASSERT_TRUE(kr >= CX_RATIO_LO && kr <= CX_RATIO_HI);
    ASSERT_TRUE(orr >= CX_RATIO_LO && orr <= CX_RATIO_HI);
    printf("    sh_candidates %llu -> %llu  tf_candidates %llu -> %llu\n",
           (unsigned long long)a->sh_candidates, (unsigned long long)b->sh_candidates,
           (unsigned long long)a->tf_candidates, (unsigned long long)b->tf_candidates);
    ASSERT_GT((long long)a->sh_candidates, 0);
    ASSERT_GT((long long)a->tf_candidates, 0);
    double sr = cx_ratio((double)b->sh_candidates, (double)a->sh_candidates);
    double fr = cx_ratio((double)b->tf_candidates, (double)a->tf_candidates);
    printf("    sh_candidates ratio %.2f  tf_candidates ratio %.2f\n", sr, fr);
    ASSERT_TRUE(sr >= CX_RATIO_LO && sr <= CX_RATIO_HI);
    ASSERT_TRUE(fr >= CX_RATIO_LO && fr <= CX_RATIO_HI);
    printf("    dk_hub_edges %llu -> %llu\n", (unsigned long long)a->dk_hub_edges,
           (unsigned long long)b->dk_hub_edges);
    ASSERT_GT((long long)a->dk_hub_edges, 0);
    double dr = cx_ratio((double)b->dk_hub_edges, (double)a->dk_hub_edges);
    printf("    dk_hub_edges ratio %.2f\n", dr);
    ASSERT_TRUE(dr >= CX_RATIO_LO && dr <= CX_RATIO_HI);
    for (int i = 0; i < CX_INFRA_COUNT; i++) {
        ASSERT_GT(a->infra_nodes[i], 0);
        ASSERT_GT(a->infra_edges[i], 0);
        double nr = cx_ratio(b->infra_nodes[i], a->infra_nodes[i]);
        double er = cx_ratio(b->infra_edges[i], a->infra_edges[i]);
        printf("    %-14s nodes %d -> %d (%.2f)  edges %d -> %d (%.2f)\n", CX_INFRA_DIRS[i],
               a->infra_nodes[i], b->infra_nodes[i], nr, a->infra_edges[i], b->infra_edges[i], er);
        ASSERT_TRUE(nr >= CX_RATIO_LO && nr <= CX_RATIO_HI);
        ASSERT_TRUE(er >= CX_RATIO_LO && er <= CX_RATIO_HI);
    }
    PASS();
}

/* Σ per-file registry work must be linear in independent copies. This is the
 * counter that measured ~4x/doubling on the v0.10.x Java path (defs_per_file
 * tracked defs_total — #1669). Guarded against vacuous passes: the base run
 * must have produced real registry work, so an accidental zeroing of the
 * counter fails loudly instead of green-washing the gate. */
TEST(complexity_perfile_registry_work_is_linear) {
    if (cx_measure_pair() != 0) {
        FAIL("failed to build/run the complexity corpus pair");
    }
    const CxMetrics *a = &g_cx_base;
    const CxMetrics *b = &g_cx_doubled;

    printf("    perfile_defs %llu -> %llu  build_files %llu -> %llu  filter_failed %llu\n",
           (unsigned long long)a->perfile_defs, (unsigned long long)b->perfile_defs,
           (unsigned long long)a->build_files, (unsigned long long)b->build_files,
           (unsigned long long)(a->filter_failed + b->filter_failed));

    /* Non-vacuous: the corpus includes languages that register per-file defs
     * (Java at minimum). If this is ever 0 the counter wiring broke — that is
     * a test defect to fix, not a pass. */
    ASSERT_GT((long long)a->perfile_defs, (long long)CX_MIN_BASE_WORK);

    double defs_r = cx_ratio((double)b->perfile_defs, (double)a->perfile_defs);
    printf("    perfile_defs ratio %.2f (linear ~2, files x corpus ~4)\n", defs_r);
    ASSERT_TRUE(defs_r <= CX_RATIO_HI);

    /* Recorded, deliberately NOT gated (the O10 note): tail_candidates and
     * fallback_rows are legitimately superlinear under replication TODAY —
     * same-short-name candidate sets grow with the copy count by design of
     * the current tail scan. Both measured ~1 ns/unit (#1669: indexing the
     * scan away cut 242M visits for 0% wall). Gate them only after those
     * scans are bounded; until then they are trend data for the report. */
    printf("    [info] tail_lookups %llu -> %llu  tail_candidates %llu -> %llu  fallback_rows "
           "%llu -> %llu\n",
           (unsigned long long)a->tail_lookups, (unsigned long long)b->tail_lookups,
           (unsigned long long)a->tail_candidates, (unsigned long long)b->tail_candidates,
           (unsigned long long)a->fallback_rows, (unsigned long long)b->fallback_rows);
    PASS();
}

static CxMetrics g_cx_big_base;
static CxMetrics g_cx_big_doubled;
static bool g_cx_big_measured = false;

static int cx_measure_bigpkg_pair(void) {
    if (g_cx_big_measured) {
        return 0;
    }
    if (cx_measure(cx_build_bigpkg, "cbm_cxbig_a", "cbm_cxbig_b", &g_cx_big_base,
                   &g_cx_big_doubled) != 0) {
        return -1;
    }
    g_cx_big_measured = true;
    return 0;
}

/* One growing package instead of independent modules. Nodes and edges must
 * still be linear in file count — and so must Σ per-file registry work: a
 * build scoped to the file's MODULE or NAMESPACE pays package-size per file
 * and lands at ratio ~4 here. Only a per-FILE-scoped build stays at ~2. This
 * is the exact #1669 growth pattern (per-file work tracking corpus share). */
TEST(complexity_shared_package_growth_stays_linear) {
    if (cx_measure_bigpkg_pair() != 0) {
        FAIL("failed to build/run the big-package corpus pair");
    }
    const CxMetrics *a = &g_cx_big_base;
    const CxMetrics *b = &g_cx_big_doubled;
    ASSERT_TRUE(cx_resolver_linear(a, b));

    double node_r = cx_ratio(b->nodes, a->nodes);
    double edge_r = cx_ratio(b->edges, a->edges);
    printf("    nodes %d -> %d (ratio %.2f)  edges %d -> %d (ratio %.2f)\n", a->nodes, b->nodes,
           node_r, a->edges, b->edges, edge_r);
    ASSERT_GT(a->nodes, 30);
    ASSERT_TRUE(node_r >= CX_RATIO_LO && node_r <= CX_RATIO_HI);
    ASSERT_TRUE(edge_r >= CX_RATIO_LO && edge_r <= CX_RATIO_HI);

    printf("    perfile_defs %llu -> %llu (ratio %.2f; linear ~2, namespace/module-scoped ~4)\n",
           (unsigned long long)a->perfile_defs, (unsigned long long)b->perfile_defs,
           cx_ratio((double)b->perfile_defs, (double)a->perfile_defs));
    /* Non-vacuous floor (false-guard audit): the package produces real
     * registry work; zero means the counter wiring broke. */
    ASSERT_GT((long long)a->perfile_defs, (long long)CX_MIN_BASE_WORK);
    double defs_r = cx_ratio((double)b->perfile_defs, (double)a->perfile_defs);
    ASSERT_TRUE(defs_r <= CX_RATIO_HI);
    PASS();
}

TEST(complexity_py_submodule_probe_avoids_registry_scan) {
    static const char *const keys[] = {"CBM_WORKERS", "CBM_INDEX_SINGLE_THREAD"};
    char *saved[2] = {NULL, NULL};
    bool ready = true;
    for (int i = 0; i < 2; i++) {
        const char *old = getenv(keys[i]);
        saved[i] = old ? strdup(old) : NULL;
        if (old && !saved[i])
            ready = false;
    }
    CxMetrics a = {0}, b = {0};
    int rc = -1, restored = 0;
    if (ready) {
        int configured = cbm_setenv(keys[0], "2", 1);
        configured |= cbm_unsetenv(keys[1]);
        if (configured == 0)
            rc = cx_measure(cx_build_pyext, "cbm_cxpy_a", "cbm_cxpy_b", &a, &b);
        for (int i = 0; i < 2; i++)
            restored |= saved[i] ? cbm_setenv(keys[i], saved[i], 1) : cbm_unsetenv(keys[i]);
    }
    for (int i = 0; i < 2; i++)
        free(saved[i]);
    /* cx_measure removes both owned corpora before any assertion below. */
    ASSERT_TRUE(ready);
    ASSERT_EQ(restored, 0);
    ASSERT_EQ(rc, 0);
    int files = CX_K_BASE * CX_PYEXT_FILES_PER_MOD;
    ASSERT_EQ(a.errors, 0);
    ASSERT_EQ(b.errors, 0);
    ASSERT_EQ(a.pyext_files, files);
    ASSERT_EQ(b.pyext_files, files * 2);
    ASSERT_EQ(a.pyext_defs, files * CX_PYEXT_DEFS_PER_FILE);
    ASSERT_EQ(b.pyext_defs, files * 2 * CX_PYEXT_DEFS_PER_FILE);
    ASSERT_EQ(a.pyext_controls, files);
    ASSERT_EQ(b.pyext_controls, files * 2);
    ASSERT_GT((long long)a.py_probes, (long long)CX_MIN_BASE_WORK);
    ASSERT_GT((long long)b.py_probes, (long long)CX_MIN_BASE_WORK);
    ASSERT_GT((long long)a.py_probe_visits, 0);
    ASSERT_GT((long long)b.py_probe_visits, 0);
    double probes_ratio = cx_ratio((double)b.py_probes, (double)a.py_probes);
    double visits_ratio = cx_ratio((double)b.py_probe_visits, (double)a.py_probe_visits);
    printf("    py_probes %llu -> %llu (%.2f), visits %llu -> %llu (%.2f)\n",
           (unsigned long long)a.py_probes, (unsigned long long)b.py_probes, probes_ratio,
           (unsigned long long)a.py_probe_visits, (unsigned long long)b.py_probe_visits,
           visits_ratio);
    ASSERT_TRUE(probes_ratio >= CX_RATIO_LO && probes_ratio <= CX_RATIO_HI);
    ASSERT_TRUE(visits_ratio <= CX_RATIO_HI);
    /* The direct registry test also enforces a logarithmic comparison bound,
     * independently of fixed stdlib size and this corpus-growth ratio. */
    PASS();
}

/* Throughput report — information only, never a gate (CI-determinism rule:
 * rates depend on the machine and scheduler, so a threshold would be a
 * lottery). Written locally under private/ (gitignored); CBM_COMPLEXITY_
 * REPORT_DIR overrides. Skipped on starved legs where rates are meaningless. */
TEST(complexity_throughput_report_written) {
    const char *skip_perf = getenv("CBM_SKIP_PERF");
    if (skip_perf && skip_perf[0] == '1') {
        /* Deliberate operator config, not a hidden environment failure
         * (no-skips policy): rates measured under CBM_SKIP_PERF starvation
         * would only mislead, so reporting is OFF and there is nothing left
         * for this test to assert. */
        fprintf(stderr, "  [complexity] CBM_SKIP_PERF=1: throughput report disabled by config\n");
        PASS();
    }
    if (cx_measure_pair() != 0) {
        FAIL("failed to build/run the complexity corpus pair");
    }
    const char *dir = getenv("CBM_COMPLEXITY_REPORT_DIR");
    if (!dir || !dir[0]) {
        dir = "private/benchmarks";
    }
    if (th_mkdir_p(dir) != 0) {
        FAIL("report dir not creatable (set CBM_COMPLEXITY_REPORT_DIR to a writable path)");
    }
    char path[1024];
    snprintf(path, sizeof(path), "%s/complexity-%lld.json", dir, (long long)time(NULL));
    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f);

    const CxMetrics *a = &g_cx_base;
    const CxMetrics *b = &g_cx_doubled;
#if defined(__APPLE__)
    const char *plat = "darwin";
#elif defined(_WIN32)
    const char *plat = "windows";
#else
    const char *plat = "linux";
#endif
#if defined(__aarch64__) || defined(_M_ARM64)
    const char *arch = "arm64";
#else
    const char *arch = "x86_64";
#endif
    fprintf(f, "{\n  \"schema\": 1,\n  \"suite\": \"complexity\",\n");
    fprintf(f, "  \"timestamp\": %lld,\n  \"platform\": \"%s\",\n  \"arch\": \"%s\",\n",
            (long long)time(NULL), plat, arch);
    fprintf(f, "  \"k_base\": %d,\n  \"files_per_module\": %d,\n", CX_K_BASE, CX_FILES_PER_MOD);
    fprintf(f, "  \"runs\": [\n");
    const CxMetrics *runs[2] = {a, b};
    for (int i = 0; i < 2; i++) {
        const CxMetrics *m = runs[i];
        fprintf(f,
                "    {\"k\": %d, \"nodes\": %d, \"edges\": %d, \"wall_s\": %.3f,\n"
                "     \"nodes_per_s\": %.0f, \"edges_per_s\": %.0f,\n"
                "     \"perfile_defs\": %llu, \"tail_candidates\": %llu, \"fallback_rows\": "
                "%llu, \"resolver\": {",
                i == 0 ? CX_K_BASE : CX_K_BASE * 2, m->nodes, m->edges, m->wall_s,
                m->wall_s > 0 ? (double)m->nodes / m->wall_s : 0.0,
                m->wall_s > 0 ? (double)m->edges / m->wall_s : 0.0,
                (unsigned long long)m->perfile_defs, (unsigned long long)m->tail_candidates,
                (unsigned long long)m->fallback_rows);
        for (unsigned event = 0; event < CBM_RESOLVE_STAT_COUNT; event++)
            fprintf(f, "%s\"%s\": %llu", event ? ", " : "",
                    cbm_resolve_stat_name((cbm_resolve_stat_t)event),
                    (unsigned long long)m->resolver.values[event]);
        fprintf(f, "}}%s\n", i == 0 ? "," : "");
    }
    fprintf(f, "  ],\n");
    fprintf(f, "  \"languages\": [\n");
    for (int t = 0; t < CX_TEMPLATE_COUNT; t++) {
        fprintf(f,
                "    {\"name\": \"%s\", \"nodes\": [%d, %d], \"edges\": [%d, %d], "
                "\"node_ratio\": %.3f}%s\n",
                CX_TEMPLATES[t].dirname, a->lang_nodes[t], b->lang_nodes[t], a->lang_edges[t],
                b->lang_edges[t], cx_ratio(b->lang_nodes[t], a->lang_nodes[t]),
                t + 1 < CX_TEMPLATE_COUNT ? "," : "");
    }
    fprintf(f, "  ],\n");
    fprintf(f, "  \"passes_ms\": {\n");
    for (int i = 0; i < 2; i++) {
        const CxMetrics *m = runs[i];
        fprintf(f, "    \"k%d\": {", i == 0 ? CX_K_BASE : CX_K_BASE * 2);
        for (int j = 0; j < m->pass_count; j++) {
            fprintf(f, "%s\"%s\": %ld", j == 0 ? "" : ", ", m->passes[j].name, m->passes[j].ms);
        }
        fprintf(f, "}%s\n", i == 0 ? "," : "");
    }
    fprintf(f, "  },\n");
    fprintf(f, "  \"ratios\": {\"nodes\": %.3f, \"edges\": %.3f, \"perfile_defs\": %.3f},\n",
            cx_ratio(b->nodes, a->nodes), cx_ratio(b->edges, a->edges),
            cx_ratio((double)b->perfile_defs, (double)a->perfile_defs));
    /* Languages without a provider, so the coverage boundary is explicit in
     * the artifact rather than implied. */
    fprintf(f, "  \"skipped_languages\": [");
    bool first = true;
    for (int lang = 0; lang < CBM_LANG_COUNT; lang++) {
        bool covered = false;
        for (int t = 0; t < CX_TEMPLATE_COUNT; t++) {
            if (CX_TEMPLATES[t].lang == (CBMLanguage)lang) {
                covered = true;
            }
        }
        char fixdir[512];
        if (!covered && cx_fixture_dir_for((CBMLanguage)lang, fixdir, sizeof(fixdir))) {
            covered = true;
        }
        if (!covered) {
            const char *name = cbm_language_name((CBMLanguage)lang);
            if (name && name[0]) {
                fprintf(f, "%s\"%s\"", first ? "" : ", ", name);
                first = false;
            }
        }
    }
    fprintf(f, "]\n}\n");
    fclose(f);
    printf("    report: %s\n", path);
    PASS();
}

/* Importance scoring must stay linear in the graph. Its generic-name
 * multiplier needs |{files a name is defined in}|; computing that per NODE
 * instead of once per distinct NAME makes a same-name group of size k cost
 * O(k^3) — measured on a large Java corpus as a multi-minute cost for a
 * handful of names. Under replication the same-name groups grow with the copy
 * count, so the per-node shape lands at ~4x per doubling here while the
 * memoized shape stays at ~2x. Gated on the counter ratio, never on wall time
 * (O9). This counter is GATED, not merely recorded: the whole point of the
 * work is that this quantity must not go superlinear. */
TEST(complexity_importance_scoring_is_linear) {
    if (cx_measure_pair() != 0) {
        FAIL("failed to build/run the complexity corpus pair");
    }
    const CxMetrics *a = &g_cx_base;
    const CxMetrics *b = &g_cx_doubled;

    printf("    imp_nodes %llu -> %llu  imp_name_visits %llu -> %llu\n",
           (unsigned long long)a->imp_nodes, (unsigned long long)b->imp_nodes,
           (unsigned long long)a->imp_name_visits, (unsigned long long)b->imp_name_visits);

    /* Non-vacuous, twice over: the pass must have run at all (a miscounted
     * PREDUMP_PASS_COUNT would silently skip it and zero both counters), and
     * the base leg must have produced enough work for a ratio to mean
     * anything. A zero here is a wiring defect to fix, never a pass. */
    ASSERT_GT((long long)a->imp_nodes, 0);
    ASSERT_GT((long long)a->imp_name_visits, (long long)CX_MIN_BASE_WORK);

    double node_r = cx_ratio((double)b->imp_nodes, (double)a->imp_nodes);
    double visit_r = cx_ratio((double)b->imp_name_visits, (double)a->imp_name_visits);
    printf("    imp_nodes ratio %.2f  imp_name_visits ratio %.2f (linear ~2, per-node ~4)\n",
           node_r, visit_r);
    ASSERT_TRUE(node_r >= CX_RATIO_LO && node_r <= CX_RATIO_HI);
    ASSERT_TRUE(visit_r <= CX_RATIO_HI);
    PASS();
}

/* ── Lexer work in error recovery (#2176) ──────────────────────────────
 * tree-sitter lexes an unparseable stretch by retrying at every byte with
 * every external token marked valid. The ReScript scanner then ran its
 * template-string loop from each byte to the next '`', '$', '\\' or NUL — to
 * the end of the file when there is none — and threw the result away. That
 * is O(stretch) per byte, O(n^2) per file, all inside lexing where the parse
 * budget's progress callback never runs; a binary Godot `.res` of high bytes
 * (or plain text such as a run of '~') was dropped by the clock instead of
 * parsed. Work is counted as the bytes the lexer pulls through a chunked
 * TSInput: a pure function of (grammar, input), independent of speed. */
enum { CX_LEX_CHUNK = 64, CX_LEX_BASE_BYTES = 4096 };

typedef struct {
    const char *src;
    uint32_t len;
    uint64_t bytes_pulled;
} CxLexInput;

static const char *cx_lex_read(void *payload, uint32_t byte_index, TSPoint position,
                               uint32_t *bytes_read) {
    (void)position;
    CxLexInput *in = (CxLexInput *)payload;
    if (byte_index >= in->len) {
        *bytes_read = 0;
        return "";
    }
    uint32_t n = in->len - byte_index;
    if (n > CX_LEX_CHUNK) {
        n = CX_LEX_CHUNK;
    }
    in->bytes_pulled += n;
    *bytes_read = n;
    return in->src + byte_index;
}

/* Bytes pulled while parsing `len` copies of `fill` as ReScript; 0 on failure. */
static uint64_t cx_rescript_lex_work(unsigned char fill, uint32_t len) {
    char *src = malloc(len);
    TSParser *parser = ts_parser_new();
    uint64_t work = 0;
    if (src && parser && ts_parser_set_language(parser, cbm_ts_language(CBM_LANG_RESCRIPT))) {
        memset(src, fill, len);
        CxLexInput in = {src, len, 0};
        TSInput input = {&in, cx_lex_read, TSInputEncodingUTF8, NULL};
        TSTree *tree = ts_parser_parse(parser, NULL, input);
        if (tree) {
            work = in.bytes_pulled;
            ts_tree_delete(tree);
        }
    }
    if (parser) {
        ts_parser_delete(parser);
    }
    free(src);
    return work;
}

TEST(complexity_rescript_error_recovery_lexing_is_linear) {
    /* 0xFF: the reporter's binary bytes (invalid UTF-8). '~': plain ASCII text
     * that ReScript cannot parse either — the defect is not binary-only. */
    static const unsigned char fills[] = {0xFF, '~'};
    for (size_t i = 0; i < sizeof(fills); i++) {
        uint64_t base = cx_rescript_lex_work(fills[i], CX_LEX_BASE_BYTES);
        uint64_t doubled = cx_rescript_lex_work(fills[i], 2 * CX_LEX_BASE_BYTES);
        double r = cx_ratio((double)doubled, (double)base);
        printf("    fill 0x%02x: lexer bytes %llu -> %llu  ratio %.2f (linear ~2, quadratic ~4)\n",
               fills[i], (unsigned long long)base, (unsigned long long)doubled, r);
        /* Non-vacuous: the lexer must at least have read the input once. */
        ASSERT_GTE(base, (uint64_t)CX_LEX_BASE_BYTES);
        ASSERT_TRUE(r >= CX_RATIO_LO && r <= CX_RATIO_HI);
    }
    PASS();
}

TEST(complexity_py_module_ranges_preserve_calls_with_bounded_work) {
    static const char *const keys[] = {"CBM_WORKERS", "CBM_INDEX_SINGLE_THREAD"};
    char *saved[2] = {NULL, NULL};
    bool ready = true;
    for (int i = 0; i < 2; i++) {
        const char *old = getenv(keys[i]);
        saved[i] = old ? strdup(old) : NULL;
        if (old && !saved[i])
            ready = false;
    }
    CxMetrics a = {0}, b = {0};
    int rc = -1, restored = 0;
    if (ready) {
        int configured = cbm_setenv(keys[0], "2", 1);
        configured |= cbm_unsetenv(keys[1]);
        if (configured == 0)
            rc = cx_measure(cx_build_pyrange, "cbm_cxrange_a", "cbm_cxrange_b", &a, &b);
        for (int i = 0; i < 2; i++)
            restored |= saved[i] ? cbm_setenv(keys[i], saved[i], 1) : cbm_unsetenv(keys[i]);
    }
    for (int i = 0; i < 2; i++)
        free(saved[i]);
    /* Both owned corpora are removed and environment restored before assertions. */
    ASSERT_TRUE(ready);
    ASSERT_EQ(restored, 0);
    ASSERT_EQ(rc, 0);
    const int files = CX_K_BASE * CX_PYRANGE_FILES_PER_K;
    ASSERT_GT(files, 50);
    ASSERT_EQ(a.errors, 0);
    ASSERT_EQ(b.errors, 0);
    ASSERT_EQ(a.pyrange_files, files);
    ASSERT_EQ(b.pyrange_files, files * 2);
    ASSERT_EQ(a.pyrange_defs, files * CX_PYRANGE_DEFS);
    ASSERT_EQ(b.pyrange_defs, files * 2 * CX_PYRANGE_DEFS);
    ASSERT_EQ(a.pyrange_classes, files * CX_PYRANGE_DEFS);
    ASSERT_EQ(b.pyrange_classes, files * 2 * CX_PYRANGE_DEFS);
    ASSERT_EQ(a.pyrange_peer_classes, files);
    ASSERT_EQ(b.pyrange_peer_classes, files * 2);
    ASSERT_EQ(a.pyrange_controls, files);
    ASSERT_EQ(b.pyrange_controls, files * 2);
    ASSERT_GTE(a.py_class_binds, (uint64_t)files);
    ASSERT_GTE(b.py_class_binds, (uint64_t)(files * 2));
    ASSERT_GTE(a.py_wildcards, (uint64_t)(files * CX_PYRANGE_WILDCARDS));
    ASSERT_GTE(b.py_wildcards, (uint64_t)(files * 2 * CX_PYRANGE_WILDCARDS));
    ASSERT_GT(a.py_class_visits, (uint64_t)0);
    ASSERT_GT(b.py_class_visits, (uint64_t)0);
    ASSERT_GT(a.py_wildcard_visits, (uint64_t)0);
    ASSERT_GT(b.py_wildcard_visits, (uint64_t)0);
    double classes = cx_ratio((double)b.py_class_binds, (double)a.py_class_binds);
    double wildcards = cx_ratio((double)b.py_wildcards, (double)a.py_wildcards);
    ASSERT_TRUE(classes >= CX_RATIO_LO && classes <= CX_RATIO_HI);
    ASSERT_TRUE(wildcards >= CX_RATIO_LO && wildcards <= CX_RATIO_HI);
    ASSERT_TRUE(cx_ratio((double)b.py_class_visits, (double)a.py_class_visits) <= CX_RATIO_HI);
    ASSERT_TRUE(cx_ratio((double)b.py_wildcard_visits, (double)a.py_wildcard_visits) <=
                CX_RATIO_HI);
    /* These counters exclude sorting/string-length work. The direct registry
     * tests separately pin absolute bounds and exact ordered answers. */
    PASS();
}

/* One file whose top level grows, not more files. TypeScript's declaration
 * baselines held the semantic pass for over 11 minutes on one such root, because
 * fetching a child by index walks from the first child: the steps to enumerate
 * n children were n*n/2. Doubling the file must only double them. */
enum { CX_FLAT_CLASSES_PER_K = 500 };

static int cx_build_flat_ts(const char *root, int k) {
    char path[1200];
    int pn = snprintf(path, sizeof(path), "%s/flat.ts", root);
    if (pn < 0 || (size_t)pn >= sizeof(path))
        return -1;
    FILE *f = fopen(path, "w");
    if (!f)
        return -1;
    bool written = true;
    for (int i = 0; written && i < k * CX_FLAT_CLASSES_PER_K; i++)
        written = fprintf(f, "export class Flat%d {}\n", i) >= 0;
    return fclose(f) == 0 && written ? 0 : -1;
}

/* Java, Kotlin and C# bodies: each identifier asks whether it is a local. */
static int cx_build_long_java_body(const char *root, int k) {
    char path[1200];
    int pn = snprintf(path, sizeof(path), "%s/Body.java", root);
    if (pn < 0 || (size_t)pn >= sizeof(path))
        return -1;
    FILE *f = fopen(path, "w");
    if (!f)
        return -1;
    bool written = fprintf(f, "class Body { int f(int a) { return a; } void big() {\n") >= 0;
    for (int i = 0; written && i < k * CX_FLAT_CLASSES_PER_K; i++)
        written = fprintf(f, "  int v%d = f(1);\n", i) >= 0;
    written = written && fprintf(f, "} }\n") >= 0;
    return fclose(f) == 0 && written ? 0 : -1;
}
TEST(complexity_oo_local_lookup_is_linear) {
    CxMetrics a = {0}, b = {0};
    ASSERT_EQ(cx_measure(cx_build_long_java_body, "cbm_cxbody_a", "cbm_cxbody_b", &a, &b), 0);
    uint64_t first = a.resolver.values[CBM_RESOLVE_STAT_LOOKUP_PROBES];
    uint64_t second = b.resolver.values[CBM_RESOLVE_STAT_LOOKUP_PROBES];
    double ratio = cx_ratio((double)second, (double)first);
    printf("    oo locals lookup_probes %llu -> %llu (ratio %.2f, linear ~2, quadratic ~4)\n",
           (unsigned long long)first, (unsigned long long)second, ratio);
    ASSERT_GTE(first, (uint64_t)(CX_K_BASE * CX_FLAT_CLASSES_PER_K));
    ASSERT_TRUE(ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI);
    PASS();
}

/* The same bodies asking which import a name means. */
static int cx_build_long_java_imports(const char *root, int k) {
    char path[1200];
    int pn = snprintf(path, sizeof(path), "%s/Imports.java", root);
    if (pn < 0 || (size_t)pn >= sizeof(path))
        return -1;
    FILE *f = fopen(path, "w");
    if (!f)
        return -1;
    int lines = k * CX_FLAT_CLASSES_PER_K;
    bool written = true;
    for (int i = 0; written && i < lines; i++)
        written = fprintf(f, "import q.r%d.S%d;\n", i, i) >= 0;
    written = written && fprintf(f, "class Imports { void big() {\n") >= 0;
    for (int i = 0; written && i < lines; i++)
        written = fprintf(f, "  S%d.call();\n", i) >= 0;
    written = written && fprintf(f, "} }\n") >= 0;
    return fclose(f) == 0 && written ? 0 : -1;
}
TEST(complexity_oo_explicit_imports_are_linear) {
    CxMetrics a = {0}, b = {0};
    ASSERT_EQ(cx_measure(cx_build_long_java_imports, "cbm_cximp_a", "cbm_cximp_b", &a, &b), 0);
    uint64_t first = a.resolver.values[CBM_RESOLVE_STAT_LOOKUP_PROBES];
    uint64_t second = b.resolver.values[CBM_RESOLVE_STAT_LOOKUP_PROBES];
    double ratio = cx_ratio((double)second, (double)first);
    printf("    oo imports lookup_probes %llu -> %llu (ratio %.2f, linear ~2, quadratic ~4)\n",
           (unsigned long long)first, (unsigned long long)second, ratio);
    ASSERT_GTE(first, (uint64_t)(CX_K_BASE * CX_FLAT_CLASSES_PER_K));
    ASSERT_TRUE(ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI);
    PASS();
}

TEST(complexity_flat_file_syntax_walk_is_linear) {
    CxMetrics a = {0}, b = {0};
    ASSERT_EQ(cx_measure(cx_build_flat_ts, "cbm_cxflat_a", "cbm_cxflat_b", &a, &b), 0);
    uint64_t first = a.resolver.values[CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS];
    uint64_t second = b.resolver.values[CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS];
    double ratio = cx_ratio((double)second, (double)first);
    printf("    syntax_child_steps %llu -> %llu (ratio %.2f, linear ~2, quadratic ~4)\n",
           (unsigned long long)first, (unsigned long long)second, ratio);
    /* Non-vacuous: every class at the root was reached in both runs. */
    ASSERT_GTE(first, (uint64_t)(CX_K_BASE * CX_FLAT_CLASSES_PER_K));
    ASSERT_GTE(second, (uint64_t)(2 * CX_K_BASE * CX_FLAT_CLASSES_PER_K));
    ASSERT_GTE(a.nodes, CX_K_BASE * CX_FLAT_CLASSES_PER_K);
    ASSERT_TRUE(ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI);
    PASS();
}

/* ── One file's source analysis ─────────────────────────────────────
 * These run the binding index alone, in-process, so a shape can be doubled cheaply and
 * each counter speaks for the analysis and nothing else. The plain mode asks tree-sitter
 * the way this code used to and charges the sibling steps that really takes: a fixture
 * that does not show about 4 there could not show a quadratic here either. */
enum { CX_BINDING_LINES = 1500, CX_BINDING_LINE_ROOM = 128, TARGET_FILES_SHORT = 12 };

typedef const char *(*CxLineFn)(char *out, size_t cap, int i);

/* The steps to reach a child: walking its siblings, or listing them once in an index. */
static uint64_t cx_stat_work(const cbm_resolve_stats_t *stats, cbm_resolve_stat_t stat) {
    cbm_resolve_stats_snapshot_t seen = cbm_resolve_stats_snapshot(stats);
    return seen.values[stat] + (stat == CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS
                                    ? seen.values[CBM_RESOLVE_STAT_SYNTAX_INDEX_NODES]
                                    : 0);
}
static uint64_t cx_binding_work(CBMLanguage language, const char *path, const char *head,
                                CxLineFn line, const char *tail, int lines, bool plain,
                                cbm_resolve_stat_t stat) {
    size_t room = strlen(head) + strlen(tail) + (size_t)lines * CX_BINDING_LINE_ROOM + 1;
    char *source = malloc(room);
    if (!source)
        return 0;
    size_t length = (size_t)snprintf(source, room, "%s", head);
    for (int i = 0; i < lines; i++) {
        char row[CX_BINDING_LINE_ROOM];
        length += (size_t)snprintf(source + length, room - length, "%s", line(row, sizeof(row), i));
    }
    length += (size_t)snprintf(source + length, room - length, "%s", tail);
    CBMFileResult *result =
        cbm_extract_file(source, (int)length, language, "test", path, 0, NULL, NULL);
    uint64_t work = 0;
    if (result) {
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_stats_t stats;
        cbm_resolve_stats_reset(&stats);
        cbm_resolve_stats_t *previous = cbm_resolve_stats_bind(&stats);
        cbm_syntax_index_test_set_plain(plain);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, result, language, source, length);
        cbm_syntax_index_test_set_plain(false);
        cbm_resolve_stats_bind(previous);
        if (index &&
            !strcmp(cbm_resolve_binding_status(index), "binding_complete_supported_subset"))
            work = cx_stat_work(&stats, stat);
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
    }
    free(source);
    return work;
}

static double cx_binding_ratio(CBMLanguage language, const char *path, const char *head,
                               CxLineFn line, const char *tail, bool plain, cbm_resolve_stat_t stat,
                               uint64_t *base) {
    uint64_t first =
        cx_binding_work(language, path, head, line, tail, CX_BINDING_LINES, plain, stat);
    uint64_t second =
        cx_binding_work(language, path, head, line, tail, CX_BINDING_LINES * 2, plain, stat);
    double ratio = cx_ratio((double)second, (double)first);
    printf("    %s%s %s %llu -> %llu (ratio %.2f)\n", path, plain ? " plain" : "",
           cbm_resolve_stat_name(stat), (unsigned long long)first, (unsigned long long)second,
           ratio);
    *base = first;
    return ratio;
}

/* Quadratic when asked plainly, linear through the syntax index. */
static bool cx_syntax_shape_binds(CBMLanguage language, const char *path, const char *head,
                                  CxLineFn line, const char *tail) {
    uint64_t base = 0;
    double plain = cx_binding_ratio(language, path, head, line, tail, true,
                                    CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS, &base);
    if (base < CX_BINDING_LINES || plain < 3.2)
        return false;
    double indexed = cx_binding_ratio(language, path, head, line, tail, false,
                                      CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS, &base);
    return base >= CX_BINDING_LINES && indexed >= CX_RATIO_LO && indexed <= CX_RATIO_HI;
}

static const char *cx_line_py_statement(char *out, size_t cap, int i) {
    snprintf(out, cap, "value_%d = make_%d(input_%d)\n", i, i, i);
    return out;
}
static const char *cx_line_ts_statement(char *out, size_t cap, int i) {
    snprintf(out, cap, "export const value_%d = make_%d(input_%d);\n", i, i, i);
    return out;
}
static const char *cx_line_ts_member(char *out, size_t cap, int i) {
    snprintf(out, cap, "  member_%d: helper_%d,\n", i, i);
    return out;
}
static const char *cx_line_py_member(char *out, size_t cap, int i) {
    snprintf(out, cap, "    value_%d = make_%d(input_%d)\n", i, i, i);
    return out;
}

/* A module's width: every occurrence asks for its parent. */
TEST(complexity_binding_wide_module_questions_are_linear) {
    ASSERT_TRUE(cx_syntax_shape_binds(CBM_LANG_PYTHON, "flat.py", "", cx_line_py_statement, ""));
    ASSERT_TRUE(
        cx_syntax_shape_binds(CBM_LANG_TYPESCRIPT, "flat.ts", "", cx_line_ts_statement, ""));
    PASS();
}

/* A wide node that is not the root: an object literal's members and a function's body. */
TEST(complexity_binding_wide_inner_nodes_are_linear) {
    ASSERT_TRUE(cx_syntax_shape_binds(CBM_LANG_TYPESCRIPT, "object.ts", "export const table = {\n",
                                      cx_line_ts_member, "};\n"));
    ASSERT_TRUE(
        cx_syntax_shape_binds(CBM_LANG_PYTHON, "body.py", "def body():\n", cx_line_py_member, ""));
    PASS();
}

/* An answer is exactly what tree-sitter says, including at the edges of the tree. */
TEST(complexity_syntax_index_answers_as_tree_sitter_does) {
    static const char source[] =
        "def f(a, b=1):\n    return [a, (b), {'k': a}]\n\nclass C:\n    x = 1\n";
    TSParser *parser = ts_parser_new();
    ASSERT_NOT_NULL(parser);
    ASSERT_TRUE(ts_parser_set_language(parser, cbm_ts_language(CBM_LANG_PYTHON)));
    TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)strlen(source));
    ts_parser_delete(parser);
    ASSERT_NOT_NULL(tree);
    cbm_syntax_index_t index;
    cbm_syntax_index_begin(&index, tree);
    TSTreeCursor cursor = ts_tree_cursor_new(ts_tree_root_node(tree));
    size_t visited = 0;
    bool same = true;
    for (bool more = true; more;) {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        visited++;
        same = same && ts_node_eq(cbm_syntax_parent(node), ts_node_parent(node));
        uint32_t count = ts_node_child_count(node);
        for (uint32_t i = 0; i <= count; i++)
            same = same && ts_node_eq(cbm_syntax_child(node, i), ts_node_child(node, i));
        for (uint32_t i = 0; i < count; i++) {
            TSNode before = i ? ts_node_child(node, i - 1) : (TSNode){0};
            TSNode child = ts_node_child(node, i);
            same = same && ts_node_eq(cbm_syntax_previous_child(child), before);
            cbm_syntax_index_test_set_plain(true);
            same = same && ts_node_eq(cbm_syntax_previous_child(child), before);
            cbm_syntax_index_test_set_plain(false);
        }
        uint32_t named = ts_node_named_child_count(node);
        for (uint32_t i = 0; i <= named; i++)
            same =
                same && ts_node_eq(cbm_syntax_named_child(node, i), ts_node_named_child(node, i));
        if (ts_tree_cursor_goto_first_child(&cursor))
            continue;
        while (!ts_tree_cursor_goto_next_sibling(&cursor)) {
            if (!ts_tree_cursor_goto_parent(&cursor)) {
                more = false;
                break;
            }
        }
    }
    ts_tree_cursor_delete(&cursor);
    cbm_syntax_index_end(&index);
    ts_tree_delete(tree);
    ASSERT_TRUE(same);
    ASSERT_GTE(visited, (size_t)20);
    PASS();
}

/* Past the width where children are listed: every position, and one past the end. */
TEST(complexity_syntax_index_wide_lists_match_tree_sitter) {
    enum { MEMBERS = 200 };
    char *source = malloc((size_t)MEMBERS * 32 + 64);
    ASSERT_NOT_NULL(source);
    size_t length = (size_t)snprintf(source, 64, "values = [\n");
    for (int i = 0; i < MEMBERS; i++)
        length += (size_t)snprintf(source + length, 32, "  item_%d,  # note\n", i);
    length += (size_t)snprintf(source + length, 8, "]\n");
    TSParser *parser = ts_parser_new();
    ASSERT_NOT_NULL(parser);
    ASSERT_TRUE(ts_parser_set_language(parser, cbm_ts_language(CBM_LANG_PYTHON)));
    TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)length);
    ts_parser_delete(parser);
    ASSERT_NOT_NULL(tree);
    TSNode list = ts_node_named_descendant_for_byte_range(ts_tree_root_node(tree), 9, 10);
    ASSERT_TRUE(!strcmp(ts_node_type(list), "list"));
    ASSERT_GT(ts_node_child_count(list), (uint32_t)MEMBERS);
    cbm_syntax_index_t index;
    cbm_syntax_index_begin(&index, tree);
    bool same = true;
    for (uint32_t i = 0; i <= ts_node_child_count(list); i++)
        same = same && ts_node_eq(cbm_syntax_child(list, i), ts_node_child(list, i));
    for (uint32_t i = 0; i <= ts_node_named_child_count(list); i++)
        same = same && ts_node_eq(cbm_syntax_named_child(list, i), ts_node_named_child(list, i));
    cbm_syntax_index_end(&index);
    ts_tree_delete(tree);
    free(source);
    ASSERT_TRUE(same);
    PASS();
}

/* Each helper is a declaration and a definition spelled alike, so a join on the name
 * alone visits every pair. Generated files repeat names exactly like this. */
static const char *cx_line_js_same_named(char *out, size_t cap, int i) {
    snprintf(out, cap, "export function outer_%d() { function helper() { return %d; } }\n", i, i);
    return out;
}
TEST(complexity_js_value_join_is_linear_in_a_repeated_name) {
    uint64_t base = 0;
    double ratio = cx_binding_ratio(CBM_LANG_TYPESCRIPT, "same.ts", "", cx_line_js_same_named, "",
                                    false, CBM_RESOLVE_STAT_CANDIDATE_VISITS, &base);
    ASSERT_GTE(base, (uint64_t)CX_BINDING_LINES);
    ASSERT_TRUE(ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI);
    PASS();
}

/* Each dotted import asks whether its package is bound yet. */
static const char *cx_line_py_dotted_import(char *out, size_t cap, int i) {
    snprintf(out, cap, "import package_%d.part\n", i);
    return out;
}
TEST(complexity_python_dotted_imports_are_linear) {
    uint64_t base = 0;
    double ratio = cx_binding_ratio(CBM_LANG_PYTHON, "imports.py", "", cx_line_py_dotted_import, "",
                                    false, CBM_RESOLVE_STAT_LOOKUP_PROBES, &base);
    ASSERT_GTE(base, (uint64_t)CX_BINDING_LINES);
    ASSERT_TRUE(ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI);
    PASS();
}

/* Each decorator's root is joined to the file's imports. */
static const char *cx_line_py_decorated(char *out, size_t cap, int i) {
    snprintf(out, cap, "from package_%d import deco_%d\n@deco_%d\ndef run_%d():\n    return 1\n", i,
             i, i, i);
    return out;
}
TEST(complexity_python_import_join_is_linear) {
    uint64_t base = 0;
    double ratio = cx_binding_ratio(CBM_LANG_PYTHON, "decorated.py", "", cx_line_py_decorated, "",
                                    false, CBM_RESOLVE_STAT_LOOKUP_PROBES, &base);
    ASSERT_GTE(base, (uint64_t)CX_BINDING_LINES);
    ASSERT_TRUE(ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI);
    PASS();
}

/* Extraction runs a language's own pass over the file, so these shapes count that. */
static uint64_t cx_extract_work(CBMLanguage language, const char *path, const char *head,
                                CxLineFn line, const char *tail, int lines, bool plain,
                                cbm_resolve_stat_t stat) {
    size_t room = strlen(head) + strlen(tail) + (size_t)lines * CX_BINDING_LINE_ROOM + 1;
    char *source = malloc(room);
    if (!source)
        return 0;
    size_t length = (size_t)snprintf(source, room, "%s", head);
    for (int i = 0; i < lines; i++) {
        char row[CX_BINDING_LINE_ROOM];
        length += (size_t)snprintf(source + length, room - length, "%s", line(row, sizeof(row), i));
    }
    length += (size_t)snprintf(source + length, room - length, "%s", tail);
    cbm_resolve_stats_t stats;
    cbm_resolve_stats_reset(&stats);
    cbm_resolve_stats_t *previous = cbm_resolve_stats_bind(&stats);
    cbm_syntax_index_test_set_plain(plain);
    CBMFileResult *result =
        cbm_extract_file(source, (int)length, language, "test", path, 0, NULL, NULL);
    cbm_syntax_index_test_set_plain(false);
    cbm_resolve_stats_bind(previous);
    uint64_t work = result ? cx_stat_work(&stats, stat) : 0;
    if (result)
        cbm_free_result(result);
    free(source);
    return work;
}
static double cx_extract_ratio(CBMLanguage language, const char *path, const char *head,
                               CxLineFn line, const char *tail, bool plain, cbm_resolve_stat_t stat,
                               uint64_t *base) {
    uint64_t first =
        cx_extract_work(language, path, head, line, tail, CX_BINDING_LINES, plain, stat);
    uint64_t second =
        cx_extract_work(language, path, head, line, tail, CX_BINDING_LINES * 2, plain, stat);
    double ratio = cx_ratio((double)second, (double)first);
    printf("    %s%s %s %llu -> %llu (ratio %.2f)\n", path, plain ? " plain" : "",
           cbm_resolve_stat_name(stat), (unsigned long long)first, (unsigned long long)second,
           ratio);
    *base = first;
    return ratio;
}

/* What the language's own pass compared while searching its registry and scopes. */
static uint64_t cx_extract_lsp_steps(CBMLanguage language, const char *path, const char *head,
                                     CxLineFn line, const char *tail, int lines) {
    size_t room = strlen(head) + strlen(tail) + (size_t)lines * CX_BINDING_LINE_ROOM + 1;
    char *source = malloc(room);
    if (!source)
        return 0;
    size_t length = (size_t)snprintf(source, room, "%s", head);
    for (int i = 0; i < lines; i++) {
        char row[CX_BINDING_LINE_ROOM];
        length += (size_t)snprintf(source + length, room - length, "%s", line(row, sizeof(row), i));
    }
    length += (size_t)snprintf(source + length, room - length, "%s", tail);
    (void)cbm_lsp_work_take();
    CBMFileResult *result =
        cbm_extract_file(source, (int)length, language, "test", path, 0, NULL, NULL);
    uint64_t work = cbm_lsp_work_take();
    if (result)
        cbm_free_result(result);
    free(source);
    return work;
}
/* Before a JavaScript file's registry was indexed, its own pass searched it from the top
 * for every function and every identifier in the body it inferred a return type from. */
static const char *cx_line_js_function_pair(char *out, size_t cap, int i) {
    snprintf(out, cap, "function f_%d(a) { return g_%d(a); }\nfunction g_%d(b) { return b; }\n", i,
             i, i);
    return out;
}
static const char *cx_line_kotlin_function_pair(char *out, size_t cap, int i) {
    snprintf(out, cap, "fun f_%d(a: Int): Int = g_%d(a)\nfun g_%d(b: Int): Int = b\n", i, i, i);
    return out;
}
/* The same with Rust's registration passes, one patch per function and per method. */
static const char *cx_line_rust_function_pair(char *out, size_t cap, int i) {
    snprintf(out, cap, "pub fn f_%d(a: i32) -> i32 { g_%d(a) }\nfn g_%d(b: i32) -> i32 { b }\n", i,
             i, i);
    return out;
}
static bool cx_pass_steps_are_linear(CBMLanguage language, const char *path, const char *head,
                                     CxLineFn line) {
    uint64_t first = cx_extract_lsp_steps(language, path, head, line, "", CX_BINDING_LINES);
    uint64_t second = cx_extract_lsp_steps(language, path, head, line, "", CX_BINDING_LINES * 2);
    double ratio = cx_ratio((double)second, (double)first);
    printf("    %s lsp steps %llu -> %llu (ratio %.2f)\n", path, (unsigned long long)first,
           (unsigned long long)second, ratio);
    return first >= CX_BINDING_LINES && ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI;
}
TEST(complexity_js_pass_registry_lookups_are_linear) {
    ASSERT_TRUE(cx_pass_steps_are_linear(CBM_LANG_JAVASCRIPT, "pairs.js", "export {};\n",
                                         cx_line_js_function_pair));
    PASS();
}
TEST(complexity_rust_pass_registration_is_linear) {
    ASSERT_TRUE(
        cx_pass_steps_are_linear(CBM_LANG_RUST, "pairs.rs", "", cx_line_rust_function_pair));
    PASS();
}
static const char *cx_line_go_type_and_method(char *out, size_t cap, int i) {
    snprintf(out, cap, "type T%d struct{ A int }\nfunc (t T%d) M() int { return t.A }\n", i, i);
    return out;
}
TEST(complexity_go_pass_registration_is_linear) {
    ASSERT_TRUE(cx_pass_steps_are_linear(CBM_LANG_GO, "types.go", "package p\n",
                                         cx_line_go_type_and_method));
    PASS();
}
/* A type name no import explains looked through every registered type. */
static const char *cx_line_java_unknown_type(char *out, size_t cap, int i) {
    snprintf(out, cap, "class A%d { Unknown%d field; Unknown%d get() { return field; } }\n", i, i,
             i);
    return out;
}
TEST(complexity_java_short_name_fallback_is_linear) {
    ASSERT_TRUE(
        cx_pass_steps_are_linear(CBM_LANG_JAVA, "Types.java", "", cx_line_java_unknown_type));
    PASS();
}
/* Each C# method and class patched the first registration of its name after a scan. */
static const char *cx_line_csharp_class(char *out, size_t cap, int i) {
    snprintf(out, cap, "class C%d { public int X; int F(int a) { return a; } }\n", i);
    return out;
}
static const char *cx_line_csharp_methods(char *out, size_t cap, int i) {
    snprintf(out, cap,
             "class C%d { public int X; int F(int a) { return a; } int G() { return 1; } "
             "int H() { return 1; } }\n",
             i);
    return out;
}
/* Every definition does linear work too, so it takes this many before the scans show. */
TEST(complexity_csharp_pass_registration_is_linear) {
    enum { LINES = 3000 };
    uint64_t first =
        cx_extract_lsp_steps(CBM_LANG_CSHARP, "methods.cs", "", cx_line_csharp_methods, "", LINES);
    uint64_t second = cx_extract_lsp_steps(CBM_LANG_CSHARP, "methods.cs", "",
                                           cx_line_csharp_methods, "", LINES * 2);
    double ratio = cx_ratio((double)second, (double)first);
    printf("    methods.cs lsp steps %llu -> %llu (ratio %.2f)\n", (unsigned long long)first,
           (unsigned long long)second, ratio);
    ASSERT_GTE(first, (uint64_t)LINES);
    ASSERT_TRUE(ratio <= CX_RATIO_HI);
    PASS();
}
/* A file of classes: each one's registration looked at all that were registered before. */
static const char *cx_line_kotlin_class(char *out, size_t cap, int i) {
    snprintf(out, cap, "class C%d { val x: Int = 1\n fun f(a: Int): Int = a }\n", i);
    return out;
}
TEST(complexity_kotlin_class_registration_is_linear) {
    ASSERT_TRUE(cx_pass_steps_are_linear(CBM_LANG_KOTLIN, "classes.kt", "package p\n",
                                         cx_line_kotlin_class));
    PASS();
}
static const char *cx_line_java_class(char *out, size_t cap, int i) {
    snprintf(out, cap, "class C%d extends B { int f; void m(int a) { } }\n", i);
    return out;
}
TEST(complexity_java_class_registration_is_linear) {
    ASSERT_TRUE(cx_pass_steps_are_linear(CBM_LANG_JAVA, "Classes.java", "", cx_line_java_class));
    PASS();
}
static const char *cx_line_ts_class(char *out, size_t cap, int i) {
    snprintf(out, cap, "export class C%d { x: number = 1; m(a: number): number { return a; } }\n",
             i);
    return out;
}
/* TypeScript's checker.ts names thousands of imports and makes a hundred thousand calls: the proof
 * looked through the imports for each callee and each annotation, and through all the narrowings of
 * a path for each receiver on it. */
static const char *cx_line_ts_imported_call(char *out, size_t cap, int i) {
    snprintf(out, cap, "import { f%d } from './m';\nf%d().m();\n", i, i);
    return out;
}
static const char *cx_line_ts_imported_annotation(char *out, size_t cap, int i) {
    snprintf(out, cap, "import { t%d } from './m';\nlet a%d: C[] = [];\na%d[0].m();\n", i, i, i);
    return out;
}
/* A narrowing covers its whole function, so the search for the one covering a receiver only
 * gets long across functions narrowing the same name. */
static const char *cx_line_ts_narrowing(char *out, size_t cap, int i) {
    snprintf(out, cap, "function f_%d(x: C) { if (x instanceof C) { x.m(); } }\n", i);
    return out;
}
TEST(complexity_ts_proof_callee_imports_are_linear) {
    ASSERT_TRUE(cx_pass_steps_are_linear(CBM_LANG_TYPESCRIPT, "calls.ts", "class C { m() {} }\n",
                                         cx_line_ts_imported_call));
    PASS();
}
TEST(complexity_ts_proof_annotation_imports_are_linear) {
    ASSERT_TRUE(cx_pass_steps_are_linear(CBM_LANG_TYPESCRIPT, "typed.ts", "class C { m() {} }\n",
                                         cx_line_ts_imported_annotation));
    PASS();
}
/* ── A file that never parses ───────────────────────────────────────
 * Declarations left open end up side by side under one error node, so every question
 * about a child is a question about a very wide parent. A 990 KB Kotlin file of these
 * lines held an index run for 50 minutes. Extraction has its index; a language's cross
 * pass parses again and needs its own. No index open means no work counted at all, so
 * the floor on the first count fails then too. */
static const char *cx_line_kotlin_unclosed(char *out, size_t cap, int i) {
    (void)out, (void)cap, (void)i;
    return "object A {\n";
}
static const char *cx_line_c_unclosed(char *out, size_t cap, int i) {
    (void)out, (void)cap, (void)i;
    return "struct A {\n";
}
static const char *cx_line_cpp_unclosed(char *out, size_t cap, int i) {
    (void)out, (void)cap, (void)i;
    return "namespace a {\n";
}
static const char *cx_line_js_unclosed(char *out, size_t cap, int i) {
    (void)out, (void)cap, (void)i;
    return "function a() {\n";
}
static const char *cx_line_dart_unclosed(char *out, size_t cap, int i) {
    (void)out, (void)cap, (void)i;
    return "class A {\n";
}
typedef void (*CxCrossFn)(CBMArena *arena, const char *source, int length,
                          CBMResolvedCallArray *out);
static void cx_cross_kotlin(CBMArena *arena, const char *source, int length,
                            CBMResolvedCallArray *out) {
    cbm_run_kotlin_lsp_cross(arena, source, length, "test.m", NULL, 0, NULL, NULL, 0, NULL, out);
}
static void cx_cross_c(CBMArena *arena, const char *source, int length, CBMResolvedCallArray *out) {
    (void)cbm_run_c_lsp_cross(arena, source, length, "test.m", false, NULL, 0, NULL, NULL, 0, NULL,
                              out);
}
static void cx_cross_cpp(CBMArena *arena, const char *source, int length,
                         CBMResolvedCallArray *out) {
    (void)cbm_run_c_lsp_cross(arena, source, length, "test.m", true, NULL, 0, NULL, NULL, 0, NULL,
                              out);
}
static void cx_cross_c_shared(CBMArena *arena, const char *source, int length,
                              CBMResolvedCallArray *out) {
    CBMTypeRegistry registry;
    cbm_registry_init(&registry, arena);
    cbm_registry_finalize(&registry);
    (void)cbm_run_c_lsp_cross_with_registry_status(arena, source, length, "test.m", false,
                                                   &registry, NULL, NULL, 0, NULL, out);
}
static uint64_t cx_cross_work(CxCrossFn run, CxLineFn line, int lines) {
    size_t room = (size_t)lines * CX_BINDING_LINE_ROOM + 1;
    char *source = malloc(room);
    if (!source)
        return 0;
    size_t length = 0;
    for (int i = 0; i < lines; i++) {
        char row[CX_BINDING_LINE_ROOM];
        length += (size_t)snprintf(source + length, room - length, "%s", line(row, sizeof(row), i));
    }
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out;
    memset(&out, 0, sizeof(out));
    cbm_resolve_stats_t stats;
    cbm_resolve_stats_reset(&stats);
    cbm_resolve_stats_t *previous = cbm_resolve_stats_bind(&stats);
    run(&arena, source, (int)length, &out);
    cbm_resolve_stats_bind(previous);
    uint64_t work = cx_stat_work(&stats, CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS);
    cbm_arena_destroy(&arena);
    free(source);
    return work;
}
static bool cx_cross_is_linear(CxCrossFn run, CxLineFn line, const char *name) {
    uint64_t first = cx_cross_work(run, line, CX_BINDING_LINES);
    uint64_t second = cx_cross_work(run, line, CX_BINDING_LINES * 2);
    double ratio = cx_ratio((double)second, (double)first);
    printf("    %s cross steps %llu -> %llu (ratio %.2f)\n", name, (unsigned long long)first,
           (unsigned long long)second, ratio);
    return first >= CX_BINDING_LINES && ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI;
}
static const char *cx_line_rust_unclosed(char *out, size_t cap, int i) {
    (void)out, (void)cap, (void)i;
    return "mod a {\n";
}
static void cx_cross_rust_facts(CBMArena *arena, const char *source, int length,
                                CBMResolvedCallArray *out) {
    (void)out;
    (void)cbm_rust_collect_source_facts(arena, source, length, "test.m", NULL, 0);
}
TEST(complexity_rust_source_facts_on_unclosed_nesting_are_linear) {
    ASSERT_TRUE(cx_cross_is_linear(cx_cross_rust_facts, cx_line_rust_unclosed, "unclosed.rs"));
    PASS();
}
TEST(complexity_kotlin_cross_on_unclosed_nesting_is_linear) {
    ASSERT_TRUE(cx_cross_is_linear(cx_cross_kotlin, cx_line_kotlin_unclosed, "unclosed.kt"));
    PASS();
}
TEST(complexity_c_cross_on_unclosed_nesting_is_linear) {
    ASSERT_TRUE(cx_cross_is_linear(cx_cross_c, cx_line_c_unclosed, "unclosed.c"));
    PASS();
}
TEST(complexity_cpp_cross_on_unclosed_nesting_is_linear) {
    ASSERT_TRUE(cx_cross_is_linear(cx_cross_cpp, cx_line_cpp_unclosed, "unclosed.cpp"));
    PASS();
}
TEST(complexity_c_shared_cross_on_unclosed_nesting_is_linear) {
    ASSERT_TRUE(cx_cross_is_linear(cx_cross_c_shared, cx_line_c_unclosed, "shared.c"));
    PASS();
}
TEST(complexity_js_proof_on_unclosed_nesting_is_linear) {
    uint64_t base = 0;
    double ratio = cx_extract_ratio(CBM_LANG_JAVASCRIPT, "unclosed.js", "", cx_line_js_unclosed, "",
                                    false, CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS, &base);
    ASSERT_GTE(base, (uint64_t)CX_BINDING_LINES);
    ASSERT_TRUE(ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI);
    PASS();
}
TEST(complexity_dart_usages_on_unclosed_nesting_is_linear) {
    /* The walk's own cursor answers, so nothing need be counted at all. */
    uint64_t base = 0;
    (void)cx_extract_ratio(CBM_LANG_DART, "unclosed.dart", "", cx_line_dart_unclosed, "", false,
                           CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS, &base);
    uint64_t twice =
        cx_extract_work(CBM_LANG_DART, "unclosed.dart", "", cx_line_dart_unclosed, "",
                        CX_BINDING_LINES * 2, false, CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS);
    ASSERT_TRUE((double)twice <= CX_RATIO_HI * (double)(base + CX_BINDING_LINES));
    PASS();
}
/* The same declarations closed: now the depth is real. Each identifier's usage
 * classifiers climb towards the root, and Kotlin's grammar gives them little to stop at. */
enum { CX_NEST_DEPTH = 400 };
static uint64_t cx_usage_climb_work(int depth) {
    static const char open[] = "object A {\n", close[] = "}\n";
    size_t room = (size_t)depth * (sizeof(open) + sizeof(close)) + 1;
    char *source = malloc(room);
    if (!source)
        return 0;
    size_t length = 0;
    for (int i = 0; i < depth; i++)
        length += (size_t)snprintf(source + length, room - length, "%s", open);
    for (int i = 0; i < depth; i++)
        length += (size_t)snprintf(source + length, room - length, "%s", close);
    cbm_usage_field_lookup_test_reset();
    CBMFileResult *result =
        cbm_extract_file(source, (int)length, CBM_LANG_KOTLIN, "test", "nest.kt", 0, NULL, NULL);
    uint64_t work = cbm_usage_field_lookup_test_work();
    if (result)
        cbm_free_result(result);
    free(source);
    return work;
}
TEST(complexity_usage_classifiers_on_deep_nesting_are_linear) {
    uint64_t first = cx_usage_climb_work(CX_NEST_DEPTH);
    uint64_t second = cx_usage_climb_work(CX_NEST_DEPTH * 2);
    double ratio = cx_ratio((double)second, (double)first);
    printf("    nest.kt usage climb %llu -> %llu (ratio %.2f)\n", (unsigned long long)first,
           (unsigned long long)second, ratio);
    ASSERT_GTE(first, (uint64_t)CX_NEST_DEPTH);
    ASSERT_TRUE(ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI);
    PASS();
}
/* A definition's decorators start from the sibling before it. */
static const char *cx_line_kotlin_object(char *out, size_t cap, int i) {
    snprintf(out, cap, "object A_%d { }\n", i);
    return out;
}
TEST(complexity_decorator_lookup_is_linear) {
    uint64_t base = 0;
    double ratio = cx_extract_ratio(CBM_LANG_KOTLIN, "objects.kt", "", cx_line_kotlin_object, "",
                                    false, CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS, &base);
    ASSERT_GTE(base, (uint64_t)CX_BINDING_LINES);
    ASSERT_TRUE(ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI);
    PASS();
}
TEST(complexity_ts_proof_narrowing_lookup_is_linear) {
    static const char head[] = "class C { m() {} }\n";
    uint64_t first = cx_extract_lsp_steps(CBM_LANG_TYPESCRIPT, "narrow.ts", head,
                                          cx_line_ts_narrowing, "", CX_BINDING_LINES);
    uint64_t second = cx_extract_lsp_steps(CBM_LANG_TYPESCRIPT, "narrow.ts", head,
                                           cx_line_ts_narrowing, "", CX_BINDING_LINES * 2);
    double ratio = cx_ratio((double)second, (double)first);
    printf("    narrow.ts lsp steps %llu -> %llu (ratio %.2f)\n", (unsigned long long)first,
           (unsigned long long)second, ratio);
    ASSERT_GTE(first, (uint64_t)CX_BINDING_LINES);
    ASSERT_TRUE(ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI);
    PASS();
}
TEST(complexity_ts_class_registration_is_linear) {
    ASSERT_TRUE(cx_pass_steps_are_linear(CBM_LANG_TYPESCRIPT, "classes.ts", "", cx_line_ts_class));
    PASS();
}
static const char *cx_line_rust_derive(char *out, size_t cap, int i) {
    snprintf(out, cap, "#[derive(Clone, Debug)]\npub struct S%d { pub a: i32 }\n", i);
    return out;
}
TEST(complexity_rust_derive_registration_is_linear) {
    ASSERT_TRUE(cx_pass_steps_are_linear(CBM_LANG_RUST, "derive.rs", "", cx_line_rust_derive));
    PASS();
}
static const char *cx_line_go_generic(char *out, size_t cap, int i) {
    snprintf(out, cap, "func F%d[T any](a T) T { return a }\n", i);
    return out;
}
TEST(complexity_go_generic_registration_is_linear) {
    ASSERT_TRUE(
        cx_pass_steps_are_linear(CBM_LANG_GO, "generic.go", "package p\n", cx_line_go_generic));
    PASS();
}
/* Each C# class looked through the file's top level for a file-scoped namespace. */
TEST(complexity_csharp_namespace_lookup_is_linear) {
    uint64_t base = 0;
    double ratio = cx_extract_ratio(CBM_LANG_CSHARP, "classes.cs", "", cx_line_csharp_class, "",
                                    false, CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS, &base);
    ASSERT_GTE(base, (uint64_t)CX_BINDING_LINES);
    ASSERT_TRUE(ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI);
    PASS();
}
/* The tail index must answer what the scan answers, duplicates and later indexes included. */
static int cx_type_row(const CBMTypeRegistry *reg, const char *name) {
    const CBMRegisteredType *found = cbm_registry_lookup_type(reg, name);
    return found ? (int)(found - reg->types) : -1;
}
static int cx_func_row(const CBMTypeRegistry *reg, const char *name) {
    const CBMRegisteredFunc *found = cbm_registry_lookup_func(reg, name);
    return found ? (int)(found - reg->funcs) : -1;
}
TEST(complexity_registry_tail_answers_as_a_scan_does) {
    enum { ROWS = 600, NAMES = 250, INDEX_AT = 200, PROBE_STEP = 37 };
    CBMArena arena, tail_arena, idx_arena;
    cbm_arena_init(&arena);
    cbm_arena_init(&tail_arena);
    cbm_arena_init(&idx_arena);
    CBMTypeRegistry with, without;
    cbm_registry_init(&with, &arena);
    cbm_registry_init(&without, &arena);
    with.tail_arena = &tail_arena;
    bool memo_used = false;
    for (int i = 0; i < ROWS; i++) {
        char name[CX_BINDING_LINE_ROOM];
        snprintf(name, sizeof(name), "t%d", i % NAMES);
        const char *qn = cbm_arena_strdup(&arena, name);
        CBMRegisteredType type = {.qualified_name = qn, .short_name = qn};
        CBMRegisteredFunc func = {.qualified_name = qn, .short_name = qn};
        cbm_registry_add_type(&with, type);
        cbm_registry_add_type(&without, type);
        cbm_registry_add_func(&with, func);
        cbm_registry_add_func(&without, func);
        if (i == INDEX_AT) {
            cbm_registry_finalize_into(&with, &idx_arena);
            cbm_registry_finalize_into(&without, &idx_arena);
        }
        memo_used = memo_used || with.type_tail.synced > with.type_tail.start;
        for (int probe = 0; probe < NAMES + PROBE_STEP; probe += PROBE_STEP) {
            char ask[CX_BINDING_LINE_ROOM];
            snprintf(ask, sizeof(ask), "t%d", probe);
            ASSERT_EQ(cx_type_row(&with, ask), cx_type_row(&without, ask));
            ASSERT_EQ(cx_func_row(&with, ask), cx_func_row(&without, ask));
            const CBMRegisteredType *own_with = cbm_registry_first_own_type(&with, ask);
            const CBMRegisteredType *own_without = cbm_registry_first_own_type(&without, ask);
            ASSERT_EQ(own_with ? (int)(own_with - with.types) : -1,
                      own_without ? (int)(own_without - without.types) : -1);
        }
    }
    ASSERT_TRUE(memo_used);
    cbm_arena_destroy(&idx_arena);
    cbm_arena_destroy(&tail_arena);
    cbm_arena_destroy(&arena);
    PASS();
}
TEST(complexity_kotlin_pass_registry_lookups_are_linear) {
    ASSERT_TRUE(cx_pass_steps_are_linear(CBM_LANG_KOTLIN, "pairs.kt", "package p\n",
                                         cx_line_kotlin_function_pair));
    PASS();
}

/* Each call in a body asks whether its name is a local, and the answer walked them all. */
static const char *cx_line_rust_let(char *out, size_t cap, int i) {
    snprintf(out, cap, "    let v_%d = f(1);\n", i);
    return out;
}
static const char *cx_line_rust_inline_mod(char *out, size_t cap, int i) {
    snprintf(out, cap, "pub mod m_%d { pub fn f() -> i32 { 1 } }\nfn g_%d() -> i32 { m_%d::f() }\n",
             i, i, i);
    return out;
}
static uint64_t cx_rust_flow_probes(const char *head, CxLineFn line, const char *tail, int lines) {
    static const char manifest[] = "[package]\nname = \"p\"\nversion = \"0.1.0\"\n";
    size_t room = strlen(head) + strlen(tail) + (size_t)lines * CX_BINDING_LINE_ROOM + 1;
    char *source = malloc(room);
    if (!source)
        return 0;
    size_t length = (size_t)snprintf(source, room, "%s", head);
    for (int i = 0; i < lines; i++) {
        char row[CX_BINDING_LINE_ROOM];
        length += (size_t)snprintf(source + length, room - length, "%s", line(row, sizeof(row), i));
    }
    length += (size_t)snprintf(source + length, room - length, "%s", tail);
    CBMFileResult *result =
        cbm_extract_file(source, (int)length, CBM_LANG_RUST, "p", "src/lib.rs", 0, NULL, NULL);
    TSParser *parser = ts_parser_new();
    TSTree *tree = NULL;
    if (parser && ts_parser_set_language(parser, cbm_ts_language(CBM_LANG_RUST)))
        tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)length);
    if (parser)
        ts_parser_delete(parser);
    cbm_rust_project_t *project = cbm_rust_project_new();
    uint64_t work = 0;
    if (result && tree && project &&
        cbm_rust_project_add_manifest(project, "Cargo.toml", manifest, strlen(manifest)) &&
        cbm_rust_project_add_file(project, "src/lib.rs", result, source, length,
                                  ts_tree_root_node(tree)) &&
        cbm_rust_project_seal(project)) {
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_stats_t stats;
        cbm_resolve_stats_reset(&stats);
        cbm_resolve_stats_t *previous = cbm_resolve_stats_bind(&stats);
        cbm_rust_file_analysis_t *analysis = cbm_rust_analyze_file(
            project, &arena, "src/lib.rs", source, length, ts_tree_root_node(tree));
        cbm_resolve_stats_bind(previous);
        if (analysis)
            work = cbm_resolve_stats_snapshot(&stats).values[CBM_RESOLVE_STAT_LOOKUP_PROBES];
        cbm_arena_destroy(&arena);
    }
    cbm_rust_project_free(project);
    if (tree)
        ts_tree_delete(tree);
    if (result)
        cbm_free_result(result);
    free(source);
    return work;
}
static bool cx_rust_flow_is_linear(const char *what, const char *head, CxLineFn line,
                                   const char *tail) {
    uint64_t first = cx_rust_flow_probes(head, line, tail, CX_BINDING_LINES);
    uint64_t second = cx_rust_flow_probes(head, line, tail, CX_BINDING_LINES * 2);
    double ratio = cx_ratio((double)second, (double)first);
    printf("    rust %s probes %llu -> %llu (ratio %.2f)\n", what, (unsigned long long)first,
           (unsigned long long)second, ratio);
    return first >= CX_BINDING_LINES && ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI;
}
TEST(complexity_rust_local_lookup_is_linear) {
    ASSERT_TRUE(cx_rust_flow_is_linear("locals", "fn f(a: i32) -> i32 { a }\nfn big() {\n",
                                       cx_line_rust_let, "}\n"));
    PASS();
}
/* A crate root's modules were a list, walked per path and per inline module. */
/* Every method on a std type looked through all the project's impls on outside types. */
static const char *cx_line_rust_ext_impl(char *out, size_t cap, int i) {
    snprintf(out, cap,
             "trait T%d{fn m%d(&self);}\nimpl T%d for String{fn m%d(&self){}}\n"
             "fn g%d(s:String){s.m%d();}\n",
             i, i, i, i, i, i);
    return out;
}
TEST(complexity_rust_outside_impls_are_linear) {
    ASSERT_TRUE(cx_rust_flow_is_linear("outside impls", "", cx_line_rust_ext_impl, ""));
    PASS();
}
/* Each impl walked looked itself up in its module's list. */
static const char *cx_line_rust_impl(char *out, size_t cap, int i) {
    snprintf(out, cap, "pub struct S%d;\nimpl S%d { pub fn m(&self) -> i32 { 1 } }\n", i, i);
    return out;
}
TEST(complexity_rust_impl_lookup_is_linear) {
    ASSERT_TRUE(cx_rust_flow_is_linear("impls", "", cx_line_rust_impl, ""));
    PASS();
}
/* A module's uses, walked for each local item found (a conditional use of its name), for each
 * imported name, and for each trait a method call asks about. */
static const char *cx_line_rust_use_and_fn(char *out, size_t cap, int i) {
    snprintf(out, cap, "use a::b%d;\nfn f%d() { f%d(); }\n", i, i, i);
    return out;
}
static const char *cx_line_rust_use_and_call(char *out, size_t cap, int i) {
    snprintf(out, cap, "use a::b%d;\nfn f%d() { b%d(); }\n", i, i, i);
    return out;
}
static const char *cx_line_rust_use_of_trait(char *out, size_t cap, int i) {
    snprintf(out, cap,
             "mod b%d{pub trait T%d{fn m%d(&self);}impl T%d for u8{fn m%d(&self){}}}\n"
             "use b%d::T%d;\nfn f%d(s:u8){s.m%d();}\n",
             i, i, i, i, i, i, i, i, i);
    return out;
}
TEST(complexity_rust_conditional_use_lookup_is_linear) {
    ASSERT_TRUE(cx_rust_flow_is_linear("uses beside items", "", cx_line_rust_use_and_fn, ""));
    PASS();
}
TEST(complexity_rust_named_use_lookup_is_linear) {
    ASSERT_TRUE(cx_rust_flow_is_linear("uses named", "", cx_line_rust_use_and_call, ""));
    PASS();
}
TEST(complexity_rust_trait_in_scope_is_linear) {
    ASSERT_TRUE(cx_rust_flow_is_linear("uses of traits", "", cx_line_rust_use_of_trait, ""));
    PASS();
}
TEST(complexity_rust_module_children_are_linear) {
    ASSERT_TRUE(cx_rust_flow_is_linear("children", "", cx_line_rust_inline_mod, ""));
    PASS();
}

/* Imports, then a use of each: every use used to scan them. */
typedef struct {
    CBMLanguage language;
    const char *path, *head, *imported, *middle, *used, *tail;
} CxImportShape;
static const CxImportShape CX_IMPORT_SHAPES[] = {
    {CBM_LANG_JAVASCRIPT, "uses.js", "", "import { n_%d } from './m_%d.js';\n", "", "n_%d();\n",
     ""},
    {CBM_LANG_TYPESCRIPT, "uses.ts", "", "import { n_%d } from './m_%d';\n", "", "n_%d();\n", ""},
    {CBM_LANG_JAVA, "Uses.java", "", "import p.q%d.R%d;\n", "public class Uses { void f() {\n",
     "  R%d.call();\n", "} }\n"},
    {CBM_LANG_KOTLIN, "uses.kt", "package p\n", "import q.r%d.S%d\n", "fun f() {\n",
     "  S%d.call()\n", "}\n"},
    {CBM_LANG_PHP, "uses.php", "<?php\nnamespace App;\n", "use Lib\\A%d;\n", "function f() {\n",
     "  A%d::go();\n", "}\n"},
    {CBM_LANG_RUST, "uses.rs", "", "use crate::m%d::f%d;\n", "fn run() {\n", "    f%d();\n", "}\n"},
    {CBM_LANG_GO, "uses.go", "package p\nimport (\n", "\tq%d \"example.com/q%d\"\n",
     ")\nfunc F() {\n", "\tq%d.G()\n", "}\n"},
};
static uint64_t cx_import_shape_steps(const CxImportShape *shape, int lines) {
    size_t room = (size_t)lines * CX_BINDING_LINE_ROOM * 2 + CX_BINDING_LINE_ROOM * 2;
    char *source = malloc(room);
    if (!source)
        return 0;
    size_t length = (size_t)snprintf(source, room, "%s", shape->head);
    for (int i = 0; i < lines; i++)
        length += (size_t)snprintf(source + length, room - length, shape->imported, i, i);
    length += (size_t)snprintf(source + length, room - length, "%s", shape->middle);
    for (int i = 0; i < lines; i++)
        length += (size_t)snprintf(source + length, room - length, shape->used, i, i);
    length += (size_t)snprintf(source + length, room - length, "%s", shape->tail);
    (void)cbm_lsp_work_take();
    CBMFileResult *result =
        cbm_extract_file(source, (int)length, shape->language, "test", shape->path, 0, NULL, NULL);
    uint64_t work = cbm_lsp_work_take();
    if (result)
        cbm_free_result(result);
    free(source);
    return work;
}
TEST(complexity_import_uses_are_linear_in_every_language_pass) {
    bool all = true;
    for (size_t i = 0; i < sizeof(CX_IMPORT_SHAPES) / sizeof(CX_IMPORT_SHAPES[0]); i++) {
        const CxImportShape *shape = &CX_IMPORT_SHAPES[i];
        uint64_t first = cx_import_shape_steps(shape, CX_BINDING_LINES);
        uint64_t second = cx_import_shape_steps(shape, CX_BINDING_LINES * 2);
        double ratio = cx_ratio((double)second, (double)first);
        printf("    %s lsp steps %llu -> %llu (ratio %.2f)\n", shape->path,
               (unsigned long long)first, (unsigned long long)second, ratio);
        all = all && first >= CX_BINDING_LINES && ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI;
    }
    ASSERT_TRUE(all);
    PASS();
}

/* Which `import a.b` rows carry their root's fact. A root's first dotted row does, unless a
 * row imports that root plainly. */
extern int cbm_resolver_test_python_root_facts(const CBMFileResult *result);
static const char *cx_line_py_root_import(char *out, size_t cap, int i) {
    snprintf(out, cap, "import pkg_%d.part\n", i);
    return out;
}
static uint64_t cx_python_root_probes(const char *source, int *facts) {
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "test",
                                             "roots.py", 0, NULL, NULL);
    if (!result)
        return 0;
    cbm_resolve_stats_t stats;
    cbm_resolve_stats_reset(&stats);
    cbm_resolve_stats_t *previous = cbm_resolve_stats_bind(&stats);
    *facts = cbm_resolver_test_python_root_facts(result);
    cbm_resolve_stats_bind(previous);
    cbm_free_result(result);
    return cbm_resolve_stats_snapshot(&stats).values[CBM_RESOLVE_STAT_LOOKUP_PROBES];
}
static uint64_t cx_python_root_shape(int lines, int *facts) {
    size_t room = (size_t)lines * CX_BINDING_LINE_ROOM + 1;
    char *source = malloc(room);
    if (!source)
        return 0;
    size_t length = 0;
    source[0] = '\0';
    for (int i = 0; i < lines; i++) {
        char row[CX_BINDING_LINE_ROOM];
        length += (size_t)snprintf(source + length, room - length, "%s",
                                   cx_line_py_root_import(row, sizeof(row), i));
    }
    uint64_t work = cx_python_root_probes(source, facts);
    free(source);
    return work;
}
TEST(complexity_python_root_facts_are_linear) {
    int first_facts = 0, second_facts = 0;
    uint64_t first = cx_python_root_shape(CX_BINDING_LINES, &first_facts);
    uint64_t second = cx_python_root_shape(CX_BINDING_LINES * 2, &second_facts);
    double ratio = cx_ratio((double)second, (double)first);
    printf("    python root facts probes %llu -> %llu (ratio %.2f)\n", (unsigned long long)first,
           (unsigned long long)second, ratio);
    ASSERT_EQ(first_facts, CX_BINDING_LINES);
    ASSERT_EQ(second_facts, CX_BINDING_LINES * 2);
    ASSERT_GTE(first, (uint64_t)CX_BINDING_LINES);
    ASSERT_TRUE(ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI);
    /* The rule, row by row: only a's first row counts, and b has a plain row. */
    int facts = -1;
    (void)cx_python_root_probes("import a.x\nimport a.y\nimport b.x\nimport b\nimport c.d.e\n",
                                &facts);
    ASSERT_EQ(facts, 2);
    (void)cx_python_root_probes("import b\nimport b.x\n", &facts);
    ASSERT_EQ(facts, 0);
    PASS();
}

/* Python's builtins are declared by every Python file, so each new file asked whether it
 * was on the list of each of them, and each call to a builtin could ask again. */
extern bool cbm_resolver_test_target_files(int count);
static uint64_t cx_target_file_probes(int count, bool *answers) {
    cbm_resolve_stats_t stats;
    cbm_resolve_stats_reset(&stats);
    cbm_resolve_stats_t *previous = cbm_resolve_stats_bind(&stats);
    *answers = cbm_resolver_test_target_files(count);
    cbm_resolve_stats_bind(previous);
    return cbm_resolve_stats_snapshot(&stats).values[CBM_RESOLVE_STAT_LOOKUP_PROBES];
}
TEST(complexity_target_file_list_is_near_linear) {
    bool first_ok = false, second_ok = false;
    uint64_t first = cx_target_file_probes(CX_BINDING_LINES, &first_ok);
    uint64_t second = cx_target_file_probes(CX_BINDING_LINES * 2, &second_ok);
    double ratio = cx_ratio((double)second, (double)first);
    printf("    target file probes %llu -> %llu (ratio %.2f)\n", (unsigned long long)first,
           (unsigned long long)second, ratio);
    ASSERT_TRUE(first_ok);
    ASSERT_TRUE(second_ok);
    ASSERT_GTE(first, (uint64_t)CX_BINDING_LINES);
    ASSERT_TRUE(ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI);
    /* Short lists are walked, and answer the same. */
    uint64_t small_probes = cx_target_file_probes(TARGET_FILES_SHORT, &first_ok);
    ASSERT_TRUE(first_ok);
    ASSERT_GTE(small_probes, (uint64_t)TARGET_FILES_SHORT);
    PASS();
}

/* The table says the same whichever way a name is found. */
TEST(complexity_name_rows_answer_as_a_scan_does) {
    enum { COUNT = 64 };
    const char *names[COUNT];
    char storage[COUNT][8];
    for (int i = 0; i < COUNT; i++) {
        snprintf(storage[i], sizeof(storage[i]), "n%d", i % 7);
        names[i] = i % 11 == 5 ? NULL : storage[i];
    }
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMNameRows rows = {0};
    bool same = true;
    /* Both short of the threshold and past it; the table may grow between questions. */
    for (int count = 1; count <= COUNT; count++) {
        for (int v = 0; v < 8; v++) {
            char name[8];
            snprintf(name, sizeof(name), "n%d", v);
            int expected = -1;
            for (int i = 0; i < count && expected < 0; i++)
                if (names[i] && !strcmp(names[i], name))
                    expected = i;
            int got = cbm_name_rows_first(&rows, &arena, names, count, name);
            while (same && expected >= 0) {
                same = got == expected;
                int after = -1;
                for (int i = expected + 1; i < count && after < 0; i++)
                    if (names[i] && !strcmp(names[i], name))
                        after = i;
                got = cbm_name_rows_next(&rows, names, count, name, got);
                expected = after;
            }
            same = same && got == -1;
        }
    }
    cbm_arena_destroy(&arena);
    ASSERT_TRUE(same);
    PASS();
}

/* A definition's parent comes through a short list of cached children; the wide root has to
 * stay on that list while its narrow children come and go. */
static const char *cx_line_erlang_function(char *out, size_t cap, int i) {
    snprintf(out, cap, "%% doc %d\nf_%d(A) -> g_%d(A).\ng_%d(B) -> B.\n", i, i, i, i);
    return out;
}
TEST(complexity_doc_lookup_keeps_a_wide_root_cached) {
    uint64_t base = 0;
    double ratio =
        cx_extract_ratio(CBM_LANG_ERLANG, "funcs.erl", "-module(m).\n", cx_line_erlang_function, "",
                         false, CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS, &base);
    ASSERT_GTE(base, (uint64_t)CX_BINDING_LINES);
    ASSERT_TRUE(ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI);
    PASS();
}

/* Each struct's fields fold into one text, and each struct used to scan the file's defs. */
static const char *cx_line_go_struct(char *out, size_t cap, int i) {
    snprintf(out, cap, "type T%d struct {\n\tA int\n\tB string\n}\n", i);
    return out;
}
static uint64_t cx_field_fold_probes(int lines, int *folded) {
    size_t room = (size_t)lines * CX_BINDING_LINE_ROOM + CX_BINDING_LINE_ROOM;
    char *source = malloc(room);
    if (!source)
        return 0;
    size_t length = (size_t)snprintf(source, room, "package p\n");
    for (int i = 0; i < lines; i++) {
        char row[CX_BINDING_LINE_ROOM];
        length += (size_t)snprintf(source + length, room - length, "%s",
                                   cx_line_go_struct(row, sizeof(row), i));
    }
    CBMFileResult *result =
        cbm_extract_file(source, (int)length, CBM_LANG_GO, "p", "a.go", 0, NULL, NULL);
    uint64_t work = 0;
    if (result) {
        CBMFileResult *cache[] = {result};
        cbm_file_info_t files[] = {{.rel_path = "a.go", .language = CBM_LANG_GO}};
        char *modules[1] = {NULL};
        int starts[2] = {0}, count = 0;
        const char *saved_source = result->source;
        int saved_length = result->source_len;
        result->source = source;
        result->source_len = (int)length;
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_stats_t stats;
        cbm_resolve_stats_reset(&stats);
        cbm_resolve_stats_t *previous = cbm_resolve_stats_bind(&stats);
        CBMLSPDef *defs =
            cbm_pxc_collect_all_defs(NULL, &arena, cache, files, 1, "p", modules, &count, starts);
        cbm_resolve_stats_bind(previous);
        work = cbm_resolve_stats_snapshot(&stats).values[CBM_RESOLVE_STAT_LOOKUP_PROBES];
        *folded = 0;
        for (int i = 0; defs && i < count; i++) {
            if (defs[i].label && !strcmp(defs[i].label, "Struct") && defs[i].field_defs &&
                !strcmp(defs[i].field_defs, "A:int|B:string")) {
                (*folded)++;
            }
        }
        cbm_arena_destroy(&arena);
        result->source = saved_source;
        result->source_len = saved_length;
        cbm_free_result(result);
    }
    free(source);
    return work;
}
TEST(complexity_struct_field_fold_is_linear) {
    int folded_first = 0, folded_second = 0;
    uint64_t first = cx_field_fold_probes(CX_BINDING_LINES, &folded_first);
    uint64_t second = cx_field_fold_probes(CX_BINDING_LINES * 2, &folded_second);
    double ratio = cx_ratio((double)second, (double)first);
    printf("    struct field fold probes %llu -> %llu (ratio %.2f)\n", (unsigned long long)first,
           (unsigned long long)second, ratio);
    ASSERT_EQ(folded_first, CX_BINDING_LINES);
    ASSERT_EQ(folded_second, CX_BINDING_LINES * 2);
    ASSERT_GTE(first, (uint64_t)CX_BINDING_LINES);
    ASSERT_TRUE(ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI);
    PASS();
}

/* A leading doc comment is the child before the definition; it used to be found by
 * counting up to the definition from the parent's first child. */
static const char *cx_line_php_function(char *out, size_t cap, int i) {
    snprintf(out, cap, "function f_%d() { return 1; }\n", i);
    return out;
}
static const char *cx_line_php_method(char *out, size_t cap, int i) {
    snprintf(out, cap, "  public function m_%d() { return 1; }\n", i);
    return out;
}
static bool cx_php_doc_shape_binds(const char *path, const char *head, CxLineFn line,
                                   const char *tail) {
    uint64_t base = 0;
    double plain = cx_extract_ratio(CBM_LANG_PHP, path, head, line, tail, true,
                                    CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS, &base);
    if (base < CX_BINDING_LINES || plain < 3.2)
        return false;
    double indexed = cx_extract_ratio(CBM_LANG_PHP, path, head, line, tail, false,
                                      CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS, &base);
    return base >= CX_BINDING_LINES && indexed >= CX_RATIO_LO && indexed <= CX_RATIO_HI;
}
/* A call to a name nothing declares looked through every registered function. */
static const char *cx_line_php_unknown_call(char *out, size_t cap, int i) {
    snprintf(out, cap, "function f_%d() { return g_%d(); }\n", i, i);
    return out;
}
TEST(complexity_php_short_name_fallback_is_linear) {
    ASSERT_TRUE(
        cx_pass_steps_are_linear(CBM_LANG_PHP, "calls.php", "<?php\n", cx_line_php_unknown_call));
    PASS();
}
TEST(complexity_php_doc_lookup_is_linear) {
    ASSERT_TRUE(cx_php_doc_shape_binds("flat.php", "<?php\n", cx_line_php_function, ""));
    ASSERT_TRUE(
        cx_php_doc_shape_binds("wide.php", "<?php\nclass C {\n", cx_line_php_method, "}\n"));
    PASS();
}

/* Python's own pass joined each import row to every import statement, each statement's
 * names to every row, and each reference to every row. */
static const char *cx_line_py_import_pair(char *out, size_t cap, int i) {
    snprintf(out, cap, "import pkg_%d.part\nfrom mod_%d import name_%d\nname_%d()\n", i, i, i, i);
    return out;
}
static const char *cx_line_py_from_item(char *out, size_t cap, int i) {
    snprintf(out, cap, "    name_%d,\n", i);
    return out;
}
TEST(complexity_python_pass_import_join_is_linear) {
    uint64_t base = 0;
    double ratio = cx_extract_ratio(CBM_LANG_PYTHON, "pairs.py", "", cx_line_py_import_pair, "",
                                    false, CBM_RESOLVE_STAT_LOOKUP_PROBES, &base);
    ASSERT_GTE(base, (uint64_t)CX_BINDING_LINES);
    ASSERT_TRUE(ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI);
    PASS();
}
/* One statement can hold them all: generated SDK packages re-export thousands of names. */
TEST(complexity_python_pass_wide_from_import_is_linear) {
    uint64_t base = 0;
    double ratio =
        cx_extract_ratio(CBM_LANG_PYTHON, "reexport.py", "from .models import (\n",
                         cx_line_py_from_item, ")\n", false, CBM_RESOLVE_STAT_LOOKUP_PROBES, &base);
    ASSERT_GTE(base, (uint64_t)CX_BINDING_LINES);
    ASSERT_TRUE(ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI);
    PASS();
}

/* ── Faster routes store the same thing ────────────────────────────── */

extern bool cbm_pipeline_test_resolver_stage_off;
extern bool cbm_pipeline_test_prepare_on_one_thread;
extern _Atomic size_t cbm_binding_cache_test_hits;
extern _Atomic size_t cbm_parse_test_trees_shared;

/* Everything the views say, except each row's ordinal: that follows worker scheduling. */
static bool cx_evidence_hash(const char *db_path, unsigned char digest[32], bool *intact) {
    static const char occurrences[] =
        "SELECT project,file_path,scope,relation,spelling,start_byte,end_byte,source_origin,"
        "binding,domain,analysis,rule,facts,target,candidate_count,reason,emitted,rules_version,"
        "properties_json FROM resolver_occurrences ORDER BY "
        "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19";
    static const char edges[] =
        "SELECT s.qualified_name,e.type,t.qualified_name,e.properties FROM edges e "
        "JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id ORDER BY 1,2,3,4";
    const char *const queries[] = {
        occurrences,
        "SELECT project,id,kind,source_file,digest,payload FROM resolver_facts ORDER BY project,id",
        "SELECT project,fact,ordinal,premise FROM resolver_supports ORDER BY project,fact,ordinal",
        "SELECT project,fact,domain,key,status FROM resolver_lookups ORDER BY project,fact",
        "SELECT * FROM resolver_file_summaries ORDER BY project,file_path",
        "SELECT * FROM resolver_derived_bindings ORDER BY project,file_path,key,kind",
        edges,
        "SELECT name,sql FROM sqlite_master WHERE name LIKE 'resolver_%' ORDER BY name"};
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return false;
    }
    cbm_sha256_ctx hash;
    cbm_sha256_init(&hash);
    uint64_t rows = 0;
    bool ok = true;
    for (size_t q = 0; ok && q < sizeof(queries) / sizeof(queries[0]); q++) {
        sqlite3_stmt *s = NULL;
        ok = sqlite3_prepare_v2(db, queries[q], -1, &s, NULL) == SQLITE_OK;
        int step = SQLITE_DONE;
        while (ok && (step = sqlite3_step(s)) == SQLITE_ROW) {
            rows++;
            for (int c = 0; c < sqlite3_column_count(s); c++) {
                const void *bytes = sqlite3_column_blob(s, c);
                int length = sqlite3_column_bytes(s, c);
                int type = sqlite3_column_type(s, c);
                cbm_sha256_update(&hash, &type, sizeof(type));
                cbm_sha256_update(&hash, &length, sizeof(length));
                if (bytes && length)
                    cbm_sha256_update(&hash, bytes, (size_t)length);
            }
        }
        ok = ok && step == SQLITE_DONE;
        sqlite3_finalize(s);
    }
    sqlite3_stmt *check = NULL;
    *intact = sqlite3_prepare_v2(db, "PRAGMA integrity_check(1)", -1, &check, NULL) == SQLITE_OK &&
              sqlite3_step(check) == SQLITE_ROW &&
              !strcmp((const char *)sqlite3_column_text(check, 0), "ok");
    sqlite3_finalize(check);
    sqlite3_close(db);
    cbm_sha256_final(&hash, digest);
    return ok && rows > 0;
}

static bool cx_index_and_hash(const char *root, const char *name, unsigned char digest[32],
                              bool *intact) {
    char db_path[600];
    int n = snprintf(db_path, sizeof(db_path), "%s/%s", root, name);
    if (n < 0 || (size_t)n >= sizeof(db_path))
        return false;
    cbm_pipeline_t *p = cbm_pipeline_new(root, db_path, CBM_MODE_FULL);
    if (!p)
        return false;
    int rc = cbm_pipeline_run(p);
    cbm_pipeline_free(p);
    return rc == 0 && cx_evidence_hash(db_path, digest, intact);
}

/* Evidence written straight into a new file's pages reads back exactly as SQL's does,
 * with the same schema text, and SQLite finds nothing wrong with the file. */
TEST(complexity_evidence_page_route_stores_what_sql_stores) {
    char root[512];
    snprintf(root, sizeof(root), "%s/cbm_cxroute_XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(root));
    ASSERT_EQ(cx_build_corpus(root, CX_K_BASE), 0);
    unsigned char direct[32], sql[32];
    bool direct_intact = false, sql_intact = false;
    bool direct_ok = cx_index_and_hash(root, "direct.db", direct, &direct_intact);
    cbm_pipeline_test_resolver_stage_off = true;
    bool sql_ok = cx_index_and_hash(root, "sql.db", sql, &sql_intact);
    cbm_pipeline_test_resolver_stage_off = false;
    ASSERT_EQ(cx_remove_corpus(root), 0);
    ASSERT_TRUE(direct_ok);
    ASSERT_TRUE(sql_ok);
    ASSERT_TRUE(direct_intact);
    ASSERT_TRUE(sql_intact);
    ASSERT_EQ(memcmp(direct, sql, sizeof(direct)), 0);
    PASS();
}

/* The shared indexes and parse batches change how long preparation takes and nothing it
 * decides. The counters show they really were used on the first run. */
TEST(complexity_shared_preparation_decides_the_same) {
    char root[512];
    snprintf(root, sizeof(root), "%s/cbm_cxprep_XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(root));
    ASSERT_EQ(cx_build_corpus(root, CX_K_BASE), 0);
    unsigned char shared[32], alone[32];
    bool shared_intact = false, alone_intact = false;
    size_t hits_before = atomic_load(&cbm_binding_cache_test_hits);
    size_t trees_before = atomic_load(&cbm_parse_test_trees_shared);
    bool shared_ok = cx_index_and_hash(root, "shared.db", shared, &shared_intact);
    size_t hits = atomic_load(&cbm_binding_cache_test_hits) - hits_before;
    size_t trees = atomic_load(&cbm_parse_test_trees_shared) - trees_before;
    cbm_pipeline_test_prepare_on_one_thread = true;
    hits_before = atomic_load(&cbm_binding_cache_test_hits);
    trees_before = atomic_load(&cbm_parse_test_trees_shared);
    bool alone_ok = cx_index_and_hash(root, "alone.db", alone, &alone_intact);
    size_t hits_alone = atomic_load(&cbm_binding_cache_test_hits) - hits_before;
    size_t trees_alone = atomic_load(&cbm_parse_test_trees_shared) - trees_before;
    cbm_pipeline_test_prepare_on_one_thread = false;
    ASSERT_EQ(cx_remove_corpus(root), 0);
    ASSERT_TRUE(shared_ok);
    ASSERT_TRUE(alone_ok);
    ASSERT_TRUE(shared_intact);
    ASSERT_GT(hits, (size_t)0);
    ASSERT_GT(trees, (size_t)0);
    ASSERT_EQ(hits_alone, (size_t)0);
    /* Workers still share a tree between their passes over one file; preparation does not. */
    ASSERT_LT(trees_alone, trees);
    ASSERT_EQ(memcmp(shared, alone, sizeof(shared)), 0);
    PASS();
}

/* A bound tree answers only the exact parse it stands for. */
TEST(complexity_parse_bind_answers_only_the_same_parse) {
    static const char source[] = "x = 1\n";
    static const char other[] = "y = 1\n";
    static const char unterminated[] = "x = 1";
    TSParser *parser = ts_parser_new();
    ASSERT_NOT_NULL(parser);
    const TSLanguage *python = cbm_ts_language(CBM_LANG_PYTHON);
    ASSERT_TRUE(ts_parser_set_language(parser, python));
    uint32_t length = (uint32_t)strlen(source);
    TSTree *tree = ts_parser_parse_string(parser, NULL, source, length);
    ASSERT_NOT_NULL(tree);
    cbm_parse_bound_t bound = {.mode = CBM_PARSE_PLAIN,
                               .language = python,
                               .source = source,
                               .length = length,
                               .tree = tree};
    cbm_parse_bind(&bound);
    size_t before = atomic_load(&cbm_parse_test_trees_shared);
#define CX_SHARED() (atomic_load(&cbm_parse_test_trees_shared) - before)
    /* The same bytes elsewhere in memory, and either mode: a newline-terminated file. */
    char copy[sizeof(source)];
    memcpy(copy, source, sizeof(source));
    TSTree *same = cbm_parse_plain(parser, copy, length);
    ASSERT_EQ(CX_SHARED(), (size_t)1);
    ts_tree_delete(same);
    same = cbm_parse_source(parser, source, length, (TSParseOptions){0});
    ASSERT_EQ(CX_SHARED(), (size_t)2);
    ts_tree_delete(same);
    /* Different bytes of the same length, a different length, another grammar, the C mode. */
    same = cbm_parse_plain(parser, other, length);
    ASSERT_EQ(CX_SHARED(), (size_t)2);
    ts_tree_delete(same);
    same = cbm_parse_plain(parser, unterminated, (uint32_t)strlen(unterminated));
    ASSERT_EQ(CX_SHARED(), (size_t)2);
    ts_tree_delete(same);
    TSParser *go = ts_parser_new();
    ASSERT_TRUE(ts_parser_set_language(go, cbm_ts_language(CBM_LANG_GO)));
    same = cbm_parse_plain(go, source, length);
    ASSERT_EQ(CX_SHARED(), (size_t)2);
    ts_tree_delete(same);
    ts_parser_delete(go);
    /* A tree of the C route holds what that route decided, and only it gets that tree. */
    bound.mode = CBM_PARSE_C;
    same = cbm_parse_plain(parser, source, length);
    ASSERT_EQ(CX_SHARED(), (size_t)2);
    ts_tree_delete(same);
    bound.mode = CBM_PARSE_PLAIN;
    /* A parser reading part of the file does not get the whole file's tree. */
    TSRange part = {.start_byte = 0, .end_byte = 1, .start_point = {0, 0}, .end_point = {0, 1}};
    ASSERT_TRUE(ts_parser_set_included_ranges(parser, &part, 1));
    same = cbm_parse_plain(parser, source, length);
    ASSERT_EQ(CX_SHARED(), (size_t)2);
    ts_tree_delete(same);
    ASSERT_TRUE(ts_parser_set_included_ranges(parser, NULL, 0));
    /* An unterminated file: the two modes are different parses. */
    TSTree *plain =
        ts_parser_parse_string(parser, NULL, unterminated, (uint32_t)strlen(unterminated));
    bound = (cbm_parse_bound_t){.mode = CBM_PARSE_PLAIN,
                                .language = python,
                                .source = unterminated,
                                .length = (uint32_t)strlen(unterminated),
                                .tree = plain};
    same =
        cbm_parse_source(parser, unterminated, (uint32_t)strlen(unterminated), (TSParseOptions){0});
    ASSERT_EQ(CX_SHARED(), (size_t)2);
    ts_tree_delete(same);
    same = cbm_parse_plain(parser, unterminated, (uint32_t)strlen(unterminated));
    ASSERT_EQ(CX_SHARED(), (size_t)3);
    ts_tree_delete(same);
    cbm_parse_bind(NULL);
    same = cbm_parse_plain(parser, unterminated, (uint32_t)strlen(unterminated));
    ASSERT_EQ(CX_SHARED(), (size_t)3);
    ts_tree_delete(same);
#undef CX_SHARED
    ts_tree_delete(plain);
    ts_tree_delete(tree);
    ts_parser_delete(parser);
    PASS();
}

/* A file's import map: each import asks what path its name has and whether it is mapped
 * already. A page of generated HTML has thousands of them; so can a Kotlin file. */
static uint64_t cx_import_map_probes(CBMLanguage language, const char *path, CxLineFn line,
                                     int lines, int *mapped) {
    size_t room = (size_t)lines * CX_BINDING_LINE_ROOM + CX_BINDING_LINE_ROOM;
    char *source = malloc(room);
    if (!source)
        return 0;
    size_t length = 0;
    for (int i = 0; i < lines; i++) {
        char row[CX_BINDING_LINE_ROOM];
        length += (size_t)snprintf(source + length, room - length, "%s", line(row, sizeof(row), i));
    }
    CBMFileResult *result =
        cbm_extract_file(source, (int)length, language, "test", path, 0, NULL, NULL);
    uint64_t work = 0;
    cbm_gbuf_t *gbuf = cbm_gbuf_new("test", "/tmp/test");
    if (result && gbuf) {
        cbm_pipeline_ctx_t ctx = {.project_name = "test", .gbuf = gbuf};
        cbm_resolve_stats_t stats;
        cbm_resolve_stats_reset(&stats);
        cbm_resolve_stats_t *previous = cbm_resolve_stats_bind(&stats);
        const char **keys = NULL, **values = NULL;
        int count = 0;
        int rc =
            cbm_pxc_build_import_map_ex(&ctx, path, language, result, true, &keys, &values, &count);
        cbm_resolve_stats_bind(previous);
        if (rc == 0)
            work = cbm_resolve_stats_snapshot(&stats).values[CBM_RESOLVE_STAT_LOOKUP_PROBES];
        *mapped = count;
        cbm_pxc_free_import_map(keys, values, count);
    }
    cbm_gbuf_free(gbuf);
    if (result)
        cbm_free_result(result);
    free(source);
    return work;
}
static const char *cx_line_kt_import(char *out, size_t cap, int i) {
    snprintf(out, cap, "import q.r_%d.S_%d\n", i, i);
    return out;
}
TEST(complexity_import_map_is_linear_in_a_files_imports) {
    int mapped_first = 0, mapped_second = 0;
    uint64_t first = cx_import_map_probes(CBM_LANG_KOTLIN, "imports.kt", cx_line_kt_import,
                                          CX_BINDING_LINES, &mapped_first);
    uint64_t second = cx_import_map_probes(CBM_LANG_KOTLIN, "imports.kt", cx_line_kt_import,
                                           CX_BINDING_LINES * 2, &mapped_second);
    double ratio = cx_ratio((double)second, (double)first);
    printf("    import map lookups %llu -> %llu (ratio %.2f)\n", (unsigned long long)first,
           (unsigned long long)second, ratio);
    ASSERT_EQ(mapped_first, CX_BINDING_LINES);
    ASSERT_EQ(mapped_second, CX_BINDING_LINES * 2);
    ASSERT_GTE(first, (uint64_t)CX_BINDING_LINES);
    ASSERT_TRUE(ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI);
    PASS();
}

/* A conflicting name blanks its edge entry and every later entry, and a repeated plain
 * name is mapped once: what the two scans did, in the order they did it. */
TEST(complexity_import_map_keeps_conflicts_and_repeats) {
    static const char source[] =
        "import a.b.S\nimport c.d.S\nimport e.f.T\nimport e.f.T\nimport g.h.U\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_KOTLIN, "test",
                                             "conflicts.kt", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    cbm_gbuf_t *gbuf = cbm_gbuf_new("test", "/tmp/test");
    ASSERT_NOT_NULL(gbuf);
    cbm_pipeline_ctx_t ctx = {.project_name = "test", .gbuf = gbuf};
    const char **keys = NULL, **values = NULL;
    int count = 0;
    ASSERT_EQ(cbm_pxc_build_import_map_ex(&ctx, "conflicts.kt", CBM_LANG_KOTLIN, result, true,
                                          &keys, &values, &count),
              0);
    ASSERT_EQ(count, 4);
    ASSERT_STR_EQ(keys[0], "S");
    ASSERT_STR_EQ(values[0], "");
    ASSERT_STR_EQ(keys[1], "S");
    ASSERT_STR_EQ(values[1], "");
    ASSERT_STR_EQ(keys[2], "T");
    ASSERT_STR_EQ(values[2], "e.f.T");
    ASSERT_STR_EQ(keys[3], "U");
    ASSERT_STR_EQ(values[3], "g.h.U");
    cbm_pxc_free_import_map(keys, values, count);
    cbm_gbuf_free(gbuf);
    cbm_free_result(result);
    PASS();
}

/* A worker's passes over one file share one parse, and a later file gets its own. */
TEST(complexity_parse_memo_lasts_one_file) {
    static const char first[] = "x = 1\n";
    static const char second[] = "yy = 2\n";
    TSParser *parser = ts_parser_new();
    ASSERT_NOT_NULL(parser);
    ASSERT_TRUE(ts_parser_set_language(parser, cbm_ts_language(CBM_LANG_PYTHON)));
    uint32_t first_length = (uint32_t)strlen(first), second_length = (uint32_t)strlen(second);
    size_t before = atomic_load(&cbm_parse_test_trees_shared);
#define CX_SHARED() (atomic_load(&cbm_parse_test_trees_shared) - before)
    /* Nothing is kept outside a memo. */
    TSTree *tree = cbm_parse_plain(parser, first, first_length);
    ts_tree_delete(tree);
    tree = cbm_parse_plain(parser, first, first_length);
    ts_tree_delete(tree);
    ASSERT_EQ(CX_SHARED(), (size_t)0);
    cbm_parse_memo_begin();
    tree = cbm_parse_source(parser, first, first_length, (TSParseOptions){0});
    ASSERT_EQ(CX_SHARED(), (size_t)0);
    /* The caller's tree and bytes may go away: the memo keeps its own. */
    char *moved = malloc(first_length);
    ASSERT_NOT_NULL(moved);
    memcpy(moved, first, first_length);
    ts_tree_delete(tree);
    tree = cbm_parse_plain(parser, moved, first_length);
    free(moved);
    ASSERT_EQ(CX_SHARED(), (size_t)1);
    ASSERT_STR_EQ(ts_node_type(ts_tree_root_node(tree)), "module");
    ts_tree_delete(tree);
    tree = cbm_parse_plain(parser, second, second_length);
    ASSERT_EQ(CX_SHARED(), (size_t)1);
    ts_tree_delete(tree);
    tree = cbm_parse_plain(parser, second, second_length);
    ASSERT_EQ(CX_SHARED(), (size_t)2);
    ts_tree_delete(tree);
    cbm_parse_memo_begin();
    tree = cbm_parse_plain(parser, first, first_length);
    ASSERT_EQ(CX_SHARED(), (size_t)2);
    ts_tree_delete(tree);
    cbm_parse_memo_end();
    tree = cbm_parse_plain(parser, first, first_length);
    ASSERT_EQ(CX_SHARED(), (size_t)2);
    ts_tree_delete(tree);
#undef CX_SHARED
    ts_parser_delete(parser);
    PASS();
}

/* ── A language adapter's two phases ────────────────────────────────
 * Over files already extracted, so what is counted is what the adapter itself walks: a
 * name's definitions in a file, a library's directives, a type's bases. */
#define CX_LINE(fn, format)                             \
    static const char *fn(char *out, size_t cap, int i) { \
        snprintf(out, cap, format, i);                  \
        return out;                                     \
    }
#define CX_LINE3(fn, format)                            \
    static const char *fn(char *out, size_t cap, int i) { \
        snprintf(out, cap, format, i, i, i);            \
        return out;                                     \
    }

typedef struct {
    char path[64];
    char *text;
} CxSourceFile;

static char *cx_lines_text(const char *head, CxLineFn line, const char *tail, int lines) {
    size_t room = strlen(head) + strlen(tail) + (size_t)lines * CX_BINDING_LINE_ROOM + 1;
    char *source = malloc(room);
    if (!source)
        return NULL;
    size_t length = (size_t)snprintf(source, room, "%s", head);
    for (int i = 0; i < lines; i++) {
        char row[CX_BINDING_LINE_ROOM];
        length += (size_t)snprintf(source + length, room - length, "%s", line(row, sizeof(row), i));
    }
    (void)snprintf(source + length, room - length, "%s", tail);
    return source;
}

static uint64_t cx_source_work(CBMLanguage language, const CxSourceFile *files, int count) {
    CBMFileResult **results = calloc((size_t)count, sizeof(*results));
    cbm_source_table_t *table = cbm_source_table_new();
    bool ok = results && table;
    for (int f = 0; ok && f < count; f++)
        results[f] = cbm_extract_file(files[f].text, (int)strlen(files[f].text), language, "test",
                                      files[f].path, 0, NULL, NULL);
    cbm_resolve_stats_t stats;
    cbm_resolve_stats_reset(&stats);
    cbm_resolve_stats_t *previous = cbm_resolve_stats_bind(&stats);
    for (int f = 0; ok && f < count; f++)
        ok = results[f] && cbm_source_collect(table, results[f], language, files[f].path,
                                              files[f].text, strlen(files[f].text));
    for (int f = 0; ok && f < count; f++) {
        CBMArena arena;
        cbm_arena_init(&arena);
        ok = cbm_source_unit_build(&arena, table, results[f], language, files[f].path,
                                   files[f].text, strlen(files[f].text)) != NULL;
        cbm_arena_destroy(&arena);
    }
    cbm_resolve_stats_bind(previous);
    uint64_t work = ok ? cx_stat_work(&stats, CBM_RESOLVE_STAT_LOOKUP_PROBES) : 0;
    for (int f = 0; results && f < count; f++)
        if (results[f])
            cbm_free_result(results[f]);
    free(results);
    if (table)
        cbm_source_table_free(table);
    return work;
}

/* One generated file, at CX_BINDING_LINES lines and at twice that. */
static bool cx_source_shape_is_near_linear(CBMLanguage language, const char *path, const char *head,
                                           CxLineFn line, const char *tail) {
    uint64_t work[2] = {0, 0};
    for (int round = 0; round < 2; round++) {
        CxSourceFile file = {.text = cx_lines_text(head, line, tail, CX_BINDING_LINES << round)};
        snprintf(file.path, sizeof(file.path), "%s", path);
        if (file.text)
            work[round] = cx_source_work(language, &file, 1);
        free(file.text);
    }
    double ratio = cx_ratio((double)work[1], (double)work[0]);
    printf("    %s source probes %llu -> %llu (ratio %.2f)\n", path, (unsigned long long)work[0],
           (unsigned long long)work[1], ratio);
    return work[0] >= CX_BINDING_LINES && ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI;
}

/* A name can repeat all through one file: a script's variable, a chunk's `local ok`, `init` in
 * a generated file. Each declaration asks which definition is its own. */
CX_LINE(cx_line_lua_same_function, "local function helper() return %d end\n")
CX_LINE(cx_line_lua_same_local, "local x = f(%d)\n")
CX_LINE(cx_line_shell_same_variable, "x=%d\n")
CX_LINE(cx_line_vim_same_function, "function! Foo()\n  return %d\nendfunction\n")
CX_LINE(cx_line_tcl_same_proc, "proc f {} { return %d }\n")
CX_LINE(cx_line_swift_same_function, "func f(a%d: Int) {}\n")
CX_LINE(cx_line_dart_same_method, "class C_%d { void go() {} }\n")
CX_LINE(cx_line_haskell_same_clause, "f %d = 1\n")

TEST(complexity_source_same_named_definitions_are_near_linear) {
    /* Every shape runs and prints, so a failure shows all of them. */
    bool ok = cx_source_shape_is_near_linear(CBM_LANG_LUA, "same.lua", "",
                                             cx_line_lua_same_function, "");
    ok = cx_source_shape_is_near_linear(CBM_LANG_LUA, "same.lua", "", cx_line_lua_same_local, "") &&
         ok;
    ok = cx_source_shape_is_near_linear(CBM_LANG_BASH, "same.sh", "", cx_line_shell_same_variable,
                                        "") &&
         ok;
    ok = cx_source_shape_is_near_linear(CBM_LANG_VIMSCRIPT, "same.vim", "",
                                        cx_line_vim_same_function, "") &&
         ok;
    ok = cx_source_shape_is_near_linear(CBM_LANG_TCL, "same.tcl", "", cx_line_tcl_same_proc, "") &&
         ok;
    ok = cx_source_shape_is_near_linear(CBM_LANG_SWIFT, "same.swift", "",
                                        cx_line_swift_same_function, "") &&
         ok;
    ok = cx_source_shape_is_near_linear(CBM_LANG_DART, "same.dart", "", cx_line_dart_same_method,
                                        "") &&
         ok;
    ok = cx_source_shape_is_near_linear(CBM_LANG_HASKELL, "Same.hs", "module Same where\n",
                                        cx_line_haskell_same_clause, "") &&
         ok;
    ASSERT_TRUE(ok);
    PASS();
}

/* A barrel library re-exports the project, and every library that imports it read its whole
 * export list again. */
static uint64_t cx_dart_barrel_work(int libraries) {
    CxSourceFile *files = calloc((size_t)libraries + 1, sizeof(*files));
    if (!files)
        return 0;
    size_t room = (size_t)libraries * 32 + 1;
    char *barrel = malloc(room);
    size_t used = 0;
    bool ok = barrel != NULL;
    for (int i = 0; ok && i < libraries; i++) {
        used += (size_t)snprintf(barrel + used, room - used, "export 'a%d.dart';\n", i);
        snprintf(files[i].path, sizeof(files[i].path), "lib/a%d.dart", i);
        files[i].text = malloc(96);
        ok = files[i].text != NULL;
        if (ok)
            snprintf(files[i].text, 96, "import 'all.dart';\nint f%d() => f%d();\n", i,
                     (i + 1) % libraries);
    }
    snprintf(files[libraries].path, sizeof(files[libraries].path), "lib/all.dart");
    files[libraries].text = barrel;
    uint64_t work = ok ? cx_source_work(CBM_LANG_DART, files, libraries + 1) : 0;
    for (int i = 0; i <= libraries; i++)
        free(files[i].text);
    free(files);
    return work;
}
TEST(complexity_dart_barrel_exports_are_near_linear) {
    enum { BARREL_LIBRARIES = 500 };
    uint64_t first = cx_dart_barrel_work(BARREL_LIBRARIES);
    uint64_t second = cx_dart_barrel_work(BARREL_LIBRARIES * 2);
    double ratio = cx_ratio((double)second, (double)first);
    printf("    barrel source probes %llu -> %llu (ratio %.2f)\n", (unsigned long long)first,
           (unsigned long long)second, ratio);
    ASSERT_GTE(first, (uint64_t)BARREL_LIBRARIES);
    ASSERT_TRUE(ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI);
    PASS();
}

/* A function of route registrations asks, for each, what its receiver was bound to. */
CX_LINE(cx_line_go_route, "  r.GET(\"/p%d\", h)\n")
CX_LINE3(cx_line_go_group, "  g%d := r.Group(\"/g%d\")\n  g%d.GET(\"/x\", h)\n")
static bool cx_go_route_shape_is_near_linear(const char *head, CxLineFn line) {
    uint64_t base = 0;
    double ratio = cx_extract_ratio(CBM_LANG_GO, "routes.go", head, line, "}\n", false,
                                    CBM_RESOLVE_STAT_LOOKUP_PROBES, &base);
    return base >= CX_BINDING_LINES && ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI;
}
TEST(complexity_go_route_bindings_are_near_linear) {
    bool ok = cx_go_route_shape_is_near_linear("package main\nfunc Register(r Router) {\n",
                                               cx_line_go_route);
    ok = cx_go_route_shape_is_near_linear("package main\nfunc Register() {\n  r := New()\n",
                                          cx_line_go_route) &&
         ok;
    ok = cx_go_route_shape_is_near_linear("package main\nfunc Register(r Router) {\n",
                                          cx_line_go_group) &&
         ok;
    ASSERT_TRUE(ok);
    PASS();
}

/* Flat assembly and routines: each statement looks back for the label above it. */
CX_LINE(cx_line_nasm_instruction, "    mov eax, %d\n")
CX_LINE(cx_line_objectscript_statement, "    Set x = %d\n")
static bool cx_flat_shape_is_linear(CBMLanguage language, const char *path, const char *head,
                                    CxLineFn line) {
    uint64_t base = 0;
    double ratio =
        cx_extract_ratio(language, path, head, line, "", false, CBM_RESOLVE_STAT_LOOKUP_PROBES, &base);
    return base >= CX_BINDING_LINES && ratio >= CX_RATIO_LO && ratio <= CX_RATIO_HI;
}
TEST(complexity_flat_preceding_label_is_linear) {
    bool ok = cx_flat_shape_is_linear(CBM_LANG_NASM, "flat.nasm", "global start\nstart:\n",
                                      cx_line_nasm_instruction);
    ok = cx_flat_shape_is_linear(CBM_LANG_OBJECTSCRIPT_ROUTINE, "Flat.mac", "Main()\n",
                                 cx_line_objectscript_statement) &&
         ok;
    ASSERT_TRUE(ok);
    PASS();
}

/* Every node of sources that parse, that do not, and that hold zero-width MISSING tokens: the
 * routed calls answer exactly as tree-sitter does. */
TEST(complexity_syntax_index_prev_siblings_answer_as_tree_sitter_does) {
    static const struct {
        CBMLanguage language;
        const char *source;
    } cases[] = {
        {CBM_LANG_PYTHON, "def f(a, b=1):\n    return [a, (b), {'k': a}]\n\nclass C:\n    x = 1\n"},
        {CBM_LANG_TYPESCRIPT,
         "@D() class A { @G('/x') m(a: number) { return a; } /* c */ n() {} }\nexport { A };\n"},
        {CBM_LANG_GO, "package p\nfunc f() {\n  r := g()\n  r.GET(\"/x\", h)\n  // c\n  if r != nil "
                      "{ return }\n}\n"},
        {CBM_LANG_C, "int f(void { return 1 }\nstruct S { int a };\nint g(void) { x = ; }\n"},
        {CBM_LANG_DART, "int f() { return 1; }\nclass C { void m() {} }\nint g( { }\n"},
        {CBM_LANG_JAVASCRIPT, "class { m( }\nfunction f(a { return a\n"},
        {CBM_LANG_NASM, "global s\ns:\n  mov eax, 1\n  ret\n"},
        {CBM_LANG_ELIXIR, "defmodule M do\n  def f(a), do: a &&\nend\n"},
    };
    size_t visited = 0, zero_width = 0;
    bool same = true;
    for (size_t c = 0; c < sizeof(cases) / sizeof(cases[0]); c++) {
        TSParser *parser = ts_parser_new();
        ASSERT_NOT_NULL(parser);
        ASSERT_TRUE(ts_parser_set_language(parser, cbm_ts_language(cases[c].language)));
        TSTree *tree =
            ts_parser_parse_string(parser, NULL, cases[c].source, (uint32_t)strlen(cases[c].source));
        ts_parser_delete(parser);
        ASSERT_NOT_NULL(tree);
        cbm_syntax_index_t index;
        cbm_syntax_index_begin(&index, tree);
        TSTreeCursor cursor = ts_tree_cursor_new(ts_tree_root_node(tree));
        for (bool more = true; more;) {
            TSNode node = ts_tree_cursor_current_node(&cursor);
            visited++;
            zero_width += ts_node_start_byte(node) == ts_node_end_byte(node);
            same = same && ts_node_eq(cbm_syntax_prev_sibling(node), ts_node_prev_sibling(node));
            same = same &&
                   ts_node_eq(cbm_syntax_prev_named_sibling(node), ts_node_prev_named_sibling(node));
            if (ts_tree_cursor_goto_first_child(&cursor))
                continue;
            while (!ts_tree_cursor_goto_next_sibling(&cursor)) {
                if (!ts_tree_cursor_goto_parent(&cursor)) {
                    more = false;
                    break;
                }
            }
        }
        ts_tree_cursor_delete(&cursor);
        cbm_syntax_index_end(&index);
        ts_tree_delete(tree);
    }
    ASSERT_TRUE(same);
    ASSERT_GTE(visited, (size_t)100);
    ASSERT_GTE(zero_width, (size_t)1);
    PASS();
}

/* Asking for the sibling before each child of a wide parent, plainly and through the index. */
static uint64_t cx_prev_sibling_work(int lines, bool plain) {
    char *source = cx_lines_text("", cx_line_py_statement, "", lines);
    TSParser *parser = ts_parser_new();
    TSTree *tree = NULL;
    if (source && parser && ts_parser_set_language(parser, cbm_ts_language(CBM_LANG_PYTHON)))
        tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)strlen(source));
    if (parser)
        ts_parser_delete(parser);
    uint64_t work = 0;
    if (tree) {
        cbm_resolve_stats_t stats;
        cbm_resolve_stats_reset(&stats);
        cbm_resolve_stats_t *previous = cbm_resolve_stats_bind(&stats);
        cbm_syntax_index_t index;
        cbm_syntax_index_begin(&index, tree);
        cbm_syntax_index_test_set_plain(plain);
        TSTreeCursor cursor = ts_tree_cursor_new(ts_tree_root_node(tree));
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            do {
                TSNode child = ts_tree_cursor_current_node(&cursor);
                (void)cbm_syntax_prev_named_sibling(child);
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        }
        ts_tree_cursor_delete(&cursor);
        cbm_syntax_index_test_set_plain(false);
        cbm_syntax_index_end(&index);
        cbm_resolve_stats_bind(previous);
        work = cx_stat_work(&stats, CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS);
        ts_tree_delete(tree);
    }
    free(source);
    return work;
}
TEST(complexity_prev_sibling_walk_is_linear) {
    uint64_t first = cx_prev_sibling_work(CX_BINDING_LINES, true);
    uint64_t second = cx_prev_sibling_work(CX_BINDING_LINES * 2, true);
    double plain = cx_ratio((double)second, (double)first);
    printf("    plain prev sibling steps %llu -> %llu (ratio %.2f)\n", (unsigned long long)first,
           (unsigned long long)second, plain);
    ASSERT_GTE(first, (uint64_t)CX_BINDING_LINES);
    ASSERT_TRUE(plain >= 3.2);
    first = cx_prev_sibling_work(CX_BINDING_LINES, false);
    second = cx_prev_sibling_work(CX_BINDING_LINES * 2, false);
    double indexed = cx_ratio((double)second, (double)first);
    printf("    indexed prev sibling steps %llu -> %llu (ratio %.2f)\n", (unsigned long long)first,
           (unsigned long long)second, indexed);
    ASSERT_GTE(first, (uint64_t)CX_BINDING_LINES);
    ASSERT_TRUE(indexed >= CX_RATIO_LO && indexed <= CX_RATIO_HI);
    PASS();
}

SUITE(complexity) {
    RUN_TEST(complexity_syntax_index_prev_siblings_answer_as_tree_sitter_does);
    RUN_TEST(complexity_prev_sibling_walk_is_linear);
    RUN_TEST(complexity_source_same_named_definitions_are_near_linear);
    RUN_TEST(complexity_dart_barrel_exports_are_near_linear);
    RUN_TEST(complexity_go_route_bindings_are_near_linear);
    RUN_TEST(complexity_flat_preceding_label_is_linear);
    RUN_TEST(complexity_import_map_is_linear_in_a_files_imports);
    RUN_TEST(complexity_import_map_keeps_conflicts_and_repeats);
    RUN_TEST(complexity_parse_memo_lasts_one_file);
    RUN_TEST(complexity_evidence_page_route_stores_what_sql_stores);
    RUN_TEST(complexity_shared_preparation_decides_the_same);
    RUN_TEST(complexity_parse_bind_answers_only_the_same_parse);
    RUN_TEST(complexity_binding_wide_module_questions_are_linear);
    RUN_TEST(complexity_binding_wide_inner_nodes_are_linear);
    RUN_TEST(complexity_syntax_index_answers_as_tree_sitter_does);
    RUN_TEST(complexity_syntax_index_wide_lists_match_tree_sitter);
    RUN_TEST(complexity_js_value_join_is_linear_in_a_repeated_name);
    RUN_TEST(complexity_python_dotted_imports_are_linear);
    RUN_TEST(complexity_python_import_join_is_linear);
    RUN_TEST(complexity_js_pass_registry_lookups_are_linear);
    RUN_TEST(complexity_import_uses_are_linear_in_every_language_pass);
    RUN_TEST(complexity_name_rows_answer_as_a_scan_does);
    RUN_TEST(complexity_target_file_list_is_near_linear);
    RUN_TEST(complexity_python_root_facts_are_linear);
    RUN_TEST(complexity_rust_pass_registration_is_linear);
    RUN_TEST(complexity_rust_local_lookup_is_linear);
    RUN_TEST(complexity_rust_conditional_use_lookup_is_linear);
    RUN_TEST(complexity_rust_named_use_lookup_is_linear);
    RUN_TEST(complexity_rust_trait_in_scope_is_linear);
    RUN_TEST(complexity_rust_module_children_are_linear);
    RUN_TEST(complexity_rust_outside_impls_are_linear);
    RUN_TEST(complexity_rust_impl_lookup_is_linear);
    RUN_TEST(complexity_go_pass_registration_is_linear);
    RUN_TEST(complexity_java_short_name_fallback_is_linear);
    RUN_TEST(complexity_csharp_pass_registration_is_linear);
    RUN_TEST(complexity_kotlin_class_registration_is_linear);
    RUN_TEST(complexity_java_class_registration_is_linear);
    RUN_TEST(complexity_ts_proof_callee_imports_are_linear);
    RUN_TEST(complexity_ts_proof_annotation_imports_are_linear);
    RUN_TEST(complexity_ts_proof_narrowing_lookup_is_linear);
    RUN_TEST(complexity_rust_source_facts_on_unclosed_nesting_are_linear);
    RUN_TEST(complexity_kotlin_cross_on_unclosed_nesting_is_linear);
    RUN_TEST(complexity_c_cross_on_unclosed_nesting_is_linear);
    RUN_TEST(complexity_cpp_cross_on_unclosed_nesting_is_linear);
    RUN_TEST(complexity_c_shared_cross_on_unclosed_nesting_is_linear);
    RUN_TEST(complexity_js_proof_on_unclosed_nesting_is_linear);
    RUN_TEST(complexity_dart_usages_on_unclosed_nesting_is_linear);
    RUN_TEST(complexity_usage_classifiers_on_deep_nesting_are_linear);
    RUN_TEST(complexity_decorator_lookup_is_linear);
    RUN_TEST(complexity_ts_class_registration_is_linear);
    RUN_TEST(complexity_rust_derive_registration_is_linear);
    RUN_TEST(complexity_go_generic_registration_is_linear);
    RUN_TEST(complexity_csharp_namespace_lookup_is_linear);
    RUN_TEST(complexity_registry_tail_answers_as_a_scan_does);
    RUN_TEST(complexity_kotlin_pass_registry_lookups_are_linear);
    RUN_TEST(complexity_doc_lookup_keeps_a_wide_root_cached);
    RUN_TEST(complexity_struct_field_fold_is_linear);
    RUN_TEST(complexity_php_doc_lookup_is_linear);
    RUN_TEST(complexity_php_short_name_fallback_is_linear);
    RUN_TEST(complexity_python_pass_import_join_is_linear);
    RUN_TEST(complexity_python_pass_wide_from_import_is_linear);
    RUN_TEST(complexity_flat_file_syntax_walk_is_linear);
    RUN_TEST(complexity_oo_local_lookup_is_linear);
    RUN_TEST(complexity_oo_explicit_imports_are_linear);
    RUN_TEST(complexity_resolver_counters_are_run_scoped);
    RUN_TEST(complexity_py_module_ranges_preserve_calls_with_bounded_work);
    RUN_TEST(complexity_rescript_error_recovery_lexing_is_linear);
    RUN_TEST(complexity_replicated_modules_scale_linearly);
    RUN_TEST(complexity_perfile_registry_work_is_linear);
    RUN_TEST(complexity_infra_resolution_is_linear);
    RUN_TEST(complexity_importance_scoring_is_linear);
    RUN_TEST(complexity_shared_package_growth_stays_linear);
    RUN_TEST(complexity_py_submodule_probe_avoids_registry_scan);
    RUN_TEST(complexity_throughput_report_written);
}
