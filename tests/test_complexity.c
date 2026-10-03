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
#include "test_framework.h"
#include "test_helpers.h"

#include "../src/foundation/compat.h"
#include "../src/foundation/compat_fs.h"
#include "../src/foundation/log.h"
#include "../src/foundation/profile.h"
#include "cbm.h"
#include "discover/discover.h"
#include "lang_specs.h"
#include "lsp/py_lsp.h"
#include "tree_sitter/api.h"
#include "pipeline/pass_lsp_cross.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "store/store.h"
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
        fprintf(f, "import f%d\n\n", j - 1);
    }
    fprintf(f, "def val%d(x):\n    return x + %d\n\n", j, j);
    if (j > 0) {
        fprintf(f, "def chain%d():\n    return f%d.val%d(1) + val%d(2)\n", j, j - 1, j - 1, j);
    } else {
        fprintf(f, "def chain0():\n    return val0(2)\n");
    }
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

/* Fixture-dir provider: tests/fixtures/complexity/<lang-name>/ — every file in
 * it is copied verbatim into each module dir. Lets a new language join the
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
        bool templated = false;
        for (int t = 0; t < CX_TEMPLATE_COUNT; t++) {
            if (CX_TEMPLATES[t].lang == (CBMLanguage)lang) {
                templated = true;
            }
        }
        if (templated) {
            continue;
        }
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
                (void)cx_copy_file(src, path);
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
    uint64_t py_probes;
    uint64_t py_probe_visits;
    int pyext_files;
    int pyext_defs;
    int pyext_controls;
    int errors;
    double wall_s;
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
            yyjson_doc *doc =
                yyjson_read(edges[i].properties_json, strlen(edges[i].properties_json), 0);
            const char *source =
                doc ? yyjson_get_str(yyjson_obj_get(yyjson_doc_get_root(doc), "source")) : NULL;
            found = found || (source && strcmp(source, "lsp") == 0);
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

static double cx_now_s(void) {
    struct timespec ts;
    cbm_profile_now(&ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static int cx_run(const char *root, const char *db_path, CxMetrics *out, bool pyext) {
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
    uint64_t pp0 = atomic_load_explicit(&cbm_py_submodule_probes, memory_order_relaxed);
    uint64_t pv0 = atomic_load_explicit(&cbm_py_submodule_probe_visits, memory_order_relaxed);

    atomic_store_explicit(&g_cx_pass_count, 0, memory_order_relaxed);
    cbm_log_set_sink_ex(cx_pass_sink, CBM_LOG_SINK_TEE);

    double t0 = cx_now_s();
    cbm_pipeline_t *p = cbm_pipeline_new(root, db_path, CBM_MODE_FULL);
    if (!p) {
        return -1;
    }
    int rc = cbm_pipeline_run(p);
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
    bool extra_ok = !pyext || cx_pyext_population(s, project, out);
    cbm_store_close(s);
    return extra_ok ? 0 : -1;
}

static double cx_ratio(double num, double den) {
    return den > 0.0 ? num / den : 0.0;
}

typedef int (*CxCorpusBuilder)(const char *root, int k);

static void cx_remove_corpus(const char *root) {
    if (th_rmtree(root) != 0) {
        fprintf(stderr, "  [complexity] WARN: could not remove temp corpus %s\n", root);
    }
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
    snprintf(root_a, sizeof(root_a), "%s/%s_XXXXXX", tmp, prefix_a);
    snprintf(root_b, sizeof(root_b), "%s/%s_XXXXXX", tmp, prefix_b);
    bool made_a = cbm_mkdtemp(root_a) != NULL;
    bool made_b = made_a && cbm_mkdtemp(root_b) != NULL;
    int rc = -1;
    if (made_b && build(root_a, CX_K_BASE) == 0 && build(root_b, CX_K_BASE * 2) == 0) {
        char db_a[600];
        char db_b[600];
        snprintf(db_a, sizeof(db_a), "%s/cx.db", root_a);
        snprintf(db_b, sizeof(db_b), "%s/cx.db", root_b);
        bool pyext = build == cx_build_pyext;
        if (cx_run(root_a, db_a, out_a, pyext) == 0 && cx_run(root_b, db_b, out_b, pyext) == 0) {
            rc = 0;
        }
    }
    if (made_b) {
        cx_remove_corpus(root_b);
    }
    if (made_a) {
        cx_remove_corpus(root_a);
    }
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

/* ── Tests ───────────────────────────────────────────────────────────── */

/* The flagship gate. Independent module copies ⇒ every extensive quantity must
 * scale linearly in the copy count. A files×corpus coupling shows up as a
 * ratio near 4 on the counter that sums per-file work. */
TEST(complexity_replicated_modules_scale_linearly) {
    if (cx_measure_pair() != 0) {
        FAIL("failed to build/run the complexity corpus pair");
    }
    const CxMetrics *a = &g_cx_base;
    const CxMetrics *b = &g_cx_doubled;

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
                "%llu}%s\n",
                i == 0 ? CX_K_BASE : CX_K_BASE * 2, m->nodes, m->edges, m->wall_s,
                m->wall_s > 0 ? (double)m->nodes / m->wall_s : 0.0,
                m->wall_s > 0 ? (double)m->edges / m->wall_s : 0.0,
                (unsigned long long)m->perfile_defs, (unsigned long long)m->tail_candidates,
                (unsigned long long)m->fallback_rows, i == 0 ? "," : "");
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

SUITE(complexity) {
    RUN_TEST(complexity_rescript_error_recovery_lexing_is_linear);
    RUN_TEST(complexity_replicated_modules_scale_linearly);
    RUN_TEST(complexity_perfile_registry_work_is_linear);
    RUN_TEST(complexity_importance_scoring_is_linear);
    RUN_TEST(complexity_shared_package_growth_stays_linear);
    RUN_TEST(complexity_py_submodule_probe_avoids_registry_scan);
    RUN_TEST(complexity_throughput_report_written);
}
