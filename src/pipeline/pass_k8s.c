/*
 * pass_k8s.c — Pipeline pass for Kubernetes manifest and Kustomize overlay processing.
 *
 * For each discovered YAML file:
 *   1. Check if it is a kustomize overlay (kustomization.yaml / kustomization.yml)
 *      → emit a Module node and IMPORTS edges for each resources/bases/patches entry
 *   2. Else if it is a generic k8s manifest (apiVersion: detected)
 *      → emit one Resource node per file (first document only — multi-document YAML is not yet
 * supported)
 *
 * Depends on: pass_infrascan.c (cbm_is_kustomize_file, cbm_is_k8s_manifest, cbm_infra_qn),
 *             extraction layer (cbm.h), graph_buffer, pipeline internals.
 */
#include "foundation/constants.h"
#include "pipeline/pipeline.h"
#include <stdint.h>
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "discover/discover.h"
#include "foundation/log.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/limits.h"
#include "cbm.h"
#include "helpers.h"               /* cbm_kind_in_set_free_cache */
#include "foundation/slab_alloc.h" /* cbm_slab_destroy_thread */
#include "foundation/platform.h"   /* cbm_default_worker_count */
#include "pipeline/worker_pool.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Internal helpers ────────────────────────────────────────────── */

/* Read entire file into heap-allocated buffer. Returns NULL on error.
 * Caller must free(). Sets *out_len to byte count. */
static char *k8s_read_file(const char *path, int *out_len) {
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return NULL;
    }

    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);

    if (size <= 0 || size > cbm_max_file_bytes()) { /* generous, env-configurable cap (B4) */
        (void)fclose(f);
        return NULL;
    }

    /* +pad: tree-sitter lexer lookahead reads past EOF; keep it in-bounds */
    enum { CBM_TS_LOOKAHEAD_PAD = 16 };
    char *buf = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)size + CBM_TS_LOOKAHEAD_PAD);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }

    size_t nread = fread(buf, SKIP_ONE, size, f);
    (void)fclose(f);
    if (nread > (size_t)size) {
        nread = (size_t)size;
    }
    memset(buf + nread, 0, CBM_TS_LOOKAHEAD_PAD);
    *out_len = (int)nread;
    return buf;
}

/* Format int to string for logging. Thread-safe via TLS. */
static const char *itoa_k8s(int val) {
    enum { RING_BUF_COUNT = 4, RING_BUF_MASK = 3 };
    static CBM_TLS char bufs[RING_BUF_COUNT][CBM_SZ_32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + SKIP_ONE) & RING_BUF_MASK;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* Extract the basename of a path (pointer into the string; no allocation). */
static const char *k8s_basename(const char *path) {
    const char *p = strrchr(path, '/');
    return p ? p + SKIP_ONE : path;
}

/* ── Kustomize handler ───────────────────────────────────────────── */

/* `result` is the cached parallel-pass result or, for an uncached file, the
 * re-extraction the preparation step made; the caller owns it. */
static void handle_kustomize(cbm_pipeline_ctx_t *ctx, const char *rel_path,
                             const CBMFileResult *result) {
    /* Emit Module node for this kustomize overlay file */
    char *mod_qn = cbm_infra_qn(ctx->project_name, rel_path, "kustomize", NULL);
    if (!mod_qn) {
        return;
    }

    int64_t mod_id = cbm_gbuf_upsert_node(ctx->gbuf, "Module", k8s_basename(rel_path), mod_qn,
                                          rel_path, SKIP_ONE, 0, "{\"source\":\"kustomize\"}");
    free(mod_qn);

    if (mod_id <= 0) {
        return;
    }

    /* With an extraction result, emit IMPORTS edges for
     * resources/bases/patches/components entries */
    int import_count = 0;
    const CBMFileResult *res = result;

    if (res) {
        for (int j = 0; j < res->imports.count; j++) {
            CBMImport *imp = &res->imports.items[j];
            if (!imp->module_path) {
                continue;
            }

            /* Compute target file QN */
            char *target_qn =
                cbm_pipeline_fqn_compute(ctx->project_name, imp->module_path, "__file__");
            if (!target_qn) {
                continue;
            }

            const cbm_gbuf_node_t *target = cbm_gbuf_find_by_qn(ctx->gbuf, target_qn);
            free(target_qn);

            if (target) {
                cbm_gbuf_insert_edge(ctx->gbuf, mod_id, target->id, "IMPORTS",
                                     "{\"via\":\"kustomize\"}");
                import_count++;
            }
        }
    }

    cbm_log_info("pass.k8s.kustomize", "file", rel_path, "imports", itoa_k8s(import_count));
}

/* ── K8s cross-manifest label-selector matching ──────────────────────
 * A Service routes traffic to the workload Pods whose labels match the
 * Service's spec.selector.  We record, per manifest, the Resource node id plus
 * its selector label-values (Services) and pod label-values (workloads), then
 * after all manifests are processed connect each Service to the workload(s) it
 * targets via an INFRA_MAPS edge.  This models the runtime traffic path the same
 * way k8s itself resolves Service → Endpoints by label.
 *
 * Matching is by label *value*: a Service selector `app: frontend` matches a
 * workload that either carries the pod label value "frontend" OR is named
 * "frontend" (the ubiquitous app==name convention, used when the pod template
 * omits explicit labels). */
enum { K8S_MAX_LABELS = 16, K8S_MAX_RECORDS = 512, K8S_LABEL_LEN = 128 };

typedef struct {
    int64_t node_id;
    char name[K8S_LABEL_LEN];
    bool is_service;
    bool is_workload;
    char selector_vals[K8S_MAX_LABELS][K8S_LABEL_LEN]; /* Service spec.selector values */
    int n_selector;
    char label_vals[K8S_MAX_LABELS][K8S_LABEL_LEN]; /* workload pod-template label values */
    int n_label;
} k8s_record_t;

typedef struct {
    k8s_record_t *items;
    int count;
    int cap;
} k8s_record_array_t;

/* Count leading-space indentation of a line (tabs are invalid YAML indent). */
static int k8s_indent(const char *line) {
    int n = 0;
    while (line[n] == ' ') {
        n++;
    }
    return n;
}

/* Split `key: value` (already de-indented). Returns 1 if a key was found; fills
 * key/val (val empty when the key opens a nested block). */
static int k8s_split_kv(const char *t, char *key, size_t key_sz, char *val, size_t val_sz) {
    key[0] = '\0';
    val[0] = '\0';
    if (t[0] == '#' || t[0] == '-' || t[0] == '\0') {
        return 0;
    }
    const char *colon = strchr(t, ':');
    if (!colon) {
        return 0;
    }
    size_t klen = (size_t)(colon - t);
    if (klen == 0 || klen >= key_sz) {
        return 0;
    }
    memcpy(key, t, klen);
    key[klen] = '\0';
    const char *v = colon + 1;
    while (*v == ' ' || *v == '\t') {
        v++;
    }
    size_t vn = 0;
    while (v[vn] && v[vn] != '\r' && v[vn] != '\n' && v[vn] != '#' && vn + 1 < val_sz) {
        val[vn] = v[vn];
        vn++;
    }
    /* trim trailing space */
    while (vn > 0 && (val[vn - 1] == ' ' || val[vn - 1] == '\t')) {
        vn--;
    }
    val[vn] = '\0';
    /* strip surrounding quotes */
    if (vn >= 2 && (val[0] == '"' || val[0] == '\'') && val[vn - 1] == val[0]) {
        memmove(val, val + 1, vn - 2);
        val[vn - 2] = '\0';
    }
    return 1;
}

static void k8s_add_val(char dst[][K8S_LABEL_LEN], int *n, const char *v) {
    if (*n >= K8S_MAX_LABELS || !v || !v[0]) {
        return;
    }
    snprintf(dst[*n], K8S_LABEL_LEN, "%s", v);
    (*n)++;
}

/* Scan a single-document k8s manifest's text for the resource name, selector
 * label-values (Service) and pod-template label-values (workload).  A small
 * indentation path-stack distinguishes spec.selector from
 * spec.template.metadata.labels and from top-level metadata.name. */
static void k8s_scan_labels(const char *source, k8s_record_t *rec) {
    enum { K8S_PATH_DEPTH = 12 };
    struct {
        int indent;
        char key[64];
    } stack[K8S_PATH_DEPTH];
    int depth = 0;
    bool got_name = false;

    const char *p = source;
    while (p && *p) {
        const char *eol = strchr(p, '\n');
        size_t len = eol ? (size_t)(eol - p) : strlen(p);
        char line[CBM_SZ_512];
        size_t cp = len < sizeof(line) - 1 ? len : sizeof(line) - 1;
        memcpy(line, p, cp);
        line[cp] = '\0';

        /* End of first YAML document — stop (one Resource per file). */
        const char *trimmed = line;
        while (*trimmed == ' ') {
            trimmed++;
        }
        if (strncmp(trimmed, "---", 3) == 0 && line == trimmed) {
            break;
        }

        if (trimmed[0] && trimmed[0] != '#') {
            int ind = k8s_indent(line);
            while (depth > 0 && stack[depth - 1].indent >= ind) {
                depth--;
            }
            char key[64];
            char val[K8S_LABEL_LEN];
            if (k8s_split_kv(trimmed, key, sizeof(key), val, sizeof(val))) {
                /* Build the current dotted path for context decisions. */
                bool under_selector = (depth >= 1 && strcmp(stack[depth - 1].key, "selector") == 0);
                bool under_labels = (depth >= 1 && strcmp(stack[depth - 1].key, "labels") == 0);
                bool under_metadata = (depth >= 1 && strcmp(stack[depth - 1].key, "metadata") == 0);

                if (val[0] == '\0') {
                    /* Block-opening key: push onto the path stack. */
                    if (depth < K8S_PATH_DEPTH) {
                        stack[depth].indent = ind;
                        snprintf(stack[depth].key, sizeof(stack[depth].key), "%s", key);
                        depth++;
                    }
                } else {
                    /* Leaf key: value. */
                    if (ind == 0 && strcmp(key, "kind") == 0) {
                        rec->is_service = (strcmp(val, "Service") == 0);
                        rec->is_workload =
                            (strcmp(val, "Deployment") == 0 || strcmp(val, "StatefulSet") == 0 ||
                             strcmp(val, "DaemonSet") == 0 || strcmp(val, "ReplicaSet") == 0 ||
                             strcmp(val, "ReplicationController") == 0 || strcmp(val, "Pod") == 0 ||
                             strcmp(val, "Job") == 0 || strcmp(val, "CronJob") == 0);
                    } else if (!got_name && under_metadata && strcmp(key, "name") == 0) {
                        snprintf(rec->name, sizeof(rec->name), "%s", val);
                        got_name = true;
                    } else if (under_selector) {
                        /* Service spec.selector matchLabels values. */
                        k8s_add_val(rec->selector_vals, &rec->n_selector, val);
                    } else if (under_labels) {
                        /* Pod-template / metadata labels values. */
                        k8s_add_val(rec->label_vals, &rec->n_label, val);
                    }
                }
            }
        }

        if (!eol) {
            break;
        }
        p = eol + 1;
    }
}

/* True if any of the service's selector values matches the workload. */
static bool k8s_selector_matches(const k8s_record_t *svc, const k8s_record_t *wl) {
    for (int s = 0; s < svc->n_selector; s++) {
        const char *sv = svc->selector_vals[s];
        if (!sv[0]) {
            continue;
        }
        if (wl->name[0] && strcmp(sv, wl->name) == 0) {
            return true;
        }
        for (int l = 0; l < wl->n_label; l++) {
            if (strcmp(sv, wl->label_vals[l]) == 0) {
                return true;
            }
        }
    }
    return false;
}

/* After all manifests are recorded, connect each Service to the workload(s) its
 * selector targets via an INFRA_MAPS edge (Service Resource → workload Resource). */
static void k8s_link_selectors(cbm_pipeline_ctx_t *ctx, const k8s_record_array_t *recs) {
    int edges = 0;
    for (int i = 0; i < recs->count; i++) {
        const k8s_record_t *svc = &recs->items[i];
        if (!svc->is_service || svc->n_selector == 0 || svc->node_id <= 0) {
            continue;
        }
        for (int j = 0; j < recs->count; j++) {
            const k8s_record_t *wl = &recs->items[j];
            if (i == j || !wl->is_workload || wl->node_id <= 0) {
                continue;
            }
            if (k8s_selector_matches(svc, wl)) {
                char props[CBM_SZ_256];
                snprintf(props, sizeof(props),
                         "{\"kind\":\"selector\",\"service\":\"%s\",\"workload\":\"%s\"}",
                         svc->name, wl->name);
                cbm_gbuf_insert_edge(ctx->gbuf, svc->node_id, wl->node_id, "INFRA_MAPS", props);
                edges++;
            }
        }
    }
    if (edges > 0) {
        cbm_log_info("pass.k8s.selectors", "linked", itoa_k8s(edges));
    }
}

/* ── K8s manifest handler ────────────────────────────────────────── */

/* source/src_len are the already-read file bytes (caller retains ownership and
 * must free after this call returns).  When `rec` is non-NULL it is populated
 * with the first Resource's node id, name and label/selector values for later
 * cross-manifest selector matching. */
static void handle_k8s_manifest(cbm_pipeline_ctx_t *ctx, const char *rel_path, const char *source,
                                const CBMFileResult *res, k8s_record_t *rec) {
    int resource_count = 0;

    if (!res) {
        return;
    }

    /* Compute file node QN for DEFINES edges */
    char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel_path, "__file__");
    const cbm_gbuf_node_t *file_node = file_qn ? cbm_gbuf_find_by_qn(ctx->gbuf, file_qn) : NULL;
    free(file_qn);

    for (int d = 0; d < res->defs.count; d++) {
        CBMDefinition *def = &res->defs.items[d];
        if (!def->label || strcmp(def->label, "Resource") != 0) {
            continue;
        }
        if (!def->name || !def->qualified_name) {
            continue;
        }

        int64_t node_id =
            cbm_gbuf_upsert_node(ctx->gbuf, "Resource", def->name, def->qualified_name, rel_path,
                                 (int)def->start_line, (int)def->end_line, "{\"source\":\"k8s\"}");

        /* DEFINES edge: File → Resource */
        if (file_node && node_id > 0) {
            cbm_gbuf_insert_edge(ctx->gbuf, file_node->id, node_id, "DEFINES", "{}");
        }

        /* Capture the first Resource for cross-manifest selector matching. */
        if (rec && rec->node_id <= 0 && node_id > 0) {
            rec->node_id = node_id;
        }

        resource_count++;
    }

    /* Record selector / pod-label values for later Service → workload linking. */
    if (rec && rec->node_id > 0) {
        k8s_scan_labels(source, rec);
    }

    cbm_log_info("pass.k8s.manifest", "file", rel_path, "resources", itoa_k8s(resource_count));
}

/* ── Helm chart handler ──────────────────────────────────────────── */

static bool is_helm_chart_file(const char *base) {
    return strcmp(base, "Chart.yaml") == 0 || strcmp(base, "Chart.yml") == 0;
}

/* Emit a Chart node for a Chart.yaml and a DEPENDS_ON edge to a (shared,
 * deduplicated) Chart node per declared dependency (#338). */
static void handle_helm_chart(cbm_pipeline_ctx_t *ctx, const char *rel_path, const char *source) {
    cbm_helm_chart_t hc;
    if (cbm_parse_helm_chart(source, &hc) != 0) {
        return;
    }

    const char *cname = hc.chart_name[0] ? hc.chart_name : k8s_basename(rel_path);
    char *chart_qn = cbm_infra_qn(ctx->project_name, rel_path, "helm-chart", NULL);
    if (!chart_qn) {
        return;
    }
    int64_t chart_id = cbm_gbuf_upsert_node(ctx->gbuf, "Chart", cname, chart_qn, rel_path, SKIP_ONE,
                                            0, "{\"source\":\"helm\"}");
    free(chart_qn);

    char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel_path, "__file__");
    const cbm_gbuf_node_t *file_node = file_qn ? cbm_gbuf_find_by_qn(ctx->gbuf, file_qn) : NULL;
    if (file_node && chart_id > 0) {
        cbm_gbuf_insert_edge(ctx->gbuf, file_node->id, chart_id, "DEFINES", "{}");
    }
    free(file_qn);

    int dep_edges = 0;
    for (int i = 0; i < hc.dep_count && chart_id > 0; i++) {
        /* Stable per-project QN so multiple charts depending on the same chart
         * link to one shared dependency node. */
        char dep_qn[CBM_SZ_512];
        snprintf(dep_qn, sizeof(dep_qn), "%s.__helm_dep__.%s", ctx->project_name, hc.deps[i]);
        int64_t dep_id =
            cbm_gbuf_upsert_node(ctx->gbuf, "Chart", hc.deps[i], dep_qn, rel_path, SKIP_ONE, 0,
                                 "{\"source\":\"helm\",\"external\":true}");
        if (dep_id > 0) {
            cbm_gbuf_insert_edge(ctx->gbuf, chart_id, dep_id, "DEPENDS_ON", "{}");
            dep_edges++;
        }
    }
    cbm_log_info("pass.k8s.helm", "file", rel_path, "deps", itoa_k8s(dep_edges));
}

/* ── Dependency-manifest handler (go.mod / requirements.txt) ──────── */

static bool is_gomod_file(const char *base) {
    return strcmp(base, "go.mod") == 0;
}

static bool is_requirements_file(const char *base) {
    return strcmp(base, "requirements.txt") == 0;
}

/* Emit a DEPENDS_ON edge from the manifest file node to a (shared, per-project)
 * external Package node.  Mirrors the Helm Chart.yaml DEPENDS_ON shape. */
static int emit_dep_edge(cbm_pipeline_ctx_t *ctx, const cbm_gbuf_node_t *src, const char *rel_path,
                         const char *ecosystem, const char *name) {
    if (!name || !name[0]) {
        return 0;
    }
    char dep_qn[CBM_SZ_512];
    snprintf(dep_qn, sizeof(dep_qn), "%s.__%s_dep__.%s", ctx->project_name, ecosystem, name);
    char dep_props[CBM_SZ_256];
    snprintf(dep_props, sizeof(dep_props), "{\"source\":\"%s\",\"external\":true}", ecosystem);
    int64_t dep_id =
        cbm_gbuf_upsert_node(ctx->gbuf, "Package", name, dep_qn, rel_path, SKIP_ONE, 0, dep_props);
    if (dep_id > 0 && dep_id != src->id) {
        cbm_gbuf_insert_edge(ctx->gbuf, src->id, dep_id, "DEPENDS_ON", "{}");
        return 1;
    }
    return 0;
}

/* Copy the first whitespace-delimited token of `line` into `out`. */
static void first_token(const char *line, char *out, size_t out_sz) {
    out[0] = '\0';
    while (*line == ' ' || *line == '\t') {
        line++;
    }
    size_t n = 0;
    while (line[n] && line[n] != ' ' && line[n] != '\t' && line[n] != '\r' && line[n] != '\n' &&
           n + 1 < out_sz) {
        out[n] = line[n];
        n++;
    }
    out[n] = '\0';
}

/* Parse go.mod `require` directives (single-line and block forms) and emit a
 * DEPENDS_ON edge per dependency.  go.mod requires are not surfaced as imports
 * by the extraction layer, so we parse the manifest text directly here. */
static int parse_gomod_deps(cbm_pipeline_ctx_t *ctx, const cbm_gbuf_node_t *src,
                            const char *rel_path, const char *source) {
    int edges = 0;
    bool in_block = false;
    const char *p = source;
    while (p && *p) {
        const char *eol = strchr(p, '\n');
        size_t len = eol ? (size_t)(eol - p) : strlen(p);
        char line[CBM_SZ_512];
        size_t cp = len < sizeof(line) - 1 ? len : sizeof(line) - 1;
        memcpy(line, p, cp);
        line[cp] = '\0';
        const char *t = line;
        while (*t == ' ' || *t == '\t') {
            t++;
        }
        if (in_block) {
            if (t[0] == ')') {
                in_block = false;
            } else if (t[0] && t[0] != '/') {
                char name[CBM_SZ_256];
                first_token(t, name, sizeof(name));
                edges += emit_dep_edge(ctx, src, rel_path, "gomod", name);
            }
        } else if (strncmp(t, "require", 7) == 0 && (t[7] == ' ' || t[7] == '\t' || t[7] == '(')) {
            const char *rest = t + 7;
            while (*rest == ' ' || *rest == '\t') {
                rest++;
            }
            if (*rest == '(') {
                in_block = true;
            } else if (*rest) {
                char name[CBM_SZ_256];
                first_token(rest, name, sizeof(name));
                edges += emit_dep_edge(ctx, src, rel_path, "gomod", name);
            }
        }
        if (!eol) {
            break;
        }
        p = eol + 1;
    }
    return edges;
}

/* Parse requirements.txt entries (one package spec per line) and emit a
 * DEPENDS_ON edge per dependency.  The package name is the leading token up to
 * the first version/extras/comment delimiter. */
static int parse_requirements_deps(cbm_pipeline_ctx_t *ctx, const cbm_gbuf_node_t *src,
                                   const char *rel_path, const char *source) {
    int edges = 0;
    const char *p = source;
    while (p && *p) {
        const char *eol = strchr(p, '\n');
        size_t len = eol ? (size_t)(eol - p) : strlen(p);
        char line[CBM_SZ_512];
        size_t cp = len < sizeof(line) - 1 ? len : sizeof(line) - 1;
        memcpy(line, p, cp);
        line[cp] = '\0';
        const char *t = line;
        while (*t == ' ' || *t == '\t') {
            t++;
        }
        /* Skip blanks, comments, options (-r, --hash), and URLs. */
        if (t[0] && t[0] != '#' && t[0] != '-' && strstr(t, "://") == NULL) {
            char name[CBM_SZ_256];
            size_t n = 0;
            while (t[n] && t[n] != '=' && t[n] != '<' && t[n] != '>' && t[n] != '!' &&
                   t[n] != '~' && t[n] != '[' && t[n] != ';' && t[n] != ' ' && t[n] != '\t' &&
                   t[n] != '\r' && n + 1 < sizeof(name)) {
                name[n] = t[n];
                n++;
            }
            name[n] = '\0';
            edges += emit_dep_edge(ctx, src, rel_path, "pypi", name);
        }
        if (!eol) {
            break;
        }
        p = eol + 1;
    }
    return edges;
}

static void handle_dep_manifest(cbm_pipeline_ctx_t *ctx, const char *rel_path, const char *source,
                                const char *ecosystem) {
    if (!source) {
        return;
    }
    char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel_path, "__file__");
    const cbm_gbuf_node_t *src = file_qn ? cbm_gbuf_find_by_qn(ctx->gbuf, file_qn) : NULL;
    free(file_qn);
    if (!src) {
        return;
    }
    int dep_edges = strcmp(ecosystem, "gomod") == 0
                        ? parse_gomod_deps(ctx, src, rel_path, source)
                        : parse_requirements_deps(ctx, src, rel_path, source);
    cbm_log_info("pass.k8s.depmanifest", "file", rel_path, "deps", itoa_k8s(dep_edges));
}

/* ── Pass entry point ────────────────────────────────────────────── */

/* What one file contributes, prepared off the graph: reading, classifying and
 * extracting are per-file work, so they run in parallel; everything that
 * touches the graph buffer then runs on this thread in file order, exactly as
 * the sequential loop did. Sequentially the pass re-read and re-parsed every
 * YAML manifest on one thread -- 1.1 s of the Go corpus (6,275 manifests;
 * profile + waste sanitizer, 2026-09-17). */
typedef enum { K8S_SKIP = 0, K8S_DEP, K8S_KUSTOMIZE, K8S_HELM, K8S_MANIFEST } k8s_kind_t;

typedef struct {
    k8s_kind_t kind;
    char *source; /* DEP, HELM, MANIFEST */
    int src_len;
    CBMFileResult *res; /* MANIFEST, or KUSTOMIZE without a cached result */
} k8s_prep_t;

typedef struct {
    cbm_pipeline_ctx_t *ctx;
    const cbm_file_info_t *files;
    k8s_prep_t *prep; /* indexed from `begin` */
    int begin;
    int end;
    _Atomic int next;
} k8s_prep_job_t;

enum { K8S_CHUNK = 1024 };

static void k8s_prep_file(const k8s_prep_job_t *job, int i, k8s_prep_t *out) {
    cbm_pipeline_ctx_t *ctx = job->ctx;
    const char *path = job->files[i].path;
    const char *rel = job->files[i].rel_path;
    CBMLanguage lang = job->files[i].language;
    const char *base = k8s_basename(rel);

    if (is_gomod_file(base) || lang == CBM_LANG_GOMOD || is_requirements_file(base)) {
        out->source = k8s_read_file(path, &out->src_len);
        out->kind = out->source ? K8S_DEP : K8S_SKIP;
    } else if (cbm_is_kustomize_file(base)) {
        out->kind = K8S_KUSTOMIZE;
        bool cached = ctx->result_cache && ctx->result_cache[i];
        if (!cached) {
            /* Fall back to re-extraction */
            int src_len = 0;
            char *source = k8s_read_file(path, &src_len);
            if (source) {
                out->res = cbm_extract_file(source, src_len, CBM_LANG_KUSTOMIZE, ctx->project_name,
                                            rel, CBM_EXTRACT_BUDGET, NULL, NULL);
                cbm_free(CBM_MEM_CLASS_OTHER, source);
            }
        }
    } else if (lang == CBM_LANG_YAML || lang == CBM_LANG_K8S) {
        /* Read source once to classify (and reuse for uncached extraction). */
        out->source = k8s_read_file(path, &out->src_len);
        if (!out->source) {
            return;
        }
        if (is_helm_chart_file(base)) {
            out->kind = K8S_HELM;
        } else if (cbm_is_k8s_manifest(base, out->source)) {
            /* Always re-extract with CBM_LANG_K8S regardless of any cached
             * result: cached results were produced during the parallel YAML
             * pass and contain no "Resource" definitions. */
            out->kind = K8S_MANIFEST;
            out->res = cbm_extract_file(out->source, out->src_len, CBM_LANG_K8S, ctx->project_name,
                                        rel, CBM_EXTRACT_BUDGET, NULL, NULL);
        } else {
            cbm_free(CBM_MEM_CLASS_OTHER, out->source);
            out->source = NULL;
        }
    }
}

static void k8s_prep_worker(int worker_id, void *arg) {
    (void)worker_id;
    k8s_prep_job_t *job = (k8s_prep_job_t *)arg;
    cbm_work_arena_keep_begin(); /* one scratch arena for this worker's files */
    while (true) {
        int i = atomic_fetch_add_explicit(&job->next, SKIP_ONE, memory_order_relaxed);
        if (i >= job->end) {
            break;
        }
        k8s_prep_file(job, i, &job->prep[i - job->begin]);
    }
    /* Per-thread extraction state, torn down like extract_worker's. */
    cbm_destroy_thread_parser();
    cbm_work_arena_release();
    cbm_slab_destroy_thread();
    cbm_kind_in_set_free_cache();
}

int cbm_pipeline_pass_k8s(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count) {
    cbm_log_info("pass.start", "pass", "k8s", "files", itoa_k8s(file_count));

    cbm_init();

    int kustomize_count = 0;
    int manifest_count = 0;
    int helm_count = 0;

    /* Collect per-manifest selector/label records for cross-manifest matching. */
    k8s_record_array_t recs = {0};
    recs.items = cbm_calloc(CBM_MEM_CLASS_OTHER, K8S_MAX_RECORDS * sizeof(*recs.items));
    recs.cap = recs.items ? K8S_MAX_RECORDS : 0;

    k8s_prep_t *prep = cbm_calloc(CBM_MEM_CLASS_OTHER, K8S_CHUNK * sizeof(*prep));
    if (!prep) {
        cbm_free(CBM_MEM_CLASS_OTHER, recs.items);
        return CBM_NOT_FOUND;
    }
    int workers = cbm_default_worker_count(false);

    for (int begin = 0; begin < file_count; begin += K8S_CHUNK) {
        int end = begin + K8S_CHUNK < file_count ? begin + K8S_CHUNK : file_count;
        if (cbm_pipeline_check_cancel(ctx)) {
            cbm_free(CBM_MEM_CLASS_OTHER, prep);
            cbm_free(CBM_MEM_CLASS_OTHER, recs.items);
            return CBM_NOT_FOUND;
        }
        memset(prep, 0, (size_t)K8S_CHUNK * sizeof(*prep));
        k8s_prep_job_t job = {.ctx = ctx, .files = files, .prep = prep, .begin = begin, .end = end};
        atomic_init(&job.next, begin);
        cbm_parallel_for(
            workers, k8s_prep_worker, &job,
            (cbm_parallel_for_opts_t){.max_workers = workers, .force_pthreads = false});

        bool cancelled = false;
        for (int i = begin; i < end; i++) {
            k8s_prep_t *p = &prep[i - begin];
            if (!cancelled && cbm_pipeline_check_cancel(ctx)) {
                cancelled = true;
            }
            if (!cancelled) {
                const char *rel = files[i].rel_path;
                const char *base = k8s_basename(rel);
                CBMFileResult *cached =
                    (ctx->result_cache && ctx->result_cache[i]) ? ctx->result_cache[i] : NULL;
                switch (p->kind) {
                case K8S_DEP:
                    handle_dep_manifest(ctx, rel, p->source,
                                        is_requirements_file(base) ? "pypi" : "gomod");
                    break;
                case K8S_KUSTOMIZE:
                    handle_kustomize(ctx, rel, cached ? cached : p->res);
                    kustomize_count++;
                    break;
                case K8S_HELM:
                    handle_helm_chart(ctx, rel, p->source);
                    helm_count++;
                    break;
                case K8S_MANIFEST: {
                    k8s_record_t *rec = (recs.count < recs.cap) ? &recs.items[recs.count] : NULL;
                    handle_k8s_manifest(ctx, rel, p->source, p->res, rec);
                    if (rec && rec->node_id > 0) {
                        recs.count++;
                    }
                    manifest_count++;
                    break;
                }
                case K8S_SKIP:
                default:
                    break;
                }
            }
            cbm_free(CBM_MEM_CLASS_OTHER, p->source);
            if (p->res) {
                cbm_free_result(p->res);
            }
        }
        if (cancelled) {
            cbm_free(CBM_MEM_CLASS_OTHER, prep);
            cbm_free(CBM_MEM_CLASS_OTHER, recs.items);
            return CBM_NOT_FOUND;
        }
    }
    cbm_free(CBM_MEM_CLASS_OTHER, prep);

    /* Connect Services to the workloads their selectors target (INFRA_MAPS). */
    k8s_link_selectors(ctx, &recs);
    cbm_free(CBM_MEM_CLASS_OTHER, recs.items);

    cbm_log_info("pass.done", "pass", "k8s", "kustomize", itoa_k8s(kustomize_count), "manifests",
                 itoa_k8s(manifest_count));
    (void)helm_count;
    return 0;
}
