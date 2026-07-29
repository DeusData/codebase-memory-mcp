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
#include "foundation/str_util.h"
#include "cbm.h"

#include <limits.h>
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
    char *buf = malloc((size_t)size + CBM_TS_LOOKAHEAD_PAD);
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

static void handle_kustomize(cbm_pipeline_ctx_t *ctx, const char *path, const char *rel_path,
                             CBMFileResult *result) {
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

    /* If we have a cached extraction result, emit IMPORTS edges for
     * resources/bases/patches/components entries */
    int import_count = 0;
    CBMFileResult *res = result;
    bool allocated = false;

    if (!res) {
        /* Fall back to re-extraction */
        int src_len = 0;
        char *source = k8s_read_file(path, &src_len);
        if (source) {
            res = cbm_extract_file_with_options(source, src_len, CBM_LANG_KUSTOMIZE,
                                                ctx->project_name, rel_path,
                                                cbm_pipeline_ctx_extract_timeout(ctx), NULL, NULL,
                                                cbm_pipeline_mode_extracts_macro_nodes(ctx->mode));
            free(source);
            allocated = true;
        }
    }

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

        if (allocated) {
            cbm_free_result(res);
        }
    }

    cbm_log_info("pass.k8s.kustomize", "file", rel_path, "imports", itoa_k8s(import_count));
}

/* ── K8s cross-manifest label-selector matching ──────────────────────
 * A Service routes traffic to the workload Pods whose labels match the
 * Service's spec.selector.  We record, per manifest, the Resource node id plus
 * its keyed selector requirements (Services) and pod labels (workloads), then
 * after all manifests are processed connect each Service to the workload(s) it
 * targets via an INFRA_MAPS edge.  This models the runtime traffic path the same
 * way k8s itself resolves Service → Endpoints by label.
 *
 * Kubernetes matchLabels semantics require every selector key/value pair.
 * Preserve the historical app==name convention only for an absent `app` label;
 * values under unrelated keys must never produce a match.
 *
 * Storage grows geometrically instead of imposing working-set caps.  Common
 * manifests allocate only the pairs they contain; total memory is O(R + L),
 * where R is the number of Resources and L is their total label pairs. */
enum { K8S_INITIAL_PAIR_CAPACITY = CBM_SZ_4, K8S_INITIAL_RECORD_CAPACITY = CBM_SZ_32 };

typedef struct {
    char *key;
    char *value;
} k8s_label_pair_t;

typedef struct {
    int64_t node_id;
    char name[CBM_SZ_256];
    bool is_service;
    bool is_workload;
    k8s_label_pair_t *selectors;
    int selector_count;
    int selector_capacity;
    k8s_label_pair_t *labels;
    int label_count;
    int label_capacity;
} k8s_record_t;

typedef struct {
    k8s_record_t *items;
    int count;
    int cap;
} k8s_record_array_t;

typedef struct {
    const char *key;
    const char *value;
    int record_index;
} k8s_label_ref_t;

typedef struct {
    k8s_label_ref_t *items;
    int count;
    int cap;
} k8s_label_ref_array_t;

static bool k8s_reserve_items(void **items, int *capacity, int required, size_t item_size,
                              int initial_capacity) {
    if (!items || !capacity || required < 0 || item_size == 0 || initial_capacity <= 0) {
        return false;
    }
    if (required <= *capacity) {
        return true;
    }
    int next_capacity = *capacity > 0 ? *capacity : initial_capacity;
    while (next_capacity < required) {
        if (next_capacity > INT_MAX / CBM_SZ_2) {
            next_capacity = required;
            break;
        }
        next_capacity *= CBM_SZ_2;
    }
    if ((size_t)next_capacity > SIZE_MAX / item_size) {
        return false;
    }
    void *grown = realloc(*items, (size_t)next_capacity * item_size);
    if (!grown) {
        return false;
    }
    *items = grown;
    *capacity = next_capacity;
    return true;
}

static bool k8s_add_pair(k8s_label_pair_t **items, int *count, int *capacity, const char *key,
                         const char *value) {
    if (!items || !count || !capacity || !key || !key[0] || !value || !value[0]) {
        return true;
    }
    if (!k8s_reserve_items((void **)items, capacity, *count + SKIP_ONE, sizeof(**items),
                           K8S_INITIAL_PAIR_CAPACITY)) {
        return false;
    }
    char *owned_key = cbm_strdup(key);
    char *owned_value = cbm_strdup(value);
    if (!owned_key || !owned_value) {
        free(owned_key);
        free(owned_value);
        return false;
    }
    (*items)[*count] = (k8s_label_pair_t){.key = owned_key, .value = owned_value};
    (*count)++;
    return true;
}

static void k8s_record_free(k8s_record_t *rec) {
    if (!rec) {
        return;
    }
    for (int i = 0; i < rec->selector_count; i++) {
        free(rec->selectors[i].key);
        free(rec->selectors[i].value);
    }
    for (int i = 0; i < rec->label_count; i++) {
        free(rec->labels[i].key);
        free(rec->labels[i].value);
    }
    free(rec->selectors);
    free(rec->labels);
    *rec = (k8s_record_t){0};
}

static void k8s_record_array_free(k8s_record_array_t *records) {
    if (!records) {
        return;
    }
    for (int i = 0; i < records->count; i++) {
        k8s_record_free(&records->items[i]);
    }
    free(records->items);
    *records = (k8s_record_array_t){0};
}

static bool k8s_record_array_append(k8s_record_array_t *records, k8s_record_t *record) {
    if (!records || !record ||
        !k8s_reserve_items((void **)&records->items, &records->cap, records->count + SKIP_ONE,
                           sizeof(*records->items), K8S_INITIAL_RECORD_CAPACITY)) {
        return false;
    }
    records->items[records->count++] = *record;
    *record = (k8s_record_t){0};
    return true;
}

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

/* Scan a single-document k8s manifest's text for the resource name, selector
 * requirements (Service) and pod-template labels (workload).  A small
 * indentation path-stack distinguishes spec.selector from
 * spec.template.metadata.labels and from top-level metadata.name. */
static bool k8s_scan_labels(const char *source, k8s_record_t *rec) {
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
        if (len >= sizeof(line)) {
            /* A valid Kubernetes label key/value line fits in this buffer.
             * Ignore unrelated oversized YAML scalars whole; parsing a prefix
             * could fabricate a selector or label match. */
            p = eol ? eol + SKIP_ONE : NULL;
            continue;
        }
        memcpy(line, p, len);
        line[len] = '\0';

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
            char key[CBM_SZ_512];
            char val[CBM_SZ_512];
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
                        if (!k8s_add_pair(&rec->selectors, &rec->selector_count,
                                          &rec->selector_capacity, key, val)) {
                            return false;
                        }
                    } else if (under_labels) {
                        if (!k8s_add_pair(&rec->labels, &rec->label_count, &rec->label_capacity,
                                          key, val)) {
                            return false;
                        }
                    }
                }
            }
        }

        if (!eol) {
            break;
        }
        p = eol + 1;
    }
    return true;
}

static int k8s_pair_compare(const void *lhs, const void *rhs) {
    const k8s_label_pair_t *a = lhs;
    const k8s_label_pair_t *b = rhs;
    int key_cmp = strcmp(a->key, b->key);
    return key_cmp != 0 ? key_cmp : strcmp(a->value, b->value);
}

static bool k8s_workload_has_key(const k8s_record_t *workload, const char *key) {
    int lo = 0;
    int hi = workload->label_count;
    while (lo < hi) {
        int mid = lo + (hi - lo) / CBM_SZ_2;
        int cmp = strcmp(workload->labels[mid].key, key);
        if (cmp < 0) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo < workload->label_count && strcmp(workload->labels[lo].key, key) == 0;
}

static bool k8s_workload_has_pair(const k8s_record_t *workload, const k8s_label_pair_t *selector) {
    if (workload->label_count > 0 &&
        bsearch(selector, workload->labels, (size_t)workload->label_count,
                sizeof(*workload->labels), k8s_pair_compare)) {
        return true;
    }
    return strcmp(selector->key, "app") == 0 && !k8s_workload_has_key(workload, "app") &&
           workload->name[0] && strcmp(selector->value, workload->name) == 0;
}

/* True only if every service selector requirement matches the workload. */
static bool k8s_selector_matches(const k8s_record_t *svc, const k8s_record_t *wl) {
    for (int i = 0; i < svc->selector_count; i++) {
        if (!k8s_workload_has_pair(wl, &svc->selectors[i])) {
            return false;
        }
    }
    return svc->selector_count > 0;
}

static int k8s_label_ref_compare(const void *lhs, const void *rhs) {
    const k8s_label_ref_t *a = lhs;
    const k8s_label_ref_t *b = rhs;
    int key_cmp = strcmp(a->key, b->key);
    if (key_cmp != 0) {
        return key_cmp;
    }
    int value_cmp = strcmp(a->value, b->value);
    if (value_cmp != 0) {
        return value_cmp;
    }
    return (a->record_index > b->record_index) - (a->record_index < b->record_index);
}

static bool k8s_label_ref_append(k8s_label_ref_array_t *refs, const char *key, const char *value,
                                 int record_index) {
    if (!k8s_reserve_items((void **)&refs->items, &refs->cap, refs->count + SKIP_ONE,
                           sizeof(*refs->items), K8S_INITIAL_RECORD_CAPACITY)) {
        return false;
    }
    refs->items[refs->count++] =
        (k8s_label_ref_t){.key = key, .value = value, .record_index = record_index};
    return true;
}

static int k8s_label_ref_key_value_compare(const k8s_label_ref_t *ref, const char *key,
                                           const char *value) {
    int key_cmp = strcmp(ref->key, key);
    return key_cmp != 0 ? key_cmp : strcmp(ref->value, value);
}

static int k8s_label_ref_bound(const k8s_label_ref_array_t *refs, const char *key,
                               const char *value, bool upper) {
    int lo = 0;
    int hi = refs->count;
    while (lo < hi) {
        int mid = lo + (hi - lo) / CBM_SZ_2;
        int cmp = k8s_label_ref_key_value_compare(&refs->items[mid], key, value);
        if (cmp < 0 || (upper && cmp == 0)) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo;
}

static bool k8s_build_workload_index(k8s_record_array_t *records, k8s_label_ref_array_t *refs) {
    for (int i = 0; i < records->count; i++) {
        k8s_record_t *record = &records->items[i];
        if (!record->is_workload || record->node_id <= 0) {
            continue;
        }
        if (record->label_count > 1) {
            qsort(record->labels, (size_t)record->label_count, sizeof(*record->labels),
                  k8s_pair_compare);
        }
        for (int j = 0; j < record->label_count; j++) {
            if (!k8s_label_ref_append(refs, record->labels[j].key, record->labels[j].value, i)) {
                return false;
            }
        }
        if (record->name[0] && !k8s_workload_has_key(record, "app") &&
            !k8s_label_ref_append(refs, "app", record->name, i)) {
            return false;
        }
    }
    if (refs->count > 1) {
        qsort(refs->items, (size_t)refs->count, sizeof(*refs->items), k8s_label_ref_compare);
    }
    return true;
}

/* After all manifests are recorded, connect each Service to the workload(s) its
 * selector targets via an INFRA_MAPS edge (Service Resource → workload Resource).
 *
 * The inverted label index avoids scanning every workload for selectors with a
 * selective requirement.  Construction is O(L log L), lookup is
 * O(S*K*log L + C*K*log M), and memory is O(L), where K is selector size, C is
 * the smallest candidate set, and M is labels per candidate workload. */
static bool k8s_link_selectors(cbm_pipeline_ctx_t *ctx, k8s_record_array_t *recs) {
    k8s_label_ref_array_t refs = {0};
    if (!k8s_build_workload_index(recs, &refs)) {
        free(refs.items);
        return false;
    }

    int edges = 0;
    for (int i = 0; i < recs->count; i++) {
        const k8s_record_t *svc = &recs->items[i];
        if (!svc->is_service || svc->selector_count == 0 || svc->node_id <= 0) {
            continue;
        }

        int candidate_lo = 0;
        int candidate_hi = 0;
        int candidate_count = INT_MAX;
        for (int s = 0; s < svc->selector_count; s++) {
            int lo =
                k8s_label_ref_bound(&refs, svc->selectors[s].key, svc->selectors[s].value, false);
            int hi =
                k8s_label_ref_bound(&refs, svc->selectors[s].key, svc->selectors[s].value, true);
            if (hi - lo < candidate_count) {
                candidate_lo = lo;
                candidate_hi = hi;
                candidate_count = hi - lo;
            }
        }

        int previous_record = -SKIP_ONE;
        for (int c = candidate_lo; c < candidate_hi; c++) {
            int record_index = refs.items[c].record_index;
            if (record_index == previous_record) {
                continue;
            }
            previous_record = record_index;
            const k8s_record_t *wl = &recs->items[record_index];
            if (k8s_selector_matches(svc, wl)) {
                char escaped_service[CBM_SZ_1K];
                char escaped_workload[CBM_SZ_1K];
                char props[CBM_SZ_2K];
                cbm_json_escape(escaped_service, sizeof(escaped_service), svc->name);
                cbm_json_escape(escaped_workload, sizeof(escaped_workload), wl->name);
                snprintf(props, sizeof(props),
                         "{\"kind\":\"selector\",\"service\":\"%s\",\"workload\":\"%s\"}",
                         escaped_service, escaped_workload);
                if (cbm_gbuf_insert_edge(ctx->gbuf, svc->node_id, wl->node_id, "INFRA_MAPS",
                                         props) <= 0) {
                    free(refs.items);
                    return false;
                }
                edges++;
            }
        }
    }
    if (edges > 0) {
        cbm_log_info("pass.k8s.selectors", "linked", itoa_k8s(edges));
    }
    free(refs.items);
    return true;
}

/* ── K8s manifest handler ────────────────────────────────────────── */

/* source/src_len are the already-read file bytes (caller retains ownership and
 * must free after this call returns).  When `rec` is non-NULL it is populated
 * with the first Resource's node id, name and label/selector values for later
 * cross-manifest selector matching. */
static bool handle_k8s_manifest(cbm_pipeline_ctx_t *ctx, const char *path, const char *rel_path,
                                const char *source, int src_len, k8s_record_t *rec) {
    (void)path; /* retained for symmetry; source is always provided now */
    int resource_count = 0;

    CBMFileResult *res =
        cbm_extract_file_with_options(source, src_len, CBM_LANG_K8S, ctx->project_name, rel_path,
                                      cbm_pipeline_ctx_extract_timeout(ctx), NULL, NULL,
                                      cbm_pipeline_mode_extracts_macro_nodes(ctx->mode));
    if (!res) {
        return true;
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

    cbm_free_result(res);

    /* Record selector / pod-label values for later Service → workload linking. */
    if (rec && rec->node_id > 0 && !k8s_scan_labels(source, rec)) {
        return false;
    }

    cbm_log_info("pass.k8s.manifest", "file", rel_path, "resources", itoa_k8s(resource_count));
    return true;
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

int cbm_pipeline_pass_k8s(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count) {
    cbm_log_info("pass.start", "pass", "k8s", "files", itoa_k8s(file_count));

    cbm_init();

    int kustomize_count = 0;
    int manifest_count = 0;
    int helm_count = 0;

    /* Collect per-manifest selector/label records for cross-manifest matching. */
    k8s_record_array_t recs = {0};

    for (int i = 0; i < file_count; i++) {
        if (cbm_pipeline_check_cancel(ctx)) {
            k8s_record_array_free(&recs);
            return CBM_NOT_FOUND;
        }

        const char *path = files[i].path;
        const char *rel = files[i].rel_path;
        CBMLanguage lang = files[i].language;
        const char *base = k8s_basename(rel);

        CBMFileResult *cached =
            (ctx->result_cache && ctx->result_cache[i]) ? ctx->result_cache[i] : NULL;

        if (is_gomod_file(base) || lang == CBM_LANG_GOMOD || is_requirements_file(base)) {
            int dep_len = 0;
            char *dep_src = k8s_read_file(path, &dep_len);
            if (dep_src) {
                handle_dep_manifest(ctx, rel, dep_src,
                                    is_requirements_file(base) ? "pypi" : "gomod");
                free(dep_src);
            }
        } else if (cbm_is_kustomize_file(base)) {
            handle_kustomize(ctx, path, rel, cached);
            kustomize_count++;
        } else if (lang == CBM_LANG_YAML || lang == CBM_LANG_K8S) {
            /* Read source once to classify (and reuse for uncached extraction). */
            int src_len = 0;
            char *source = k8s_read_file(path, &src_len);
            if (source) {
                if (is_helm_chart_file(base)) {
                    handle_helm_chart(ctx, rel, source);
                    helm_count++;
                } else if (cbm_is_k8s_manifest(base, source)) {
                    /* Always re-extract with CBM_LANG_K8S regardless of any cached
                     * result: cached results were produced during the parallel YAML
                     * pass and contain no "Resource" definitions.  Pass the already-
                     * read source buffer so handle_k8s_manifest does not re-read. */
                    (void)cached; /* cached YAML result intentionally discarded */
                    k8s_record_t rec = {0};
                    if (!handle_k8s_manifest(ctx, path, rel, source, src_len, &rec)) {
                        k8s_record_free(&rec);
                        free(source);
                        k8s_record_array_free(&recs);
                        cbm_log_error("pass.k8s.err", "phase", "selector_scan", "file", rel);
                        return CBM_STORE_ERR;
                    }
                    if (rec.node_id > 0 && !k8s_record_array_append(&recs, &rec)) {
                        k8s_record_free(&rec);
                        free(source);
                        k8s_record_array_free(&recs);
                        cbm_log_error("pass.k8s.err", "phase", "record_append", "file", rel);
                        return CBM_STORE_ERR;
                    }
                    k8s_record_free(&rec);
                    manifest_count++;
                }
                free(source);
            }
        }
    }

    /* Connect Services to the workloads their selectors target (INFRA_MAPS). */
    if (!k8s_link_selectors(ctx, &recs)) {
        k8s_record_array_free(&recs);
        cbm_log_error("pass.k8s.err", "phase", "selector_link");
        return CBM_STORE_ERR;
    }
    k8s_record_array_free(&recs);

    cbm_log_info("pass.done", "pass", "k8s", "kustomize", itoa_k8s(kustomize_count), "manifests",
                 itoa_k8s(manifest_count));
    (void)helm_count;
    return 0;
}
