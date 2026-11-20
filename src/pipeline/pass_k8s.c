/*
 * pass_k8s.c — Pipeline pass for Kubernetes manifests, Helm charts and dependency manifests.
 *
 * For each discovered YAML file that is a generic k8s manifest (apiVersion: detected)
 *   → emit one Resource node per file (first document only — multi-document YAML is not yet
 * supported). Kustomizations are not handled here: their entries are IMPORTS of the
 * file itself, resolved by the import resolver (resolve_kustomize_entry).
 *
 * Depends on: pass_infrascan.c (cbm_is_k8s_manifest, cbm_infra_qn),
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
#include "helpers.h"                /* cbm_kind_in_set_free_cache */
#include "foundation/slab_alloc.h"  /* cbm_slab_destroy_thread */
#include "foundation/platform.h"    /* cbm_default_worker_count */
#include "foundation/str_util.h"    /* cbm_json_escape */
#include "foundation/json_writer.h" /* cbm_json_writer_allocator */
#include "pipeline/helm_template.h"
#include "pipeline/worker_pool.h"

#include <ctype.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define K8S_SLEN(s) (sizeof(s) - 1)

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

/* ── K8s cross-manifest label-selector matching ──────────────────────
 * A Service routes traffic to the Pods whose labels carry EVERY pair of the
 * Service's spec.selector, in the Service's own namespace -- the way k8s
 * resolves Service → Endpoints. Per manifest we record the Resource node, its
 * namespace and the label pairs that take part: a Service's spec.selector, a
 * workload's pod-template labels, a bare Pod's metadata.labels. After all
 * manifests are read, an inverted index from "key=value" to workloads turns
 * each Service into a scan of the workloads carrying its rarest pair rather
 * than of every workload; each match becomes an INFRA_MAPS edge
 * (Service → workload). */
enum { K8S_LABEL_LEN = 128, K8S_GROW_MIN = 64 };

typedef enum {
    K8S_REC_OTHER = 0,
    K8S_REC_SERVICE,  /* pairs = spec.selector */
    K8S_REC_WORKLOAD, /* pairs = pod-template labels */
    K8S_REC_POD,      /* pairs = metadata.labels */
    K8S_REC_PDB,      /* pairs = spec.selector.matchLabels */
    K8S_REC_NETPOL,   /* pairs = spec.podSelector.matchLabels */
} k8s_rec_kind_t;

typedef struct {
    int64_t node_id;
    char name[K8S_LABEL_LEN];
    char ns[K8S_LABEL_LEN]; /* metadata.namespace; "" when unset */
    k8s_rec_kind_t kind;
    int pair_off; /* this record's pairs: pairs[pair_off .. pair_off + pair_n) */
    int pair_n;
    bool has_expr; /* its label selector also has matchExpressions */
} k8s_record_t;

typedef struct {
    k8s_record_t *items;
    int count;
    int cap;
    char **pairs; /* "key=value", heap-owned */
    int pair_count;
    int pair_cap;
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

/* Which label block a leaf `key: value` sits in, by its parent path. */
typedef enum {
    K8S_PAIR_NONE = 0,
    K8S_PAIR_SELECTOR,       /* spec.selector (a Service's selector is a flat map) */
    K8S_PAIR_TEMPLATE_LABEL, /* spec.template.metadata.labels, also under a CronJob's jobTemplate */
    K8S_PAIR_META_LABEL,     /* metadata.labels */
    K8S_PAIR_MATCH_LABELS,   /* spec.selector.matchLabels (a PodDisruptionBudget's) */
    K8S_PAIR_POD_SELECTOR,   /* spec.podSelector.matchLabels (a NetworkPolicy's) */
} k8s_pair_block_t;

static k8s_pair_block_t k8s_pair_block(const char *parent_path) {
    if (strcmp(parent_path, "spec.selector") == 0) {
        return K8S_PAIR_SELECTOR;
    }
    if (strcmp(parent_path, "spec.selector.matchLabels") == 0) {
        return K8S_PAIR_MATCH_LABELS;
    }
    if (strcmp(parent_path, "spec.podSelector.matchLabels") == 0) {
        return K8S_PAIR_POD_SELECTOR;
    }
    if (strcmp(parent_path, "spec.template.metadata.labels") == 0 ||
        strcmp(parent_path, "spec.jobTemplate.spec.template.metadata.labels") == 0) {
        return K8S_PAIR_TEMPLATE_LABEL;
    }
    if (strcmp(parent_path, "metadata.labels") == 0) {
        return K8S_PAIR_META_LABEL;
    }
    return K8S_PAIR_NONE;
}

static k8s_rec_kind_t k8s_rec_kind(const char *kind) {
    static const char *const workloads[] = {
        "Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "ReplicationController",
        "Job",        "CronJob",     "Rollout",   NULL};
    if (strcmp(kind, "Service") == 0) {
        return K8S_REC_SERVICE;
    }
    if (strcmp(kind, "PodDisruptionBudget") == 0) {
        return K8S_REC_PDB;
    }
    if (strcmp(kind, "NetworkPolicy") == 0) {
        return K8S_REC_NETPOL;
    }
    if (strcmp(kind, "Pod") == 0) {
        return K8S_REC_POD;
    }
    for (int i = 0; workloads[i]; i++) {
        if (strcmp(kind, workloads[i]) == 0) {
            return K8S_REC_WORKLOAD;
        }
    }
    return K8S_REC_OTHER;
}

/* The block whose pairs a record of this kind contributes. */
static k8s_pair_block_t k8s_kind_block(k8s_rec_kind_t kind) {
    switch (kind) {
    case K8S_REC_SERVICE:
        return K8S_PAIR_SELECTOR;
    case K8S_REC_WORKLOAD:
        return K8S_PAIR_TEMPLATE_LABEL;
    case K8S_REC_POD:
        return K8S_PAIR_META_LABEL;
    case K8S_REC_PDB:
        return K8S_PAIR_MATCH_LABELS;
    case K8S_REC_NETPOL:
        return K8S_PAIR_POD_SELECTOR;
    case K8S_REC_OTHER:
    default:
        return K8S_PAIR_NONE;
    }
}

/* One scanned `key: value` with the block it sits in. */
typedef struct {
    k8s_pair_block_t block;
    char *kv; /* "key=value", heap-owned until moved into the record array */
} k8s_scanned_pair_t;

typedef struct {
    k8s_scanned_pair_t *items;
    int count;
    int cap;
} k8s_scanned_t;

static char *k8s_join_kv(const char *key, const char *val) {
    size_t kl = strlen(key);
    size_t vl = strlen(val);
    char *kv = cbm_alloc(CBM_MEM_CLASS_OTHER, kl + vl + 2);
    if (!kv) {
        return NULL;
    }
    memcpy(kv, key, kl);
    kv[kl] = '=';
    memcpy(kv + kl + 1, val, vl + 1);
    return kv;
}

static void k8s_scanned_push(k8s_scanned_t *s, k8s_pair_block_t block, const char *key,
                             const char *val) {
    if (s->count == s->cap) {
        int cap = s->cap ? s->cap * 2 : K8S_GROW_MIN;
        k8s_scanned_pair_t *grown =
            cbm_realloc(CBM_MEM_CLASS_OTHER, s->items, (size_t)cap * sizeof(*grown));
        if (!grown) {
            return;
        }
        s->items = grown;
        s->cap = cap;
    }
    char *kv = k8s_join_kv(key, val);
    if (kv) {
        s->items[s->count++] = (k8s_scanned_pair_t){.block = block, .kv = kv};
    }
}

/* Dotted path of the open block keys, e.g. "spec.template.metadata.labels". */
static void k8s_parent_path(char *out, size_t out_sz, const char (*keys)[64], int depth) {
    out[0] = '\0';
    size_t w = 0;
    for (int i = 0; i < depth && w + 1 < out_sz; i++) {
        int n = snprintf(out + w, out_sz - w, "%s%s", i ? "." : "", keys[i]);
        if (n < 0) {
            break;
        }
        w += (size_t)n < out_sz - w ? (size_t)n : out_sz - w - 1;
    }
}

/* `key: { a: b, c: d }` — a flow-style map is the same label map written on
 * one line. One the line buffer cut short (no closing brace) is skipped
 * whole: a selector missing pairs would select too much. */
static void k8s_scan_flow_map(k8s_scanned_t *out, const char (*keys)[64], int depth,
                              const char *key, const char *val) {
    size_t n = strlen(val);
    if (n < PAIR_LEN || val[0] != '{' || val[n - SKIP_ONE] != '}') {
        return;
    }
    char path[CBM_SZ_512];
    k8s_parent_path(path, sizeof(path), keys, depth);
    size_t w = strlen(path);
    snprintf(path + w, sizeof(path) - w, "%s%s", w ? "." : "", key);
    k8s_pair_block_t block = k8s_pair_block(path);
    if (block == K8S_PAIR_NONE) {
        return;
    }
    char inner[CBM_SZ_512];
    snprintf(inner, sizeof(inner), "%.*s", (int)(n - PAIR_LEN), val + SKIP_ONE);
    for (char *item = inner; item;) {
        char *comma = strchr(item, ',');
        if (comma) {
            *comma = '\0';
        }
        while (*item == ' ') {
            item++;
        }
        char k[64];
        char v[K8S_LABEL_LEN];
        if (k8s_split_kv(item, k, sizeof(k), v, sizeof(v)) && v[0]) {
            size_t kl = strlen(k);
            while (kl > 0 && k[kl - SKIP_ONE] == ' ') {
                k[--kl] = '\0';
            }
            k8s_scanned_push(out, block, k, v);
        }
        item = comma ? comma + SKIP_ONE : NULL;
    }
}

/* Scan ONE manifest document's text for its name, namespace, kind and
 * label pairs into `rec` / `out`. A small indentation path-stack gives every
 * leaf its parent path, which tells spec.selector from the pod template's
 * labels and from the object's own metadata.labels. */
static void k8s_scan_labels(const char *source, k8s_record_t *rec, k8s_scanned_t *out) {
    enum { K8S_PATH_DEPTH = 12 };
    int indents[K8S_PATH_DEPTH];
    char keys[K8S_PATH_DEPTH][64];
    int depth = 0;
    bool got_name = false;
    bool seen_key = false;

    const char *p = source;
    while (p && *p) {
        const char *eol = strchr(p, '\n');
        size_t len = eol ? (size_t)(eol - p) : strlen(p);
        char line[CBM_SZ_512];
        size_t cp = len < sizeof(line) - 1 ? len : sizeof(line) - 1;
        memcpy(line, p, cp);
        line[cp] = '\0';

        /* End of the document — stop (the caller hands over one document). */
        const char *trimmed = line;
        while (*trimmed == ' ') {
            trimmed++;
        }
        /* A `---` before any key opens the first document; it does not end it. */
        if (strncmp(trimmed, "---", 3) == 0 && line == trimmed) {
            if (seen_key) {
                break;
            }
            if (!eol) {
                break;
            }
            p = eol + 1;
            continue;
        }

        if (trimmed[0] && trimmed[0] != '#') {
            seen_key = true;
            int ind = k8s_indent(line);
            while (depth > 0 && indents[depth - 1] >= ind) {
                depth--;
            }
            char key[64];
            char val[CBM_SZ_512]; /* a whole flow map fits: the line is the limit */
            if (k8s_split_kv(trimmed, key, sizeof(key), val, sizeof(val))) {
                if (strcmp(key, "matchExpressions") == 0) {
                    char parent[CBM_SZ_512];
                    k8s_parent_path(parent, sizeof(parent), (const char (*)[64])keys, depth);
                    rec->has_expr = rec->has_expr || strcmp(parent, "spec.selector") == 0 ||
                                    strcmp(parent, "spec.podSelector") == 0;
                }
                if (val[0] == '\0') {
                    /* Block-opening key: push onto the path stack. */
                    if (depth < K8S_PATH_DEPTH) {
                        indents[depth] = ind;
                        snprintf(keys[depth], sizeof(keys[depth]), "%s", key);
                        depth++;
                    }
                } else if (ind == 0 && strcmp(key, "kind") == 0) {
                    rec->kind = k8s_rec_kind(val);
                } else if (val[0] == '{') {
                    k8s_scan_flow_map(out, (const char (*)[64])keys, depth, key, val);
                } else if (depth == 1 && strcmp(keys[0], "metadata") == 0) {
                    if (!got_name && strcmp(key, "name") == 0) {
                        snprintf(rec->name, sizeof(rec->name), "%s", val);
                        got_name = true;
                    } else if (strcmp(key, "namespace") == 0) {
                        snprintf(rec->ns, sizeof(rec->ns), "%s", val);
                    }
                } else {
                    char parent[CBM_SZ_512];
                    k8s_parent_path(parent, sizeof(parent), (const char (*)[64])keys, depth);
                    k8s_pair_block_t block = k8s_pair_block(parent);
                    if (block != K8S_PAIR_NONE) {
                        k8s_scanned_push(out, block, key, val);
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

static bool k8s_records_reserve(k8s_record_array_t *recs, int pair_extra) {
    if (recs->count == recs->cap) {
        int cap = recs->cap ? recs->cap * 2 : K8S_GROW_MIN;
        k8s_record_t *grown =
            cbm_realloc(CBM_MEM_CLASS_OTHER, recs->items, (size_t)cap * sizeof(*grown));
        if (!grown) {
            return false;
        }
        recs->items = grown;
        recs->cap = cap;
    }
    if (recs->pair_count + pair_extra > recs->pair_cap) {
        int cap = recs->pair_cap ? recs->pair_cap : K8S_GROW_MIN;
        while (cap < recs->pair_count + pair_extra) {
            cap *= 2;
        }
        char **grown = cbm_realloc(CBM_MEM_CLASS_OTHER, recs->pairs, (size_t)cap * sizeof(*grown));
        if (!grown) {
            return false;
        }
        recs->pairs = grown;
        recs->pair_cap = cap;
    }
    return true;
}

/* Record the manifest behind Resource `node_id`: scan its text, keep the
 * pairs its kind contributes, drop the rest. */
static void k8s_record_manifest(k8s_record_array_t *recs, int64_t node_id, const char *source) {
    k8s_record_t rec = {.node_id = node_id};
    k8s_scanned_t scanned = {0};
    k8s_scan_labels(source, &rec, &scanned);
    k8s_pair_block_t keep = k8s_kind_block(rec.kind);
    int kept = 0;
    for (int i = 0; i < scanned.count; i++) {
        kept += scanned.items[i].block == keep;
    }
    if (rec.kind != K8S_REC_OTHER && k8s_records_reserve(recs, kept)) {
        rec.pair_off = recs->pair_count;
        for (int i = 0; i < scanned.count; i++) {
            if (scanned.items[i].block == keep) {
                recs->pairs[recs->pair_count++] = scanned.items[i].kv;
                scanned.items[i].kv = NULL;
            }
        }
        rec.pair_n = recs->pair_count - rec.pair_off;
        recs->items[recs->count++] = rec;
    }
    for (int i = 0; i < scanned.count; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, scanned.items[i].kv);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, scanned.items);
}

/* Record the document at lines [start_line, end_line] (1-based, inclusive) of
 * a multi-document manifest: every object's selector or pod labels come from
 * its own document. */
static void k8s_record_document(k8s_record_array_t *recs, int64_t node_id, const char *source,
                                int start_line, int end_line) {
    const char *begin = source;
    for (int line = 1; begin && line < start_line; line++) {
        begin = strchr(begin, '\n');
        begin = begin ? begin + SKIP_ONE : NULL;
    }
    if (!begin) {
        return;
    }
    const char *end = begin;
    for (int line = start_line; *end && line <= end_line; line++) {
        const char *eol = strchr(end, '\n');
        end = eol ? eol + SKIP_ONE : end + strlen(end);
    }
    size_t n = (size_t)(end - begin);
    char *doc = cbm_alloc(CBM_MEM_CLASS_OTHER, n + SKIP_ONE);
    if (!doc) {
        return;
    }
    memcpy(doc, begin, n);
    doc[n] = '\0';
    k8s_record_manifest(recs, node_id, doc);
    cbm_free(CBM_MEM_CLASS_OTHER, doc);
}

/* Built-in kinds that live outside any namespace (IDENTITY.md). A CRD's own
 * scope (spec.scope) is the curated / generated table's job. */
static bool k8s_cluster_scoped(const char *kind) {
    static const char *const kinds[] = {"Namespace",
                                        "Node",
                                        "PersistentVolume",
                                        "ClusterRole",
                                        "ClusterRoleBinding",
                                        "CustomResourceDefinition",
                                        "StorageClass",
                                        "PriorityClass",
                                        "ValidatingWebhookConfiguration",
                                        "MutatingWebhookConfiguration",
                                        "APIService",
                                        "CSIDriver",
                                        "CSINode",
                                        "IngressClass",
                                        "RuntimeClass",
                                        "VolumeAttachment",
                                        "ValidatingAdmissionPolicy",
                                        "ValidatingAdmissionPolicyBinding",
                                        "ClusterIssuer"};
    for (size_t i = 0; i < sizeof(kinds) / sizeof(kinds[0]); i++) {
        if (strcmp(kind, kinds[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* A Resource's properties from its identity key, the part of its QN after
 * "::": `<group>/<Kind>/<declared namespace or ->/<name>` (IDENTITY.md).
 * `obj_key` is the logical identity: the namespace it lives in, `_` for a
 * cluster-scoped kind, `?` when the deploy decides; `effective_ns` (when
 * not NULL) is the namespace a kustomize build gives it. `version` (NULL or
 * "": none) is the version of its apiVersion, which the key leaves out;
 * `labels` (NULL: none) its metadata.labels as "k=v,k2=v2" (extract_k8s.c:
 * nothing JSON escapes in it); `ecosystem` (NULL: a manifest's) what
 * declares it otherwise -- "terraform" (pass_terraform.c), plain text. NULL
 * for a QN without a well-formed key (the caller keeps the plain source
 * tag). */
static char *k8s_resource_props(const char *qn, const char *effective_ns, const char *version,
                                const char *labels, const char *ecosystem) {
    enum { K8S_KEY_PARTS = 4, K8S_ESC = 6 * K8S_LABEL_LEN };
    const char *key = qn ? strstr(qn, "::") : NULL;
    if (!key) {
        return NULL;
    }
    key += K8S_SLEN("::");
    char part[K8S_KEY_PARTS][K8S_LABEL_LEN];
    int parts = 0;
    for (const char *p = key; parts < K8S_KEY_PARTS; parts++) {
        const char *slash = parts < K8S_KEY_PARTS - SKIP_ONE ? strchr(p, '/') : NULL;
        size_t len = slash ? (size_t)(slash - p) : strlen(p);
        if (len == 0 || len >= K8S_LABEL_LEN || (!slash && parts < K8S_KEY_PARTS - SKIP_ONE)) {
            return NULL;
        }
        memcpy(part[parts], p, len);
        part[parts][len] = '\0';
        p = slash ? slash + SKIP_ONE : p + len;
    }
    const char *group = part[0];
    const char *kind = part[1];
    const char *ns = strcmp(part[2], "-") == 0 ? "" : part[2];
    const char *name = part[3];
    const char *effective = k8s_cluster_scoped(kind) ? "_"
                            : effective_ns           ? effective_ns
                            : ns[0]                  ? ns
                                                     : "?";
    char esc[K8S_KEY_PARTS + PAIR_LEN][K8S_ESC];
    cbm_json_escape(esc[0], K8S_ESC, group);
    cbm_json_escape(esc[1], K8S_ESC, kind);
    cbm_json_escape(esc[2], K8S_ESC, ns);
    cbm_json_escape(esc[3], K8S_ESC, name);
    cbm_json_escape(esc[4], K8S_ESC, effective);
    cbm_json_escape(esc[5], K8S_ESC, version ? version : "");
    size_t n =
        K8S_KEY_PARTS * K8S_ESC + PAIR_LEN * K8S_ESC + CBM_SZ_256 + (labels ? strlen(labels) : 0);
    char *props = cbm_alloc(CBM_MEM_CLASS_OTHER, n);
    if (!props) {
        return NULL;
    }
    int w = snprintf(props, n, "{\"source\":\"k8s\"");
    if (ecosystem && ecosystem[0]) {
        w += snprintf(props + w, n - (size_t)w, ",\"ecosystem\":\"%s\"", ecosystem);
    }
    w += snprintf(props + w, n - (size_t)w, ",\"group\":\"%s\",\"kind\":\"%s\"", esc[0], esc[1]);
    if (esc[5][0]) {
        w += snprintf(props + w, n - (size_t)w, ",\"version\":\"%s\"", esc[5]);
    }
    if (labels && labels[0]) {
        w += snprintf(props + w, n - (size_t)w, ",\"labels\":\"%s\"", labels);
    }
    if (ns[0]) {
        w += snprintf(props + w, n - (size_t)w, ",\"namespace\":\"%s\"", esc[2]);
    }
    snprintf(props + w, n - (size_t)w, ",\"obj_key\":\"%s/%s/%s/%s\"}", esc[0], esc[1], esc[4],
             esc[3]);
    return props;
}

char *cbm_k8s_resource_props(const char *qn, const char *effective_ns, const char *version,
                             const char *labels, const char *ecosystem) {
    return k8s_resource_props(qn, effective_ns, version, labels, ecosystem);
}

/* The string property `key` of node `n` (as k8s_resource_props writes it:
 * no escapes inside) into `out`, "" when it has none. */
static void k8s_node_prop(const cbm_gbuf_node_t *n, const char *key, char *out, size_t out_sz) {
    out[0] = '\0';
    char head[CBM_SZ_64];
    snprintf(head, sizeof(head), "\"%s\":\"", key);
    const char *v = n->properties_json ? strstr(n->properties_json, head) : NULL;
    if (v) {
        v += strlen(head);
        const char *e = strchr(v, '"');
        snprintf(out, out_sz, "%.*s", e ? (int)(e - v) : 0, v);
    }
}

/* The string property `key` of node `n` as k8s_resource_props writes it,
 * any length (caller frees), NULL when it has none. */
static char *k8s_node_prop_dup(const cbm_gbuf_node_t *n, const char *key) {
    char head[CBM_SZ_64];
    snprintf(head, sizeof(head), "\"%s\":\"", key);
    const char *v = n->properties_json ? strstr(n->properties_json, head) : NULL;
    const char *e = v ? strchr(v + strlen(head), '"') : NULL;
    if (!e) {
        return NULL;
    }
    v += strlen(head);
    char *out = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)(e - v) + SKIP_ONE);
    if (out) {
        memcpy(out, v, (size_t)(e - v));
        out[e - v] = '\0';
    }
    return out;
}

static void k8s_records_free(k8s_record_array_t *recs) {
    for (int i = 0; i < recs->pair_count; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, recs->pairs[i]);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, recs->pairs);
    cbm_free(CBM_MEM_CLASS_OTHER, recs->items);
    *recs = (k8s_record_array_t){0};
}

/* Workloads (record indices, ascending) that carry one "key=value" pair. */
typedef struct {
    int *items;
    int count;
    int cap;
} k8s_posting_t;

static void k8s_posting_free(const char *key, void *value, void *userdata) {
    (void)key;
    (void)userdata;
    k8s_posting_t *posting = value;
    cbm_free(CBM_MEM_CLASS_OTHER, posting->items);
    cbm_free(CBM_MEM_CLASS_OTHER, posting);
}

static bool k8s_posting_add(CBMHashTable *index, const char *kv, int rec_idx) {
    k8s_posting_t *posting = cbm_ht_get(index, kv);
    if (!posting) {
        posting = cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*posting));
        if (!posting) {
            return false;
        }
        cbm_ht_set(index, kv, posting);
    }
    if (posting->count && posting->items[posting->count - 1] == rec_idx) {
        return true; /* the same pair twice in one workload */
    }
    if (posting->count == posting->cap) {
        int cap = posting->cap ? posting->cap * 2 : K8S_GROW_MIN;
        int *grown = cbm_realloc(CBM_MEM_CLASS_OTHER, posting->items, (size_t)cap * sizeof(*grown));
        if (!grown) {
            return false;
        }
        posting->items = grown;
        posting->cap = cap;
    }
    posting->items[posting->count++] = rec_idx;
    return true;
}

static bool k8s_has_pair(const k8s_record_array_t *recs, const k8s_record_t *rec, const char *kv) {
    for (int i = 0; i < rec->pair_n; i++) {
        if (strcmp(recs->pairs[rec->pair_off + i], kv) == 0) {
            return true;
        }
    }
    return false;
}

/* ── Bundles: what is deployed together (IDENTITY.md) ────────────── */

/* A Service selects pods of its own deployment, never a same-labelled copy
 * elsewhere in the repository. A bundle is a kustomize build root (a
 * kustomization no other one includes) with every manifest its entries
 * reach; outside kustomize, the Helm chart a file sits in, else its
 * directory. A base several overlays include belongs to each of them. */

typedef struct {
    char *path; /* the kustomization file */
    char *ns;   /* its `namespace:`, NULL when unset */
    int *kids;  /* kustomizations its entries name (indices) */
    int kid_n;
    int kid_cap;
    const char **files; /* manifests its entries name (graph-owned paths) */
    int file_n;
    int file_cap;
    bool referenced; /* included by another kustomization */
    bool component;  /* `kind: Component`: patches the build including it */
    char *prefix;    /* its `namePrefix:`, NULL when unset */
    char *suffix;    /* its `nameSuffix:`, NULL when unset */
    int rename_line; /* the line of its namePrefix, else nameSuffix; 0: neither */
} k8s_kust_t;

typedef struct {
    k8s_kust_t *items;
    int count;
    int cap;
} k8s_kusts_t;

/* One bundle a manifest belongs to, with the namespace the bundle gives
 * its objects (NULL: their own). `bundle` is an interned name, compared by
 * pointer. */
typedef struct {
    const char *bundle;
    const char *ns;
} k8s_member_t;

typedef struct {
    k8s_member_t *items;
    int count;
    int cap;
} k8s_members_t;

typedef struct {
    CBMHashTable *names;   /* bundle name -> itself (owned key) */
    CBMHashTable *members; /* manifest path -> k8s_members_t* */
    CBMHashTable *charts;  /* Helm chart directory -> itself (owned key) */
    CBMHashTable *deploys; /* manifest path -> k8s_members_t* of its deploy scopes */
} k8s_bundles_t;

static bool k8s_grow(void **items, int *cap, int count, size_t elem) {
    if (count < *cap) {
        return true;
    }
    int ncap = *cap ? *cap * PAIR_LEN : K8S_GROW_MIN;
    void *grown = cbm_realloc(CBM_MEM_CLASS_OTHER, *items, (size_t)ncap * elem);
    if (!grown) {
        return false;
    }
    *items = grown;
    *cap = ncap;
    return true;
}

/* The value of a kustomization's top-level `key:` line into `val`, its
 * 1-based line into `*line` (when not NULL); false when it has none, or an
 * empty one. */
static bool k8s_kustomize_top_at(const char *source, const char *key, char *val, size_t val_sz,
                                 int *line) {
    char head[CBM_SZ_64];
    int hl = snprintf(head, sizeof(head), "%s:", key);
    int at = SKIP_ONE;
    for (const char *p = source; p && *p && hl > 0; at++) {
        const char *eol = strchr(p, '\n');
        size_t len = eol ? (size_t)(eol - p) : strlen(p);
        if (strncmp(p, head, (size_t)hl) == 0) {
            char text[CBM_SZ_512];
            snprintf(text, sizeof(text), "%.*s", (int)len, p);
            char k[CBM_SZ_64];
            if (line) {
                *line = at;
            }
            return k8s_split_kv(text, k, sizeof(k), val, val_sz) && val[0];
        }
        p = eol ? eol + SKIP_ONE : NULL;
    }
    return false;
}

static bool k8s_kustomize_top(const char *source, const char *key, char *val, size_t val_sz) {
    return k8s_kustomize_top_at(source, key, val, val_sz, NULL);
}

/* The top-level `namespace:` of a kustomization, NULL when it sets none. */
static char *k8s_kustomize_namespace(const char *source) {
    char val[K8S_LABEL_LEN];
    return k8s_kustomize_top(source, "namespace", val, sizeof(val))
               ? cbm_mem_strdup(CBM_MEM_CLASS_OTHER, val)
               : NULL;
}

/* A kustomize component (`kind: Component`), whose patches act on the build
 * of each kustomization including it. */
static bool k8s_kustomize_component(const char *source) {
    char val[K8S_LABEL_LEN];
    return k8s_kustomize_top(source, "kind", val, sizeof(val)) && strcmp(val, "Component") == 0;
}

/* A kustomization's `namePrefix:` / `nameSuffix:` (`key`), NULL when it
 * sets none -- or one with `/` or a blank in it, which is no name part; its
 * line into `*line` when that is still 0. */
static char *k8s_kustomize_name_part(const char *source, const char *key, int *line) {
    char val[K8S_LABEL_LEN];
    int at = 0;
    if (!k8s_kustomize_top_at(source, key, val, sizeof(val), &at) || strpbrk(val, "/ \t")) {
        return NULL;
    }
    if (*line == 0) {
        *line = at;
    }
    return cbm_mem_strdup(CBM_MEM_CLASS_OTHER, val);
}

static void k8s_kusts_push(k8s_kusts_t *ks, const char *path, const char *source) {
    if (!k8s_grow((void **)&ks->items, &ks->cap, ks->count, sizeof(*ks->items))) {
        return;
    }
    k8s_kust_t k = {.path = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, path),
                    .ns = k8s_kustomize_namespace(source),
                    .component = k8s_kustomize_component(source)};
    k.prefix = k8s_kustomize_name_part(source, "namePrefix", &k.rename_line);
    k.suffix = k8s_kustomize_name_part(source, "nameSuffix", &k.rename_line);
    ks->items[ks->count++] = k;
}

static void k8s_kusts_free(k8s_kusts_t *ks) {
    for (int i = 0; i < ks->count; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, ks->items[i].path);
        cbm_free(CBM_MEM_CLASS_OTHER, ks->items[i].ns);
        cbm_free(CBM_MEM_CLASS_OTHER, ks->items[i].prefix);
        cbm_free(CBM_MEM_CLASS_OTHER, ks->items[i].suffix);
        cbm_free(CBM_MEM_CLASS_OTHER, ks->items[i].kids);
        cbm_free(CBM_MEM_CLASS_OTHER, (void *)ks->items[i].files);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, ks->items);
    *ks = (k8s_kusts_t){0};
}

/* qsort for this file's vectors. An empty vector's items is NULL, and glibc
 * declares qsort's base nonnull, so sorting it is undefined even with n == 0
 * (UBSan nonnull-attribute on Linux). Fewer than two elements need no sort. */
static void k8s_qsort(void *base, size_t n, size_t size, int (*cmp)(const void *, const void *)) {
    if (base && n > 1) {
        qsort(base, n, size, cmp);
    }
}

static int k8s_cmp_str(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

static int k8s_cmp_int(const void *a, const void *b) {
    int x = *(const int *)a;
    int y = *(const int *)b;
    return (x > y) - (x < y);
}

static int k8s_cmp_kust(const void *a, const void *b) {
    return strcmp(((const k8s_kust_t *)a)->path, ((const k8s_kust_t *)b)->path);
}

/* index + 1 of the kustomization at `path`, 0 when none. */
static int k8s_kust_at(CBMHashTable *by_path, const char *path) {
    intptr_t at = (intptr_t)cbm_ht_get(by_path, path);
    return (int)at;
}

/* The kustomization file inside directory `dir`, as kustomize looks for it. */
static int k8s_kust_in_dir(CBMHashTable *by_path, const char *dir) {
    static const char *const names[] = {"kustomization.yaml", "kustomization.yml", "Kustomization"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        char path[CBM_SZ_1K];
        snprintf(path, sizeof(path), "%s%s%s", dir, dir[0] ? "/" : "", names[i]);
        int k = k8s_kust_at(by_path, path);
        if (k) {
            return k;
        }
    }
    return 0;
}

/* Wire each kustomization to what its entries name, from the IMPORTS edges
 * the kustomize resolver drew: a directory means the kustomization in it,
 * a file another kustomization or a manifest. */
static void k8s_kusts_wire(cbm_pipeline_ctx_t *ctx, k8s_kusts_t *ks, CBMHashTable *by_path) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    if (cbm_gbuf_find_edges_by_type(ctx->gbuf, "IMPORTS", &edges, &count) != 0) {
        return;
    }
    for (int i = 0; i < count; i++) {
        const cbm_gbuf_node_t *src = cbm_gbuf_find_by_id(ctx->gbuf, edges[i]->source_id);
        int k = src && src->file_path ? k8s_kust_at(by_path, src->file_path) : 0;
        const cbm_gbuf_node_t *tgt = k ? cbm_gbuf_find_by_id(ctx->gbuf, edges[i]->target_id) : NULL;
        if (!tgt || !tgt->file_path) {
            continue;
        }
        k8s_kust_t *kust = &ks->items[k - SKIP_ONE];
        /* a patch file (`patches`, `patchesStrategicMerge`) is no member of
         * the build: its documents change objects, they are none */
        const char *props = edges[i]->properties_json;
        if (props && strstr(props, "\"local_name\":\"patches")) {
            continue;
        }
        int kid = tgt->label && strcmp(tgt->label, "Folder") == 0
                      ? k8s_kust_in_dir(by_path, tgt->file_path)
                      : k8s_kust_at(by_path, tgt->file_path);
        if (kid && kid != k) {
            if (k8s_grow((void **)&kust->kids, &kust->kid_cap, kust->kid_n, sizeof(int))) {
                kust->kids[kust->kid_n++] = kid - SKIP_ONE;
                ks->items[kid - SKIP_ONE].referenced = true;
            }
        } else if (!kid && strcmp(tgt->label ? tgt->label : "", "Folder") != 0) {
            if (k8s_grow((void **)&kust->files, &kust->file_cap, kust->file_n,
                         sizeof(*kust->files))) {
                kust->files[kust->file_n++] = tgt->file_path;
            }
        }
    }
    for (int i = 0; i < ks->count; i++) {
        k8s_qsort(ks->items[i].files, (size_t)ks->items[i].file_n, sizeof(*ks->items[i].files),
                  k8s_cmp_str);
        k8s_qsort(ks->items[i].kids, (size_t)ks->items[i].kid_n, sizeof(int), k8s_cmp_int);
    }
}

static const char *k8s_bundle_name(k8s_bundles_t *b, const char *kind, const char *where) {
    char name[CBM_SZ_1K];
    snprintf(name, sizeof(name), "%s:%s", kind, where);
    const char *known = cbm_ht_get_key(b->names, name);
    if (known) {
        return known;
    }
    char *owned = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, name);
    if (owned) {
        cbm_ht_set(b->names, owned, owned);
    }
    return owned;
}

static void k8s_member_add(CBMHashTable *table, const char *file, const char *bundle,
                           const char *ns) {
    k8s_members_t *m = cbm_ht_get(table, file);
    if (!m) {
        m = cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*m));
        if (!m) {
            return;
        }
        cbm_ht_set(table, file, m);
    }
    for (int i = 0; i < m->count; i++) {
        if (m->items[i].bundle == bundle) {
            return; /* reached twice from one root: the first path's namespace */
        }
    }
    if (k8s_grow((void **)&m->items, &m->cap, m->count, sizeof(*m->items))) {
        m->items[m->count++] = (k8s_member_t){bundle, ns};
    }
}

/* Every build root's manifests, each with the namespace the outermost
 * kustomization on its path sets (kustomize applies the root's namespace
 * transformer last, so it wins). */
static void k8s_bundles_from_kusts(k8s_bundles_t *b, k8s_kusts_t *ks) {
    typedef struct {
        int kust;
        const char *ns;
    } frame_t;
    int *seen =
        cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)(ks->count ? ks->count : SKIP_ONE) * sizeof(int));
    frame_t *stack = NULL;
    int depth = 0;
    int cap = 0;
    for (int r = 0; seen && r < ks->count; r++) {
        if (ks->items[r].referenced) {
            continue;
        }
        const char *bundle = k8s_bundle_name(b, "kustomize", ks->items[r].path);
        if (!bundle || !k8s_grow((void **)&stack, &cap, depth, sizeof(*stack))) {
            continue;
        }
        stack[depth++] = (frame_t){r, ks->items[r].ns};
        while (depth > 0) {
            frame_t f = stack[--depth];
            if (seen[f.kust] == r + SKIP_ONE) {
                continue;
            }
            seen[f.kust] = r + SKIP_ONE;
            const k8s_kust_t *k = &ks->items[f.kust];
            for (int i = 0; i < k->file_n; i++) {
                k8s_member_add(b->members, k->files[i], bundle, f.ns);
            }
            for (int i = k->kid_n - SKIP_ONE; i >= 0; i--) {
                if (k8s_grow((void **)&stack, &cap, depth, sizeof(*stack))) {
                    int kid = k->kids[i];
                    stack[depth++] = (frame_t){kid, f.ns ? f.ns : ks->items[kid].ns};
                }
            }
        }
    }
    cbm_free(CBM_MEM_CLASS_OTHER, stack);
    cbm_free(CBM_MEM_CLASS_OTHER, seen);
}

static void k8s_dirname(const char *path, char *out, size_t out_sz) {
    const char *slash = strrchr(path, '/');
    snprintf(out, out_sz, "%.*s", slash ? (int)(slash - path) : 0, path);
}

/* The bundles of the manifest at `file`: its kustomize build roots; else
 * the Helm chart it sits in; else its directory. */
static const k8s_members_t *k8s_bundles_of(k8s_bundles_t *b, const char *file,
                                           k8s_members_t *single) {
    const k8s_members_t *m = cbm_ht_get(b->members, file);
    if (m && m->count > 0) {
        return m;
    }
    char dir[CBM_SZ_1K];
    k8s_dirname(file, dir, sizeof(dir));
    char up[CBM_SZ_1K];
    snprintf(up, sizeof(up), "%s", dir);
    const char *bundle = NULL;
    while (!bundle) {
        if (cbm_ht_get(b->charts, up)) {
            bundle = k8s_bundle_name(b, "chart", up);
            break;
        }
        if (!up[0]) {
            break;
        }
        char parent[CBM_SZ_1K];
        k8s_dirname(up, parent, sizeof(parent));
        snprintf(up, sizeof(up), "%s", parent);
    }
    if (!bundle) {
        bundle = k8s_bundle_name(b, "dir", dir);
    }
    single->items[0] = (k8s_member_t){bundle, NULL};
    single->count = bundle ? SKIP_ONE : 0;
    return single;
}

static void k8s_free_owned_key(const char *key, void *value, void *ud) {
    (void)value;
    (void)ud;
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)key);
}

static void k8s_free_members(const char *key, void *value, void *ud) {
    (void)key;
    (void)ud;
    k8s_members_t *m = value;
    cbm_free(CBM_MEM_CLASS_OTHER, m->items);
    cbm_free(CBM_MEM_CLASS_OTHER, m);
}

static void k8s_bundles_free(k8s_bundles_t *b) {
    if (b->members) {
        cbm_ht_foreach(b->members, k8s_free_members, NULL);
        cbm_ht_free(b->members);
    }
    if (b->deploys) {
        cbm_ht_foreach(b->deploys, k8s_free_members, NULL);
        cbm_ht_free(b->deploys);
    }
    if (b->names) {
        cbm_ht_foreach(b->names, k8s_free_owned_key, NULL);
        cbm_ht_free(b->names);
    }
    if (b->charts) {
        cbm_ht_foreach(b->charts, k8s_free_owned_key, NULL);
        cbm_ht_free(b->charts);
    }
    *b = (k8s_bundles_t){0};
}

/* A Helm chart's directory bundles the plain manifests below it. */
static void k8s_chart_dir_register(k8s_bundles_t *b, const char *dir) {
    if (b->charts && !cbm_ht_get(b->charts, dir)) {
        char *owned = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, dir);
        if (owned) {
            cbm_ht_set(b->charts, owned, owned);
        }
    }
}

static void k8s_chart_dir_add(k8s_bundles_t *b, const char *chart_yaml) {
    char dir[CBM_SZ_1K];
    k8s_dirname(chart_yaml, dir, sizeof(dir));
    k8s_chart_dir_register(b, dir);
}

/* Build the kustomize bundles (call once every manifest and kustomization is
 * known and the IMPORTS edges exist). */
static void k8s_bundles_build(cbm_pipeline_ctx_t *ctx, k8s_kusts_t *ks, k8s_bundles_t *b) {
    b->names = cbm_ht_create(0);
    b->members = cbm_ht_create(0);
    if (!b->charts) {
        b->charts = cbm_ht_create(0);
    }
    if (!b->names || !b->members || !b->charts) {
        return;
    }
    k8s_qsort(ks->items, (size_t)ks->count, sizeof(*ks->items), k8s_cmp_kust);
    CBMHashTable *by_path = cbm_ht_create(0);
    if (!by_path) {
        return;
    }
    for (int i = 0; i < ks->count; i++) {
        cbm_ht_set(by_path, ks->items[i].path, (void *)(intptr_t)(i + SKIP_ONE));
    }
    k8s_kusts_wire(ctx, ks, by_path);
    cbm_ht_free(by_path);
    k8s_bundles_from_kusts(b, ks);
}

/* A Service and a workload meet when they share a bundle whose namespaces
 * agree: two set namespaces must be equal; an unset one is the deploy's. */
static bool k8s_share_bundle(k8s_bundles_t *b, const cbm_pipeline_ctx_t *ctx,
                             const k8s_record_t *svc, const k8s_record_t *wl) {
    const cbm_gbuf_node_t *sn = cbm_gbuf_find_by_id(ctx->gbuf, svc->node_id);
    const cbm_gbuf_node_t *wn = cbm_gbuf_find_by_id(ctx->gbuf, wl->node_id);
    if (!sn || !wn || !sn->file_path || !wn->file_path) {
        return false;
    }
    k8s_member_t one_s[SKIP_ONE];
    k8s_member_t one_w[SKIP_ONE];
    k8s_members_t single_s = {.items = one_s, .cap = SKIP_ONE};
    k8s_members_t single_w = {.items = one_w, .cap = SKIP_ONE};
    const k8s_members_t *ms = k8s_bundles_of(b, sn->file_path, &single_s);
    const k8s_members_t *mw = k8s_bundles_of(b, wn->file_path, &single_w);
    for (int i = 0; i < ms->count; i++) {
        for (int j = 0; j < mw->count; j++) {
            if (ms->items[i].bundle != mw->items[j].bundle) {
                continue;
            }
            const char *ns_s = ms->items[i].ns ? ms->items[i].ns : svc->ns;
            const char *ns_w = mw->items[j].ns ? mw->items[j].ns : wl->ns;
            if (!ns_s[0] || !ns_w[0] || strcmp(ns_s, ns_w) == 0) {
                return true;
            }
        }
    }
    return false;
}

/* The declared namespace inside a Resource QN's key ("" when `-`). */
static void k8s_declared_ns(const char *qn, char *out, size_t out_sz) {
    out[0] = '\0';
    const char *key = qn ? strstr(qn, "::") : NULL;
    const char *p = key ? key + K8S_SLEN("::") : NULL;
    for (int part = 0; p && part < PAIR_LEN; part++) {
        p = strchr(p, '/');
        p = p ? p + SKIP_ONE : NULL;
    }
    const char *end = p ? strchr(p, '/') : NULL;
    if (end && !(end - p == SKIP_ONE && p[0] == '-')) {
        snprintf(out, out_sz, "%.*s", (int)(end - p), p);
    }
}

/* Objects a kustomize build deploys live in the namespace it gives them:
 * when every bundle of the file agrees on one, that is the namespace of
 * the object's logical key; bundles that disagree leave it to the deploy. */
static void k8s_apply_effective_namespaces(cbm_pipeline_ctx_t *ctx, k8s_bundles_t *b) {
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (!b->members || cbm_gbuf_find_by_label(ctx->gbuf, "Resource", &nodes, &count) != 0) {
        return;
    }
    for (int i = 0; i < count; i++) {
        const cbm_gbuf_node_t *n = nodes[i];
        const k8s_members_t *m = n->file_path ? cbm_ht_get(b->members, n->file_path) : NULL;
        if (!m || m->count == 0) {
            continue;
        }
        char declared[K8S_LABEL_LEN];
        k8s_declared_ns(n->qualified_name, declared, sizeof(declared));
        const char *eff = NULL;
        bool agree = true;
        bool any_set = false;
        for (int j = 0; j < m->count; j++) {
            const char *v = m->items[j].ns ? m->items[j].ns : declared;
            any_set = any_set || m->items[j].ns;
            if (!v[0] || (eff && strcmp(eff, v) != 0)) {
                agree = false;
            }
            eff = eff ? eff : v;
        }
        if (!any_set) {
            continue; /* no build sets a namespace: the declared one stands */
        }
        char version[K8S_LABEL_LEN];
        char ecosystem[K8S_LABEL_LEN];
        k8s_node_prop(n, "version", version, sizeof(version));
        k8s_node_prop(n, "ecosystem", ecosystem, sizeof(ecosystem));
        char *labels = k8s_node_prop_dup(n, "labels");
        char *props =
            k8s_resource_props(n->qualified_name, agree ? eff : "?", version, labels, ecosystem);
        cbm_free(CBM_MEM_CLASS_OTHER, labels);
        if (props) {
            (void)cbm_gbuf_node_set_properties_json((cbm_gbuf_node_t *)n, props);
            cbm_free(CBM_MEM_CLASS_OTHER, props);
        }
    }
}

/* Candidates examined by the resolvers below (pipeline_internal.h). */
_Atomic uint64_t g_k8s_candidates = 0;

/* ── Object references (PHASE1.md) ───────────────────────────────── */

/* A reference fact of a manifest, kept past its file's extraction. */
typedef struct {
    int64_t src;      /* the referencing Resource */
    char *kind;       /* target kind */
    char *ns;         /* explicit target namespace, NULL = the source's */
    char *name;       /* target metadata.name */
    const char *via;  /* static: the extractor's table */
    const char *edge; /* static: "USAGE", "INFRA_MAPS", "DEPENDS_ON" or "CONNECTS_TO" */
    char *route;      /* an exposed route "<host><path>" the edge leaves from, NULL = src */
    char *key;        /* CONNECTS_TO: the environment variable addressing the target */
    char *container;  /* CONNECTS_TO: its container, NULL = unnamed */
} k8s_ref_t;

/* A CRD and the group/kind it declares. */
typedef struct {
    int64_t node;
    char *group;
    char *kind;
} k8s_crd_t;

/* An object that deploys a repository directory (Flux Kustomization, Argo
 * Application). */
typedef struct {
    int64_t node;
    char *dir;       /* normalised, "" = the repository root */
    char *ns;        /* the namespace it deploys into, NULL = the objects' own */
    const char *via; /* static: "flux" / "argo" */
} k8s_deploy_t;

/* A HelmRelease and the chart it deploys: a repository path (Git / Bucket
 * source), else a chart name of a Helm repository. */
typedef struct {
    int64_t node;
    char *chart;
    char *source; /* the sourceRef kind, NULL when unset */
} k8s_hr_t;

/* An environment variable an object's container sets (never its value). */
typedef struct {
    int64_t node;
    char *key;
    char *container;    /* NULL: unnamed */
    const char *source; /* static: "value" or the valueFrom kind */
    int line;
} k8s_env_t;

/* A container image an object runs, and the container running it. */
typedef struct {
    int64_t node;
    char *image;
    char *container; /* NULL: unnamed */
    int line;        /* the image's line */
    int start_line;  /* the container's lines */
    int end_line;
} k8s_img_t;

/* A patch a kustomization states (extract_k8s.c PATCH fact): what it names
 * in the kustomization's build. */
typedef struct {
    char *kust;    /* the kustomization file */
    char *path;    /* the patch file (repository path), NULL: inline */
    char *kind;    /* kind / group / version / name / ns named, NULL: any; */
    char *group;   /*   kind and name both NULL: the documents of the */
    char *version; /*   patch file name them */
    char *name;
    char *ns;
    char *selector; /* labelSelector, NULL: none */
    int line;
} k8s_patch_t;

/* A chart a kustomization inflates into its build (`helmCharts`). */
typedef struct {
    char *kust;    /* the kustomization file */
    char *chart;   /* the chart name */
    char *url;     /* its repository, NULL when unset */
    char *version; /* NULL when unset */
    char *release; /* the release name, NULL when unset */
    char *ns;      /* the release namespace, NULL when unset */
    char *values;  /* the values file (repository path), NULL when none */
    int line;
} k8s_kchart_t;

typedef struct {
    k8s_ref_t *refs;
    int ref_n;
    int ref_cap;
    k8s_crd_t *crds;
    int crd_n;
    int crd_cap;
    k8s_deploy_t *deploys;
    int dep_n;
    int dep_cap;
    k8s_hr_t *hrs;
    int hr_n;
    int hr_cap;
    k8s_hr_t *srcs; /* an OCIRepository (node) and the URL it fetches (chart) */
    int src_n;
    int src_cap;
    k8s_img_t *imgs;
    int img_n;
    int img_cap;
    k8s_env_t *envs;
    int env_n;
    int env_cap;
    k8s_patch_t *patches;
    int patch_n;
    int patch_cap;
    k8s_kchart_t *kcharts;
    int kchart_n;
    int kchart_cap;
} k8s_facts_t;

static void k8s_facts_free(k8s_facts_t *f) {
    for (int i = 0; i < f->ref_n; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, f->refs[i].kind);
        cbm_free(CBM_MEM_CLASS_OTHER, f->refs[i].ns);
        cbm_free(CBM_MEM_CLASS_OTHER, f->refs[i].name);
        cbm_free(CBM_MEM_CLASS_OTHER, f->refs[i].route);
        cbm_free(CBM_MEM_CLASS_OTHER, f->refs[i].key);
        cbm_free(CBM_MEM_CLASS_OTHER, f->refs[i].container);
    }
    for (int i = 0; i < f->crd_n; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, f->crds[i].group);
        cbm_free(CBM_MEM_CLASS_OTHER, f->crds[i].kind);
    }
    for (int i = 0; i < f->dep_n; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, f->deploys[i].dir);
        cbm_free(CBM_MEM_CLASS_OTHER, f->deploys[i].ns);
    }
    for (int i = 0; i < f->hr_n; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, f->hrs[i].chart);
        cbm_free(CBM_MEM_CLASS_OTHER, f->hrs[i].source);
    }
    for (int i = 0; i < f->src_n; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, f->srcs[i].chart);
    }
    for (int i = 0; i < f->img_n; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, f->imgs[i].image);
        cbm_free(CBM_MEM_CLASS_OTHER, f->imgs[i].container);
    }
    for (int i = 0; i < f->env_n; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, f->envs[i].key);
        cbm_free(CBM_MEM_CLASS_OTHER, f->envs[i].container);
    }
    for (int i = 0; i < f->patch_n; i++) {
        k8s_patch_t *p = &f->patches[i];
        cbm_free(CBM_MEM_CLASS_OTHER, p->kust);
        cbm_free(CBM_MEM_CLASS_OTHER, p->path);
        cbm_free(CBM_MEM_CLASS_OTHER, p->kind);
        cbm_free(CBM_MEM_CLASS_OTHER, p->group);
        cbm_free(CBM_MEM_CLASS_OTHER, p->version);
        cbm_free(CBM_MEM_CLASS_OTHER, p->name);
        cbm_free(CBM_MEM_CLASS_OTHER, p->ns);
        cbm_free(CBM_MEM_CLASS_OTHER, p->selector);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, f->patches);
    for (int i = 0; i < f->kchart_n; i++) {
        k8s_kchart_t *c = &f->kcharts[i];
        cbm_free(CBM_MEM_CLASS_OTHER, c->kust);
        cbm_free(CBM_MEM_CLASS_OTHER, c->chart);
        cbm_free(CBM_MEM_CLASS_OTHER, c->url);
        cbm_free(CBM_MEM_CLASS_OTHER, c->version);
        cbm_free(CBM_MEM_CLASS_OTHER, c->release);
        cbm_free(CBM_MEM_CLASS_OTHER, c->ns);
        cbm_free(CBM_MEM_CLASS_OTHER, c->values);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, f->kcharts);
    cbm_free(CBM_MEM_CLASS_OTHER, f->envs);
    cbm_free(CBM_MEM_CLASS_OTHER, f->hrs);
    cbm_free(CBM_MEM_CLASS_OTHER, f->srcs);
    cbm_free(CBM_MEM_CLASS_OTHER, f->imgs);
    cbm_free(CBM_MEM_CLASS_OTHER, f->refs);
    cbm_free(CBM_MEM_CLASS_OTHER, f->crds);
    cbm_free(CBM_MEM_CLASS_OTHER, f->deploys);
    *f = (k8s_facts_t){0};
}

/* A deploy path as a repository directory: "./a//b/" -> "a/b", "." -> "";
 * false for URLs, templates, variables and paths leaving the repository. */
static bool k8s_norm_dir(const char *in, char *out, size_t out_sz) {
    if (!in || strstr(in, "://") || strstr(in, "{{") || strchr(in, '$') || out_sz == 0) {
        return false;
    }
    size_t n = 0;
    out[0] = '\0';
    for (const char *p = in; *p;) {
        while (*p == '/') {
            p++;
        }
        size_t sl = strcspn(p, "/");
        if (sl == 0) {
            break;
        }
        if (sl == SKIP_ONE && p[0] == '.') {
            p += sl;
            continue;
        }
        if (sl == PAIR_LEN && p[0] == '.' && p[SKIP_ONE] == '.') {
            if (n == 0) {
                return false;
            }
            char *slash = strrchr(out, '/');
            n = slash ? (size_t)(slash - out) : 0;
            out[n] = '\0';
            p += sl;
            continue;
        }
        if (n + SKIP_ONE + sl >= out_sz) {
            return false;
        }
        if (n) {
            out[n++] = '/';
        }
        memcpy(out + n, p, sl);
        n += sl;
        out[n] = '\0';
        p += sl;
    }
    return true;
}

/* Keep a file's facts, its def indices mapped to the Resource nodes. */
static void k8s_facts_keep(k8s_facts_t *f, const CBMFileResult *res, const int64_t *node_of) {
    for (int i = 0; i < res->k8s_facts.count; i++) {
        const CBMK8sFact *x = &res->k8s_facts.items[i];
        if (x->def_index < 0 || x->def_index >= res->defs.count || node_of[x->def_index] <= 0) {
            continue;
        }
        int64_t node = node_of[x->def_index];
        if (x->fact == CBM_K8S_FACT_DEPLOY && x->via && strcmp(x->via, "helmrelease") == 0) {
            if (x->name && k8s_grow((void **)&f->hrs, &f->hr_cap, f->hr_n, sizeof(*f->hrs))) {
                f->hrs[f->hr_n++] =
                    (k8s_hr_t){node, cbm_mem_strdup(CBM_MEM_CLASS_OTHER, x->name),
                               x->kind ? cbm_mem_strdup(CBM_MEM_CLASS_OTHER, x->kind) : NULL};
            }
        } else if (x->fact == CBM_K8S_FACT_DEPLOY) {
            char dir[CBM_SZ_1K];
            if (k8s_norm_dir(x->name, dir, sizeof(dir)) &&
                k8s_grow((void **)&f->deploys, &f->dep_cap, f->dep_n, sizeof(*f->deploys))) {
                f->deploys[f->dep_n++] = (k8s_deploy_t){
                    node, cbm_mem_strdup(CBM_MEM_CLASS_OTHER, dir),
                    x->ns ? cbm_mem_strdup(CBM_MEM_CLASS_OTHER, x->ns) : NULL, x->via};
            }
        } else if (x->fact == CBM_K8S_FACT_SOURCE) {
            if (x->name && k8s_grow((void **)&f->srcs, &f->src_cap, f->src_n, sizeof(*f->srcs))) {
                f->srcs[f->src_n++] =
                    (k8s_hr_t){node, cbm_mem_strdup(CBM_MEM_CLASS_OTHER, x->name), NULL};
            }
        } else if (x->fact == CBM_K8S_FACT_IMAGE) {
            if (x->name && k8s_grow((void **)&f->imgs, &f->img_cap, f->img_n, sizeof(*f->imgs))) {
                f->imgs[f->img_n++] = (k8s_img_t){
                    node,
                    cbm_mem_strdup(CBM_MEM_CLASS_OTHER, x->name),
                    x->container ? cbm_mem_strdup(CBM_MEM_CLASS_OTHER, x->container) : NULL,
                    x->line,
                    x->start_line,
                    x->end_line};
            }
        } else if (x->fact == CBM_K8S_FACT_ENV) {
            if (x->name && k8s_grow((void **)&f->envs, &f->env_cap, f->env_n, sizeof(*f->envs))) {
                f->envs[f->env_n++] = (k8s_env_t){
                    node, cbm_mem_strdup(CBM_MEM_CLASS_OTHER, x->name),
                    x->container ? cbm_mem_strdup(CBM_MEM_CLASS_OTHER, x->container) : NULL, x->via,
                    x->line};
            }
        } else if (x->fact == CBM_K8S_FACT_CRD) {
            if (k8s_grow((void **)&f->crds, &f->crd_cap, f->crd_n, sizeof(*f->crds))) {
                f->crds[f->crd_n++] =
                    (k8s_crd_t){node, cbm_mem_strdup(CBM_MEM_CLASS_OTHER, x->name),
                                cbm_mem_strdup(CBM_MEM_CLASS_OTHER, x->kind)};
            }
        } else if (k8s_grow((void **)&f->refs, &f->ref_cap, f->ref_n, sizeof(*f->refs))) {
            bool route = x->fact == CBM_K8S_FACT_ROUTE && x->route;
            f->refs[f->ref_n++] =
                (k8s_ref_t){.src = node,
                            .kind = x->kind ? cbm_mem_strdup(CBM_MEM_CLASS_OTHER, x->kind) : NULL,
                            .ns = x->ns ? cbm_mem_strdup(CBM_MEM_CLASS_OTHER, x->ns) : NULL,
                            .name = x->name ? cbm_mem_strdup(CBM_MEM_CLASS_OTHER, x->name) : NULL,
                            .via = x->via,
                            .edge = x->edge,
                            .route = route ? cbm_mem_strdup(CBM_MEM_CLASS_OTHER, x->route) : NULL,
                            .key = x->key ? cbm_mem_strdup(CBM_MEM_CLASS_OTHER, x->key) : NULL,
                            .container = x->key && x->container
                                             ? cbm_mem_strdup(CBM_MEM_CLASS_OTHER, x->container)
                                             : NULL};
        }
    }
}

/* ── Deploy scopes (PHASE1 P1.3) ───────────────────────────────────── */

/* A Flux Kustomization or Argo Application deploys a repository directory:
 * the build of a kustomization there, else every manifest below it, a
 * sub-directory's kustomization by its build (as Flux generates one). The
 * objects a root deploy reaches through such directories -- and the deploys
 * among them, transitively -- form one deploy scope, a cluster: references
 * meet across kustomize roots inside it (a Kustomization's dependsOn, a
 * Certificate's ClusterIssuer). A path that names no directory of the
 * repository (another source's layout) deploys nothing. */

typedef struct {
    const char **items; /* graph-owned paths */
    int count;
    int cap;
} k8s_paths_t;

static void k8s_paths_push(k8s_paths_t *v, const char *p) {
    if (k8s_grow((void **)&v->items, &v->cap, v->count, sizeof(*v->items))) {
        v->items[v->count++] = p;
    }
}

static void k8s_paths_sort_unique(k8s_paths_t *v) {
    if (v->count == 0) {
        return;
    }
    k8s_qsort((void *)v->items, (size_t)v->count, sizeof(*v->items), k8s_cmp_str);
    int w = SKIP_ONE;
    for (int i = SKIP_ONE; i < v->count; i++) {
        if (strcmp(v->items[w - SKIP_ONE], v->items[i]) != 0) {
            v->items[w++] = v->items[i];
        }
    }
    v->count = w;
}

/* The manifests the build of kustomization `root` reaches; `seen` marks the
 * kustomizations visited under `mark`. */
static void k8s_kust_closure(const k8s_kusts_t *ks, int root, int *seen, int mark,
                             k8s_paths_t *out) {
    int *stack = NULL;
    int depth = 0;
    int cap = 0;
    if (!k8s_grow((void **)&stack, &cap, depth, sizeof(int))) {
        return;
    }
    stack[depth++] = root;
    while (depth > 0) {
        int k = stack[--depth];
        if (seen[k] == mark) {
            continue;
        }
        seen[k] = mark;
        for (int i = 0; i < ks->items[k].file_n; i++) {
            k8s_paths_push(out, ks->items[k].files[i]);
        }
        for (int i = 0; i < ks->items[k].kid_n; i++) {
            if (k8s_grow((void **)&stack, &cap, depth, sizeof(int))) {
                stack[depth++] = ks->items[k].kids[i];
            }
        }
    }
    cbm_free(CBM_MEM_CLASS_OTHER, stack);
}

/* The manifests a deploy of directory `dir` applies; `manifests` is every
 * manifest path, sorted (a directory's are one range). */
static void k8s_dir_members(const k8s_kusts_t *ks, CBMHashTable *by_path,
                            const k8s_paths_t *manifests, const char *dir, int *seen, int mark,
                            k8s_paths_t *out, k8s_paths_t *units) {
    size_t dl = strlen(dir);
    int lo = 0;
    int hi = dl ? manifests->count : 0;
    while (lo < hi) {
        int mid = lo + (hi - lo) / PAIR_LEN;
        int c = strncmp(manifests->items[mid], dir, dl);
        if (c < 0 || (c == 0 && manifests->items[mid][dl] < '/')) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    for (int i = lo; i < manifests->count; i++) {
        const char *file = manifests->items[i];
        if (dl && (strncmp(file, dir, dl) != 0 || file[dl] != '/')) {
            break;
        }
        /* the outermost kustomization between `dir` and the file applies it */
        char up[CBM_SZ_1K];
        k8s_dirname(file, up, sizeof(up));
        int top = 0;
        for (;;) {
            int k = k8s_kust_in_dir(by_path, up);
            top = k ? k : top;
            if (strlen(up) <= dl) {
                break;
            }
            char parent[CBM_SZ_1K];
            k8s_dirname(up, parent, sizeof(parent));
            snprintf(up, sizeof(up), "%s", parent);
        }
        if (!top) {
            k8s_paths_push(out, file);
            if (units) {
                k8s_paths_push(units, file);
            }
        } else if (seen[top - SKIP_ONE] != mark) {
            if (units) {
                k8s_paths_push(units, ks->items[top - SKIP_ONE].path);
            }
            k8s_kust_closure(ks, top - SKIP_ONE, seen, mark, out);
        }
    }
    k8s_paths_sort_unique(out);
}

/* A deploy's sort key: where it is declared, then which one. */
typedef struct {
    const char *file;
    const char *qn;
    const char *dir;
    int deploy;
} k8s_deploy_key_t;

static int k8s_cmp_deploy_key(const void *a, const void *b) {
    const k8s_deploy_key_t *x = a;
    const k8s_deploy_key_t *y = b;
    int c = strcmp(x->file, y->file);
    c = c ? c : strcmp(x->qn, y->qn);
    c = c ? c : strcmp(x->dir, y->dir);
    return c ? c : (x->deploy > y->deploy) - (x->deploy < y->deploy);
}

/* The deploys declared in `file`: a range of `keys` (sorted by file). */
static int k8s_deploys_in(const k8s_deploy_key_t *keys, int n, const char *file, int *first) {
    int lo = 0;
    int hi = n;
    while (lo < hi) {
        int mid = lo + (hi - lo) / PAIR_LEN;
        if (strcmp(keys[mid].file, file) < 0) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    int end = lo;
    while (end < n && strcmp(keys[end].file, file) == 0) {
        end++;
    }
    *first = lo;
    return end - lo;
}

typedef struct {
    int *items;
    int count;
    int cap;
} k8s_ints_t;

/* Build the deploy scopes (after the kustomize bundles). */
static void k8s_deploys_build(cbm_pipeline_ctx_t *ctx, k8s_kusts_t *ks, k8s_bundles_t *b,
                              const k8s_facts_t *f) {
    int n = f->dep_n;
    const cbm_gbuf_node_t **res = NULL;
    int rn = 0;
    if (n == 0 || !b->names || cbm_gbuf_find_by_label(ctx->gbuf, "Resource", &res, &rn) != 0) {
        return;
    }
    b->deploys = cbm_ht_create(0);
    k8s_paths_t manifests = {0};
    for (int i = 0; i < rn; i++) {
        if (res[i]->file_path) {
            k8s_paths_push(&manifests, res[i]->file_path);
        }
    }
    k8s_paths_sort_unique(&manifests);
    CBMHashTable *by_path = cbm_ht_create(0);
    for (int i = 0; by_path && i < ks->count; i++) {
        cbm_ht_set(by_path, ks->items[i].path, (void *)(intptr_t)(i + SKIP_ONE));
    }
    size_t un = (size_t)n;
    k8s_paths_t *members = cbm_calloc(CBM_MEM_CLASS_OTHER, un * sizeof(*members));
    k8s_ints_t *adj = cbm_calloc(CBM_MEM_CLASS_OTHER, un * sizeof(*adj));
    const cbm_gbuf_node_t **node = cbm_calloc(CBM_MEM_CLASS_OTHER, un * sizeof(*node));
    k8s_deploy_key_t *keys = cbm_calloc(CBM_MEM_CLASS_OTHER, un * sizeof(*keys));
    int *indeg = cbm_calloc(CBM_MEM_CLASS_OTHER, un * sizeof(int));
    int *reached = cbm_calloc(CBM_MEM_CLASS_OTHER, un * sizeof(int));
    int *visit = cbm_calloc(CBM_MEM_CLASS_OTHER, un * sizeof(int));
    int *queue = cbm_calloc(CBM_MEM_CLASS_OTHER, un * sizeof(int));
    int *seen =
        cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)(ks->count ? ks->count : SKIP_ONE) * sizeof(int));
    int roots = 0;
    if (b->deploys && by_path && members && adj && node && keys && indeg && reached && visit &&
        queue && seen) {
        for (int d = 0; d < n; d++) {
            node[d] = cbm_gbuf_find_by_id(ctx->gbuf, f->deploys[d].node);
            keys[d] = (k8s_deploy_key_t){
                node[d] && node[d]->file_path ? node[d]->file_path : "",
                node[d] && node[d]->qualified_name ? node[d]->qualified_name : "",
                f->deploys[d].dir, d};
            k8s_dir_members(ks, by_path, &manifests, f->deploys[d].dir, seen, d + SKIP_ONE,
                            &members[d], NULL);
        }
        k8s_qsort(keys, un, sizeof(*keys), k8s_cmp_deploy_key);
        /* deploy d reaches deploy e when e sits in a manifest d applies */
        for (int d = 0; d < n; d++) {
            for (int i = 0; i < members[d].count; i++) {
                int first = 0;
                int cnt = k8s_deploys_in(keys, n, members[d].items[i], &first);
                for (int j = first; j < first + cnt; j++) {
                    int e = keys[j].deploy;
                    if (e != d &&
                        k8s_grow((void **)&adj[d].items, &adj[d].cap, adj[d].count, sizeof(int))) {
                        adj[d].items[adj[d].count++] = e;
                        indeg[e]++;
                    }
                }
            }
        }
        /* roots: deploys no other reaches; then, in order, any a cycle hides */
        for (int pass = 0; pass < PAIR_LEN; pass++) {
            for (int oi = 0; oi < n; oi++) {
                int r = keys[oi].deploy;
                if (reached[r] || (pass == 0 && indeg[r] != 0)) {
                    continue;
                }
                const char *bundle = k8s_bundle_name(
                    b, "deploy", node[r] ? node[r]->qualified_name : f->deploys[r].dir);
                if (!bundle) {
                    continue;
                }
                roots++;
                int mark = pass * n + oi + SKIP_ONE;
                int qh = 0;
                int qt = 0;
                queue[qt++] = r;
                visit[r] = mark;
                while (qh < qt) {
                    int d = queue[qh++];
                    reached[d] = SKIP_ONE;
                    if (node[d] && node[d]->file_path) {
                        k8s_member_add(b->deploys, node[d]->file_path, bundle, NULL);
                    }
                    for (int i = 0; i < members[d].count; i++) {
                        k8s_member_add(b->deploys, members[d].items[i], bundle, f->deploys[d].ns);
                    }
                    for (int i = 0; i < adj[d].count; i++) {
                        int e = adj[d].items[i];
                        if (visit[e] != mark) {
                            visit[e] = mark;
                            queue[qt++] = e;
                        }
                    }
                }
            }
        }
    }
    for (int d = 0; members && d < n; d++) {
        cbm_free(CBM_MEM_CLASS_OTHER, (void *)members[d].items);
    }
    for (int d = 0; adj && d < n; d++) {
        cbm_free(CBM_MEM_CLASS_OTHER, adj[d].items);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, members);
    cbm_free(CBM_MEM_CLASS_OTHER, adj);
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)node);
    cbm_free(CBM_MEM_CLASS_OTHER, keys);
    cbm_free(CBM_MEM_CLASS_OTHER, indeg);
    cbm_free(CBM_MEM_CLASS_OTHER, reached);
    cbm_free(CBM_MEM_CLASS_OTHER, visit);
    cbm_free(CBM_MEM_CLASS_OTHER, queue);
    cbm_free(CBM_MEM_CLASS_OTHER, seen);
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)manifests.items);
    if (by_path) {
        cbm_ht_free(by_path);
    }
    cbm_log_info("pass.k8s.deploys", "deploys", itoa_k8s(n), "roots", itoa_k8s(roots));
}

/* ── Bundle-keyed candidate indexes ───────────────────────────────── */

/* Candidates are looked up per bundle the source shares, never by name
 * across the repository: copies of one name in independent bundles would
 * otherwise make every lookup visit every copy (quadratic in copies). */

typedef struct {
    int64_t *items;
    int count;
    int cap;
} k8s_ids_t;

static void k8s_ids_push(k8s_ids_t *v, int64_t id) {
    if (k8s_grow((void **)&v->items, &v->cap, v->count, sizeof(*v->items))) {
        v->items[v->count++] = id;
    }
}

static int k8s_cmp_id(const void *a, const void *b) {
    int64_t x = *(const int64_t *)a;
    int64_t y = *(const int64_t *)b;
    return (x > y) - (x < y);
}

static void k8s_ids_sort_unique(k8s_ids_t *v) {
    if (v->count < PAIR_LEN) {
        return;
    }
    k8s_qsort(v->items, (size_t)v->count, sizeof(*v->items), k8s_cmp_id);
    int w = SKIP_ONE;
    for (int i = SKIP_ONE; i < v->count; i++) {
        if (v->items[i] != v->items[w - SKIP_ONE]) {
            v->items[w++] = v->items[i];
        }
    }
    v->count = w;
}

static void k8s_free_ids_entry(const char *key, void *value, void *ud) {
    (void)ud;
    k8s_ids_t *v = value;
    cbm_free(CBM_MEM_CLASS_OTHER, v->items);
    cbm_free(CBM_MEM_CLASS_OTHER, v);
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)key);
}

/* "<bundle>\x1f<name>": an interned bundle (by address) with an object
 * name, a type or a label pair. */
static void k8s_bkey(char *out, size_t out_sz, const char *bundle, const char *name) {
    snprintf(out, out_sz, "%p\x1f%s", (const void *)bundle, name);
}

static void k8s_index_add(CBMHashTable *ix, const char *key, int64_t id) {
    k8s_ids_t *v = cbm_ht_get(ix, key);
    if (!v) {
        v = cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*v));
        char *owned = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, key);
        if (!v || !owned) {
            cbm_free(CBM_MEM_CLASS_OTHER, v);
            cbm_free(CBM_MEM_CLASS_OTHER, owned);
            return;
        }
        cbm_ht_set(ix, owned, v);
    }
    if (v->count == 0 || v->items[v->count - SKIP_ONE] != id) {
        k8s_ids_push(v, id);
    }
}

/* Calls `fn(bundle, ud)` for every bundle of `file`: its build roots /
 * chart / dir and, when `deploys`, the deploy scopes reaching it. */
typedef void (*k8s_scope_fn)(const char *bundle, void *ud);

static void k8s_each_scope(k8s_bundles_t *b, const char *file, bool deploys, k8s_scope_fn fn,
                           void *ud) {
    k8s_member_t one[SKIP_ONE];
    k8s_members_t single = {.items = one, .cap = SKIP_ONE};
    const k8s_members_t *m = k8s_bundles_of(b, file, &single);
    for (int i = 0; i < m->count; i++) {
        fn(m->items[i].bundle, ud);
    }
    const k8s_members_t *d = deploys && b->deploys ? cbm_ht_get(b->deploys, file) : NULL;
    for (int i = 0; d && i < d->count; i++) {
        fn(d->items[i].bundle, ud);
    }
}

typedef struct {
    CBMHashTable *ix;
    const char *name;
    int64_t id;
    k8s_ids_t *found;
} k8s_scope_job_t;

static void k8s_scope_add(const char *bundle, void *ud) {
    k8s_scope_job_t *j = ud;
    char key[CBM_SZ_1K];
    k8s_bkey(key, sizeof(key), bundle, j->name);
    k8s_index_add(j->ix, key, j->id);
}

static void k8s_scope_find(const char *bundle, void *ud) {
    k8s_scope_job_t *j = ud;
    char key[CBM_SZ_1K];
    k8s_bkey(key, sizeof(key), bundle, j->name);
    const k8s_ids_t *v = cbm_ht_get(j->ix, key);
    for (int i = 0; v && i < v->count; i++) {
        k8s_ids_push(j->found, v->items[i]);
    }
}

/* Index every Resource by "<scope>\x1f<Kind/name>" over its bundles (and
 * deploy scopes when `deploys`). */
static CBMHashTable *k8s_index_objects(cbm_pipeline_ctx_t *ctx, k8s_bundles_t *b, bool deploys) {
    CBMHashTable *ix = cbm_ht_create(0);
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (!ix || cbm_gbuf_find_by_label(ctx->gbuf, "Resource", &nodes, &count) != 0) {
        return ix;
    }
    for (int i = 0; i < count; i++) {
        if (nodes[i]->file_path && nodes[i]->name) {
            k8s_scope_job_t j = {ix, nodes[i]->name, nodes[i]->id, NULL};
            k8s_each_scope(b, nodes[i]->file_path, deploys, k8s_scope_add, &j);
        }
    }
    return ix;
}

/* The ids `name` has in any scope of `file`, sorted, unique. */
static void k8s_find_in_scopes(k8s_bundles_t *b, CBMHashTable *ix, const char *file,
                               const char *name, bool deploys, k8s_ids_t *found) {
    found->count = 0;
    k8s_scope_job_t j = {ix, name, 0, found};
    k8s_each_scope(b, file, deploys, k8s_scope_find, &j);
    k8s_ids_sort_unique(found);
}

/* The namespace of a Resource's logical key ("" when unknown `?` or
 * cluster-scoped `_`), else its declared one. */
static void k8s_effective_ns(const cbm_gbuf_node_t *n, char *out, size_t out_sz) {
    out[0] = '\0';
    const char *k = n->properties_json ? strstr(n->properties_json, "\"obj_key\":\"") : NULL;
    if (!k) {
        k8s_declared_ns(n->qualified_name, out, out_sz);
        return;
    }
    k += K8S_SLEN("\"obj_key\":\"");
    for (int part = 0; k && part < PAIR_LEN; part++) { /* past group and kind */
        k = strchr(k, '/');
        k = k ? k + SKIP_ONE : NULL;
    }
    const char *end = k ? strchr(k, '/') : NULL;
    if (end && !(end - k == SKIP_ONE && (k[0] == '?' || k[0] == '_'))) {
        snprintf(out, out_sz, "%.*s", (int)(end - k), k);
    }
}

/* Does the target at `tgt` answer `ref` from `src`: one bundle, and the
 * namespace the reference asks for (explicit, else the source's own there)
 * agrees with the target's there? A cluster-scoped kind has no namespace. */
static bool k8s_ref_meets(k8s_bundles_t *b, const k8s_ref_t *ref, const cbm_gbuf_node_t *src,
                          const cbm_gbuf_node_t *tgt) {
    k8s_member_t one_s[SKIP_ONE];
    k8s_member_t one_t[SKIP_ONE];
    k8s_members_t single_s = {.items = one_s, .cap = SKIP_ONE};
    k8s_members_t single_t = {.items = one_t, .cap = SKIP_ONE};
    const k8s_members_t *ms = k8s_bundles_of(b, src->file_path, &single_s);
    const k8s_members_t *mt = k8s_bundles_of(b, tgt->file_path, &single_t);
    char src_ns[K8S_LABEL_LEN];
    char tgt_ns[K8S_LABEL_LEN];
    k8s_declared_ns(src->qualified_name, src_ns, sizeof(src_ns));
    k8s_declared_ns(tgt->qualified_name, tgt_ns, sizeof(tgt_ns));
    bool cluster = k8s_cluster_scoped(ref->kind);
    for (int i = 0; i < ms->count; i++) {
        for (int j = 0; j < mt->count; j++) {
            if (ms->items[i].bundle != mt->items[j].bundle) {
                continue;
            }
            const char *want = ref->ns ? ref->ns : ms->items[i].ns ? ms->items[i].ns : src_ns;
            const char *have = mt->items[j].ns ? mt->items[j].ns : tgt_ns;
            if (cluster || !want[0] || !have[0] || strcmp(want, have) == 0) {
                return true;
            }
        }
    }
    /* or one deploy scope: the cluster a Flux / Argo root deploys */
    const k8s_members_t *ds = b->deploys ? cbm_ht_get(b->deploys, src->file_path) : NULL;
    const k8s_members_t *dt = ds ? cbm_ht_get(b->deploys, tgt->file_path) : NULL;
    if (!dt) {
        return false;
    }
    k8s_effective_ns(src, src_ns, sizeof(src_ns));
    k8s_effective_ns(tgt, tgt_ns, sizeof(tgt_ns));
    for (int i = 0; i < ds->count; i++) {
        for (int j = 0; j < dt->count; j++) {
            if (ds->items[i].bundle != dt->items[j].bundle) {
                continue;
            }
            const char *want = ref->ns ? ref->ns : ds->items[i].ns ? ds->items[i].ns : src_ns;
            const char *have = dt->items[j].ns ? dt->items[j].ns : tgt_ns;
            if (cluster || !want[0] || !have[0] || strcmp(want, have) == 0) {
                return true;
            }
        }
    }
    return false;
}

/* The Route node of a route `src` (an Ingress, an HTTPRoute) exposes, which
 * `src` DEFINES; 0 on failure. Its QN is the route itself, so every object
 * exposing one host and path shares it. */
static int64_t k8s_route_node(cbm_pipeline_ctx_t *ctx, const cbm_gbuf_node_t *src,
                              const k8s_ref_t *ref) {
    enum { K8S_ROUTE_ESC = 6 * CBM_SZ_256 };
    const char *slash = strchr(ref->route, '/');
    if (!slash) {
        return 0;
    }
    char qn[CBM_SZ_1K];
    char host[CBM_SZ_256];
    char esc_host[K8S_ROUTE_ESC];
    char esc_path[K8S_ROUTE_ESC];
    snprintf(qn, sizeof(qn), "__route__ingress__%s", ref->route);
    snprintf(host, sizeof(host), "%.*s", (int)(slash - ref->route), ref->route);
    cbm_json_escape(esc_host, sizeof(esc_host), host);
    cbm_json_escape(esc_path, sizeof(esc_path), slash);
    char props[2 * K8S_ROUTE_ESC + CBM_SZ_256];
    snprintf(props, sizeof(props),
             "{\"source\":\"k8s\",\"via\":\"%s\",\"host\":\"%s\",\"path\":\"%s\"}", ref->via,
             esc_host, esc_path);
    int64_t id = cbm_gbuf_upsert_node(ctx->gbuf, "Route", ref->route, qn, src->file_path,
                                      src->start_line, src->end_line, props);
    if (id > 0) {
        cbm_gbuf_insert_edge(ctx->gbuf, src->id, id, "DEFINES", "{}");
    }
    return id;
}

/* Draw every reference to the object(s) it names inside its bundle;
 * an unresolved reference draws nothing. An exposed route's reference
 * leaves from its Route node. */
static void k8s_resolve_refs(cbm_pipeline_ctx_t *ctx, k8s_bundles_t *b, const k8s_facts_t *f) {
    int edges = 0;
    int unresolved = 0;
    CBMHashTable *ix = f->ref_n > 0 ? k8s_index_objects(ctx, b, true) : NULL;
    k8s_ids_t found = {0};
    const cbm_gbuf_node_t **cands = NULL;
    int cand_cap = 0;
    for (int i = 0; ix && i < f->ref_n; i++) {
        const k8s_ref_t *ref = &f->refs[i];
        const cbm_gbuf_node_t *src = cbm_gbuf_find_by_id(ctx->gbuf, ref->src);
        int64_t from = src ? src->id : 0;
        if (ref->route) {
            from = src && src->file_path ? k8s_route_node(ctx, src, ref) : 0;
            if (from <= 0 || !ref->kind || !ref->name) {
                continue;
            }
        }
        char name[CBM_SZ_1K];
        snprintf(name, sizeof(name), "%s/%s", ref->kind, ref->name);
        int n = 0;
        if (src && src->file_path) {
            k8s_find_in_scopes(b, ix, src->file_path, name, true, &found);
            for (int c = 0; c < found.count; c++) {
                const cbm_gbuf_node_t *t = cbm_gbuf_find_by_id(ctx->gbuf, found.items[c]);
                if (t && k8s_grow((void **)&cands, &cand_cap, n, sizeof(*cands))) {
                    cands[n++] = t;
                }
            }
        }
        atomic_fetch_add_explicit(&g_k8s_candidates, (uint64_t)(n), memory_order_relaxed);
        char props[CBM_SZ_1K];
        if (ref->key) { /* CONNECTS_TO: the variable and container, never the value */
            char kesc[CBM_SZ_256];
            char cesc[CBM_SZ_256];
            cbm_json_escape(kesc, (int)sizeof(kesc), ref->key);
            cbm_json_escape(cesc, (int)sizeof(cesc), ref->container ? ref->container : "-");
            snprintf(props, sizeof(props), "{\"via\":\"%s\",\"key\":\"%s\",\"container\":\"%s\"}",
                     ref->via, kesc, cesc);
        } else {
            snprintf(props, sizeof(props), "{\"%s\":\"%s\"}",
                     strcmp(ref->edge, "INFRA_MAPS") == 0   ? "kind"
                     : strcmp(ref->edge, "DEPENDS_ON") == 0 ? "via"
                                                            : "ref",
                     ref->via);
        }
        /* A name is one object per kind and namespace, so two declarations
         * of it in one bundle are alternatives (versioned manifests side by
         * side): the declaration in the reference's own file, when there is
         * one, is the one it means. */
        bool own_file = false;
        for (int c = 0; c < n && !own_file; c++) {
            const cbm_gbuf_node_t *t = cands[c];
            own_file = t != src && t->label && strcmp(t->label, "Resource") == 0 && t->file_path &&
                       strcmp(t->file_path, src->file_path) == 0 && k8s_ref_meets(b, ref, src, t);
        }
        int hits = 0;
        for (int c = 0; c < n; c++) {
            const cbm_gbuf_node_t *t = cands[c];
            if (t == src || !t->label || strcmp(t->label, "Resource") != 0 || !t->file_path ||
                (own_file && strcmp(t->file_path, src->file_path) != 0) ||
                !k8s_ref_meets(b, ref, src, t)) {
                continue;
            }
            cbm_gbuf_insert_edge(ctx->gbuf, from, t->id, ref->edge, props);
            hits++;
        }
        edges += hits;
        unresolved += hits == 0;
    }
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)cands);
    cbm_free(CBM_MEM_CLASS_OTHER, found.items);
    if (ix) {
        cbm_ht_foreach(ix, k8s_free_ids_entry, NULL);
        cbm_ht_free(ix);
    }
    if (f->ref_n > 0) {
        cbm_log_info("pass.k8s.refs", "refs", itoa_k8s(f->ref_n), "edges", itoa_k8s(edges),
                     "unresolved", itoa_k8s(unresolved));
    }
}

/* A CRD's group/kind, for grouping the CRDs of one type. */
typedef struct {
    char key[CBM_SZ_512];
    int crd;
} k8s_crd_key_t;

static int k8s_cmp_crd_key(const void *a, const void *b) {
    const k8s_crd_key_t *x = a;
    const k8s_crd_key_t *y = b;
    int c = strcmp(x->key, y->key);
    return c ? c : (x->crd > y->crd) - (x->crd < y->crd);
}

/* A custom resource is an instance of the CRD declaring its group and
 * kind. A CRD vendored in several places: the copy deployed with the
 * resource (one bundle) is the one; with no copy beside it, the resource
 * stands for all of them. */
/* A CRD node says which group/kind it declares (search, and the Go operator
 * pass's CRD -> type link). */
static void k8s_mark_crd(cbm_pipeline_ctx_t *ctx, const k8s_crd_t *crd) {
    cbm_gbuf_node_t *n = (cbm_gbuf_node_t *)cbm_gbuf_find_by_id(ctx->gbuf, crd->node);
    const char *props = n ? n->properties_json : NULL;
    size_t pl = props ? strlen(props) : 0;
    if (!props || pl < PAIR_LEN || props[pl - SKIP_ONE] != '}' || strstr(props, "\"crd_kind\"")) {
        return;
    }
    char group[K8S_LABEL_LEN * PAIR_LEN];
    char kind[K8S_LABEL_LEN * PAIR_LEN];
    cbm_json_escape(group, (int)sizeof(group), crd->group);
    cbm_json_escape(kind, (int)sizeof(kind), crd->kind);
    size_t n_out = pl + strlen(group) + strlen(kind) + CBM_SZ_64;
    char *out = cbm_alloc(CBM_MEM_CLASS_OTHER, n_out);
    if (out) {
        snprintf(out, n_out, "%.*s,\"crd_group\":\"%s\",\"crd_kind\":\"%s\"}", (int)(pl - SKIP_ONE),
                 props, group, kind);
        (void)cbm_gbuf_node_set_properties_json(n, out);
        cbm_free(CBM_MEM_CLASS_OTHER, out);
    }
}

static void k8s_link_crds(cbm_pipeline_ctx_t *ctx, k8s_bundles_t *b, const k8s_facts_t *f) {
    if (f->crd_n == 0) {
        return;
    }
    for (int i = 0; i < f->crd_n; i++) {
        k8s_mark_crd(ctx, &f->crds[i]);
    }
    /* group/kind -> the run of its CRDs in `order` (sorted by group/kind), so
     * each resource visits only the CRDs of its own type. */
    k8s_crd_key_t *order = cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)f->crd_n * sizeof(*order));
    CBMHashTable *by_type = cbm_ht_create(0);
    if (!order || !by_type) {
        cbm_free(CBM_MEM_CLASS_OTHER, order);
        if (by_type) {
            cbm_ht_free(by_type);
        }
        return;
    }
    for (int i = 0; i < f->crd_n; i++) {
        snprintf(order[i].key, sizeof(order[i].key), "%s/%s", f->crds[i].group, f->crds[i].kind);
        order[i].crd = i;
    }
    k8s_qsort(order, (size_t)f->crd_n, sizeof(*order), k8s_cmp_crd_key);
    for (int i = f->crd_n - SKIP_ONE; i >= 0; i--) {
        cbm_ht_set(by_type, order[i].key, (void *)(intptr_t)(i + SKIP_ONE)); /* first of run */
    }
    /* "<bundle>\x1f<group/kind>" -> the CRDs of that type in that bundle */
    CBMHashTable *in_bundle = cbm_ht_create(0);
    for (int i = 0; in_bundle && i < f->crd_n; i++) {
        const cbm_gbuf_node_t *crd = cbm_gbuf_find_by_id(ctx->gbuf, f->crds[order[i].crd].node);
        if (crd && crd->file_path) {
            k8s_scope_job_t j = {in_bundle, order[i].key, crd->id, NULL};
            k8s_each_scope(b, crd->file_path, false, k8s_scope_add, &j);
        }
    }
    k8s_ids_t beside = {0};
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    int edges = 0;
    if (in_bundle && cbm_gbuf_find_by_label(ctx->gbuf, "Resource", &nodes, &count) == 0) {
        for (int i = 0; i < count; i++) {
            const char *key =
                nodes[i]->qualified_name ? strstr(nodes[i]->qualified_name, "::") : NULL;
            if (!key || !nodes[i]->file_path) {
                continue;
            }
            key += K8S_SLEN("::");
            const char *kind_end = strchr(key, '/');
            kind_end = kind_end ? strchr(kind_end + SKIP_ONE, '/') : NULL;
            if (!kind_end) {
                continue;
            }
            char type[CBM_SZ_512];
            snprintf(type, sizeof(type), "%.*s", (int)(kind_end - key), key);
            int first = (int)(intptr_t)cbm_ht_get(by_type, type) - SKIP_ONE;
            if (first < 0) {
                continue;
            }
            k8s_find_in_scopes(b, in_bundle, nodes[i]->file_path, type, false, &beside);
            if (beside.count > 0) {
                atomic_fetch_add_explicit(&g_k8s_candidates, (uint64_t)beside.count,
                                          memory_order_relaxed);
                for (int r = 0; r < beside.count; r++) {
                    if (beside.items[r] != nodes[i]->id) {
                        cbm_gbuf_insert_edge(ctx->gbuf, nodes[i]->id, beside.items[r], "USAGE",
                                             "{\"ref\":\"crd\"}");
                        edges++;
                    }
                }
                continue;
            }
            int end = first;
            while (end < f->crd_n && strcmp(order[end].key, type) == 0) {
                end++;
            }
            atomic_fetch_add_explicit(&g_k8s_candidates, (uint64_t)(end - first),
                                      memory_order_relaxed);
            for (int r = first; r < end; r++) {
                int64_t crd_id = f->crds[order[r].crd].node;
                if (crd_id != nodes[i]->id) {
                    cbm_gbuf_insert_edge(ctx->gbuf, nodes[i]->id, crd_id, "USAGE",
                                         "{\"ref\":\"crd\"}");
                    edges++;
                }
            }
        }
    }
    cbm_free(CBM_MEM_CLASS_OTHER, beside.items);
    if (in_bundle) {
        cbm_ht_foreach(in_bundle, k8s_free_ids_entry, NULL);
        cbm_ht_free(in_bundle);
    }
    cbm_ht_free(by_type);
    cbm_free(CBM_MEM_CLASS_OTHER, order);
    if (edges > 0) {
        cbm_log_info("pass.k8s.crds", "instances", itoa_k8s(edges));
    }
}

/* Selector postings keyed by "<bundle>\x1f<key=value>" (owned keys): the
 * workloads (record indices) of one bundle carrying one label pair. */
static void k8s_posting_free_owned(const char *key, void *value, void *userdata) {
    k8s_posting_free(key, value, userdata);
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)key);
}

typedef struct {
    CBMHashTable *index;
    const k8s_record_array_t *recs;
    const k8s_record_t *rec;
    int rec_idx;
    k8s_ids_t *cand; /* record indices */
} k8s_sel_job_t;

static void k8s_sel_post(const char *bundle, void *ud) {
    k8s_sel_job_t *j = ud;
    for (int p = 0; p < j->rec->pair_n; p++) {
        char key[CBM_SZ_1K];
        k8s_bkey(key, sizeof(key), bundle, j->recs->pairs[j->rec->pair_off + p]);
        if (!cbm_ht_get(j->index, key)) {
            k8s_posting_t *posting = cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*posting));
            char *owned = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, key);
            if (!posting || !owned) {
                cbm_free(CBM_MEM_CLASS_OTHER, posting);
                cbm_free(CBM_MEM_CLASS_OTHER, owned);
                continue;
            }
            cbm_ht_set(j->index, owned, posting);
        }
        (void)k8s_posting_add(j->index, key, j->rec_idx);
    }
}

/* The workloads of `bundle` carrying the selector's rarest pair there. */
static void k8s_sel_rarest(const char *bundle, void *ud) {
    k8s_sel_job_t *j = ud;
    const k8s_posting_t *rarest = NULL;
    for (int p = 0; p < j->rec->pair_n; p++) {
        char key[CBM_SZ_1K];
        k8s_bkey(key, sizeof(key), bundle, j->recs->pairs[j->rec->pair_off + p]);
        const k8s_posting_t *posting = cbm_ht_get(j->index, key);
        if (!posting) {
            return;
        }
        if (!rarest || posting->count < rarest->count) {
            rarest = posting;
        }
    }
    for (int c = 0; rarest && c < rarest->count; c++) {
        k8s_ids_push(j->cand, rarest->items[c]);
    }
}

/* A workload a selector matched: its object name, declaring file and
 * whether that is the selector's file. */
typedef struct {
    const char *name; /* "Kind/name" (graph-owned) */
    const char *ns;
    bool own;
    int rec;
} k8s_match_t;

static int k8s_cmp_match(const void *a, const void *b) {
    const k8s_match_t *x = a;
    const k8s_match_t *y = b;
    int c = strcmp(x->name, y->name);
    c = c ? c : strcmp(x->ns, y->ns);
    return c ? c : (x->rec > y->rec) - (x->rec < y->rec);
}

/* Draw the selector's edge to each match. A name is one object per kind and
 * namespace, so matches declaring one name in several files are alternatives
 * (versioned manifests side by side): the one in the selector's file, when
 * there is one, is the one it selects. */
static int k8s_draw_matches(cbm_pipeline_ctx_t *ctx, const k8s_record_array_t *recs,
                            const k8s_record_t *svc, k8s_match_t *m, int n) {
    k8s_qsort(m, (size_t)n, sizeof(*m), k8s_cmp_match);
    int edges = 0;
    for (int g = 0; g < n;) {
        int end = g;
        bool own = false;
        while (end < n && strcmp(m[end].name, m[g].name) == 0 && strcmp(m[end].ns, m[g].ns) == 0) {
            own = own || m[end].own;
            end++;
        }
        for (int i = g; i < end; i++) {
            if (own && !m[i].own) {
                continue;
            }
            const k8s_record_t *wl = &recs->items[m[i].rec];
            if (svc->kind == K8S_REC_SERVICE) {
                char props[CBM_SZ_256];
                snprintf(props, sizeof(props),
                         "{\"kind\":\"selector\",\"service\":\"%s\",\"workload\":\"%s\"}",
                         svc->name, wl->name);
                cbm_gbuf_insert_edge(ctx->gbuf, svc->node_id, wl->node_id, "INFRA_MAPS", props);
            } else {
                cbm_gbuf_insert_edge(ctx->gbuf, svc->node_id, wl->node_id, "USAGE",
                                     svc->kind == K8S_REC_PDB ? "{\"ref\":\"selector\"}"
                                                              : "{\"ref\":\"podSelector\"}");
            }
            edges++;
        }
        g = end;
    }
    return edges;
}

/* Connect each Service to the workloads its selector targets via an INFRA_MAPS
 * edge (Service Resource → workload Resource), inside a shared bundle; a
 * PodDisruptionBudget or NetworkPolicy to the workloads it selects (USAGE). */
static void k8s_link_selectors(cbm_pipeline_ctx_t *ctx, const k8s_record_array_t *recs,
                               k8s_bundles_t *bundles) {
    CBMHashTable *index = cbm_ht_create(0);
    if (!index) {
        return;
    }
    k8s_match_t *matches = NULL;
    int match_cap = 0;
    for (int i = 0; i < recs->count; i++) {
        const k8s_record_t *wl = &recs->items[i];
        const cbm_gbuf_node_t *wn = cbm_gbuf_find_by_id(ctx->gbuf, wl->node_id);
        if ((wl->kind == K8S_REC_WORKLOAD || wl->kind == K8S_REC_POD) && wn && wn->file_path) {
            k8s_sel_job_t j = {index, recs, wl, i, NULL};
            k8s_each_scope(bundles, wn->file_path, false, k8s_sel_post, &j);
        }
    }
    k8s_ids_t cand = {0};
    int edges = 0;
    for (int i = 0; i < recs->count; i++) {
        /* a Service, PodDisruptionBudget or NetworkPolicy selector; one with
         * matchExpressions selects fewer pods than its matchLabels, and an
         * empty one every pod of the namespace: neither is drawn */
        const k8s_record_t *svc = &recs->items[i];
        if ((svc->kind != K8S_REC_SERVICE && svc->kind != K8S_REC_PDB &&
             svc->kind != K8S_REC_NETPOL) ||
            svc->pair_n == 0 || svc->has_expr) {
            continue;
        }
        /* Candidates: per bundle of the selector, the workloads there
         * carrying its rarest pair. */
        const cbm_gbuf_node_t *sn = cbm_gbuf_find_by_id(ctx->gbuf, svc->node_id);
        cand.count = 0;
        if (sn && sn->file_path) {
            k8s_sel_job_t j = {index, recs, svc, i, &cand};
            k8s_each_scope(bundles, sn->file_path, false, k8s_sel_rarest, &j);
        }
        k8s_ids_sort_unique(&cand);
        atomic_fetch_add_explicit(&g_k8s_candidates, (uint64_t)cand.count, memory_order_relaxed);
        int n = 0;
        for (int c = 0; c < cand.count; c++) {
            int rec = (int)cand.items[c];
            const k8s_record_t *wl = &recs->items[rec];
            bool all = k8s_share_bundle(bundles, ctx, svc, wl);
            for (int p = 0; all && p < svc->pair_n; p++) {
                all = k8s_has_pair(recs, wl, recs->pairs[svc->pair_off + p]);
            }
            const cbm_gbuf_node_t *wn = all ? cbm_gbuf_find_by_id(ctx->gbuf, wl->node_id) : NULL;
            if (wn && wn->name && wn->file_path &&
                k8s_grow((void **)&matches, &match_cap, n, sizeof(*matches))) {
                matches[n++] =
                    (k8s_match_t){wn->name, wl->ns, strcmp(wn->file_path, sn->file_path) == 0, rec};
            }
        }
        edges += k8s_draw_matches(ctx, recs, svc, matches, n);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, matches);
    cbm_free(CBM_MEM_CLASS_OTHER, cand.items);
    cbm_ht_foreach(index, k8s_posting_free_owned, NULL);
    cbm_ht_free(index);
    if (edges > 0) {
        cbm_log_info("pass.k8s.selectors", "linked", itoa_k8s(edges));
    }
}

/* ── K8s manifest handler ────────────────────────────────────────── */

/* source/src_len are the already-read file bytes (caller retains ownership and
 * must free after this call returns). Every Resource's own document is
 * recorded in `recs` for cross-manifest selector matching. */
static void handle_k8s_manifest(cbm_pipeline_ctx_t *ctx, const char *rel_path, const char *source,
                                const CBMFileResult *res, k8s_record_array_t *recs,
                                k8s_facts_t *facts) {
    int resource_count = 0;

    if (!res) {
        return;
    }

    /* Compute file node QN for DEFINES edges */
    char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel_path, "__file__");
    const cbm_gbuf_node_t *file_node = file_qn ? cbm_gbuf_find_by_qn(ctx->gbuf, file_qn) : NULL;
    free(file_qn);

    int64_t *node_of =
        cbm_calloc(CBM_MEM_CLASS_OTHER,
                   (size_t)(res->defs.count ? res->defs.count : SKIP_ONE) * sizeof(int64_t));
    for (int d = 0; d < res->defs.count; d++) {
        CBMDefinition *def = &res->defs.items[d];
        if (!def->label || strcmp(def->label, "Resource") != 0) {
            continue;
        }
        if (!def->name || !def->qualified_name) {
            continue;
        }

        const char *version = NULL; /* extract_k8s.c's extra properties */
        const char *labels = NULL;
        for (const char **kv = def->extra_props; kv && kv[0] && kv[SKIP_ONE]; kv += PAIR_LEN) {
            if (strcmp(kv[0], "version") == 0) {
                version = kv[SKIP_ONE];
            } else if (strcmp(kv[0], "labels") == 0) {
                labels = kv[SKIP_ONE];
            }
        }
        char *props = k8s_resource_props(def->qualified_name, NULL, version, labels, NULL);
        int64_t node_id = cbm_gbuf_upsert_node(
            ctx->gbuf, "Resource", def->name, def->qualified_name, rel_path, (int)def->start_line,
            (int)def->end_line, props ? props : "{\"source\":\"k8s\"}");
        cbm_free(CBM_MEM_CLASS_OTHER, props);

        /* DEFINES edge: File → Resource */
        if (file_node && node_id > 0) {
            cbm_gbuf_insert_edge(ctx->gbuf, file_node->id, node_id, "DEFINES", "{}");
        }

        /* Record its selector / pod-label pairs for Service → workload linking. */
        if (node_id > 0 && source) {
            k8s_record_document(recs, node_id, source, (int)def->start_line, (int)def->end_line);
        }
        if (node_of) {
            node_of[d] = node_id;
        }

        resource_count++;
    }
    if (node_of) {
        k8s_facts_keep(facts, res, node_of);
        cbm_free(CBM_MEM_CLASS_OTHER, node_of);
    }

    cbm_log_info("pass.k8s.manifest", "file", rel_path, "resources", itoa_k8s(resource_count));
}

/* ── Helm chart handler ──────────────────────────────────────────── */

static bool is_helm_chart_file(const char *base) {
    return strcmp(base, "Chart.yaml") == 0 || strcmp(base, "Chart.yml") == 0;
}

/* A chart's `file://` dependency: the chart in that directory of this
 * repository. Linked once every Chart.yaml has been read, so a dependency
 * indexed after its dependent still resolves. One that names no chart of this
 * repository (an absolute build-container path, a directory without a chart)
 * stays a declared dependency on the external node, like a remote one. */
typedef struct {
    int64_t chart_id;
    char name[CBM_HELM_NAME_MAX]; /* dependency name, for the external fallback */
    char *chart_rel;              /* the depending Chart.yaml */
    char *target_qn;              /* QN of the depended-on Chart node; NULL = unresolvable */
    char *props;                  /* edge properties */
} k8s_local_dep_t;

typedef struct {
    k8s_local_dep_t *items;
    int count;
    int cap;
} k8s_local_deps_t;

static bool k8s_is_file_repository(const char *repository) {
    return strncmp(repository, "file://", K8S_SLEN("file://")) == 0;
}

static char *k8s_dep_props(const char *repository) {
    char escaped[CBM_HELM_REPO_MAX * 2];
    cbm_json_escape(escaped, (int)sizeof(escaped), repository);
    size_t n = strlen(escaped) + sizeof("{\"repository\":\"\"}");
    char *props = cbm_alloc(CBM_MEM_CLASS_OTHER, n);
    if (props) {
        snprintf(props, n, "{\"repository\":\"%s\"}", escaped);
    }
    return props;
}

/* ── External charts ─────────────────────────────────────────────── */

/* An external Chart is keyed by its publisher: `__chart__<url>/<name>`, the
 * same QN in every project, so two repositories naming one published chart
 * name one node. A chart whose repository does not resolve to a URL (a
 * `@alias`, an empty or templated repository, a source the pass cannot read)
 * keeps the per-project `<project>.__helm_dep__.<name>`: no identity is
 * claimed for what was not stated. */
static const char K8S_CHART_QN_PREFIX[] = "__chart__";
static const char K8S_HELM_DEP_MARK[] = ".__helm_dep__.";

/* A byte a stated repository URL may hold: no whitespace or control byte,
 * no quote, template or shell expansion, escape or angle bracket. */
static bool k8s_url_byte_ok(unsigned char c) {
    return c > ' ' && c < 0x7f && !strchr("\"'`{}$<>\\", c);
}

bool cbm_chart_repo_url(const char *url, char *out, size_t sz) {
    if (!url || !out || sz == 0) {
        return false;
    }
    while (*url == ' ' || *url == '\t') {
        url++;
    }
    size_t n = strlen(url);
    while (n > 0 && isspace((unsigned char)url[n - SKIP_ONE])) {
        n--;
    }
    if (n == 0 || n + SKIP_ONE > sz || !isalpha((unsigned char)url[0])) {
        return false;
    }
    for (size_t i = 0; i < n; i++) {
        if (!k8s_url_byte_ok((unsigned char)url[i])) {
            return false;
        }
    }
    /* a query or fragment may carry a token (?token=...): like credentials
     * in the authority, never put into a QN or a property */
    if (memchr(url, '?', n) || memchr(url, '#', n)) {
        return false;
    }
    size_t s = 0; /* the scheme: [A-Za-z][A-Za-z0-9+.-]* then :// */
    while (s < n &&
           (isalnum((unsigned char)url[s]) || url[s] == '+' || url[s] == '-' || url[s] == '.')) {
        s++;
    }
    if (s + K8S_SLEN("://") > n || strncmp(url + s, "://", K8S_SLEN("://")) != 0) {
        return false;
    }
    size_t host = s + K8S_SLEN("://");
    size_t path = host;
    while (path < n && url[path] != '/') {
        path++;
    }
    /* no host, or credentials in the authority: never put into a QN */
    if (path == host || memchr(url + host, '@', path - host)) {
        return false;
    }
    for (size_t i = 0; i < path; i++) {
        out[i] = (char)tolower((unsigned char)url[i]);
    }
    memcpy(out + path, url + path, n - path);
    out[n] = '\0';
    if (strncmp(out, "file://", K8S_SLEN("file://")) == 0) {
        return false; /* a directory, not a publisher */
    }
    size_t end = n;
    while (end > path && out[end - SKIP_ONE] == '/') {
        end--;
    }
    if (end - path >= K8S_SLEN(".git") &&
        strncmp(out + end - K8S_SLEN(".git"), ".git", K8S_SLEN(".git")) == 0) {
        end -= K8S_SLEN(".git");
        while (end > path && out[end - SKIP_ONE] == '/') {
            end--;
        }
    }
    out[end] = '\0';
    return true;
}

bool cbm_chart_ref_repo(const char *ref, char *out, size_t sz) {
    if (!ref || !out || sz == 0) {
        return false;
    }
    size_t end = strlen(ref);
    while (end > 0 && ref[end - SKIP_ONE] == '/') {
        end--;
    }
    while (end > 0 && ref[end - SKIP_ONE] != '/') {
        end--;
    }
    while (end > 0 && ref[end - SKIP_ONE] == '/') {
        end--;
    }
    if (end == 0 || end + SKIP_ONE > sz) {
        return false;
    }
    memcpy(out, ref, end);
    out[end] = '\0';
    return true;
}

/* QN and node properties of the external Chart `name` (non-NULL) of
 * repository `url` (NULL: unknown). */
static void k8s_chart_identity(const char *project, const char *name, const char *url, char *qn,
                               size_t qsz, char *props, size_t psz) {
    char norm[CBM_SZ_1K];
    if (name[0] && !strchr(name, '/') && cbm_chart_repo_url(url, norm, sizeof(norm))) {
        int w = snprintf(qn, qsz, "%s%s/%s", K8S_CHART_QN_PREFIX, norm, name);
        if (w > 0 && (size_t)w < qsz) {
            char esc[CBM_SZ_2K];
            cbm_json_escape(esc, (int)sizeof(esc), norm);
            snprintf(props, psz, "{\"source\":\"helm\",\"external\":true,\"repository\":\"%s\"}",
                     esc);
            return;
        }
    }
    snprintf(qn, qsz, "%s%s%s", project, K8S_HELM_DEP_MARK, name);
    snprintf(props, psz, "{\"source\":\"helm\",\"external\":true}");
}

int64_t cbm_pipeline_external_chart(cbm_pipeline_ctx_t *ctx, const char *name, const char *url,
                                    const char *file) {
    if (!name) {
        return 0;
    }
    char qn[CBM_SZ_2K];
    char props[CBM_SZ_4K];
    k8s_chart_identity(ctx->project_name, name, url, qn, sizeof(qn), props, sizeof(props));
    const cbm_gbuf_node_t *known = cbm_gbuf_find_by_qn(ctx->gbuf, qn);
    if (known) {
        return known->id;
    }
    return cbm_gbuf_upsert_node(ctx->gbuf, "Chart", name, qn, file, SKIP_ONE, 0, props);
}

bool cbm_pipeline_chart_is_external(const cbm_gbuf_node_t *node) {
    const char *qn = node ? node->qualified_name : NULL;
    return qn && (strncmp(qn, K8S_CHART_QN_PREFIX, K8S_SLEN(K8S_CHART_QN_PREFIX)) == 0 ||
                  strstr(qn, K8S_HELM_DEP_MARK) != NULL);
}

/* DEPENDS_ON from `chart_id` to the external Chart node of `name` in
 * `repository` (charts depending on the same published chart meet). */
static bool k8s_external_dep(cbm_pipeline_ctx_t *ctx, int64_t chart_id, const char *name,
                             const char *repository, const char *chart_rel, const char *props) {
    char dep_qn[CBM_SZ_2K];
    char node_props[CBM_SZ_4K];
    k8s_chart_identity(ctx->project_name, name, repository, dep_qn, sizeof(dep_qn), node_props,
                       sizeof(node_props));
    int64_t dep_id =
        cbm_gbuf_upsert_node(ctx->gbuf, "Chart", name, dep_qn, chart_rel, SKIP_ONE, 0, node_props);
    if (dep_id <= 0) {
        return false;
    }
    cbm_gbuf_insert_edge(ctx->gbuf, chart_id, dep_id, "DEPENDS_ON", props ? props : "{}");
    return true;
}

/* QN of the chart a relative `file://` directory names, core-owned; NULL for
 * an absolute path (it names the build host, not this repo). */
static char *k8s_local_chart_qn(const char *project, const char *chart_rel, const char *dir) {
    if (!dir[0] || dir[0] == '/') {
        return NULL;
    }
    char rel[CBM_HELM_REPO_MAX + K8S_SLEN("./") + K8S_SLEN("/Chart.yaml")];
    snprintf(rel, sizeof(rel), "%s%s/Chart.yaml", strncmp(dir, "../", 3) == 0 ? "" : "./", dir);
    /* fqn.c hands out libc-allocated strings: copy into the core, release them. */
    char *path = cbm_pipeline_resolve_relative_import(chart_rel, rel);
    char *qn = path ? cbm_infra_qn(project, path, "helm-chart", NULL) : NULL;
    free(path);
    char *owned = qn ? cbm_mem_strdup(CBM_MEM_CLASS_OTHER, qn) : NULL;
    free(qn);
    return owned;
}

static void k8s_local_deps_push(k8s_local_deps_t *deps, int64_t chart_id, const char *chart_rel,
                                const char *project, const cbm_helm_dep_t *dep) {
    if (deps->count == deps->cap) {
        int cap = deps->cap ? deps->cap * 2 : K8S_GROW_MIN;
        k8s_local_dep_t *grown =
            cbm_realloc(CBM_MEM_CLASS_OTHER, deps->items, (size_t)cap * sizeof(*grown));
        if (!grown) {
            return;
        }
        deps->items = grown;
        deps->cap = cap;
    }
    k8s_local_dep_t *d = &deps->items[deps->count++];
    *d = (k8s_local_dep_t){.chart_id = chart_id};
    d->target_qn = k8s_local_chart_qn(project, chart_rel, dep->repository + K8S_SLEN("file://"));
    snprintf(d->name, sizeof(d->name), "%s", dep->name);
    d->chart_rel = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, chart_rel);
    d->props = k8s_dep_props(dep->repository);
}

static void k8s_local_deps_free(k8s_local_deps_t *deps) {
    for (int i = 0; i < deps->count; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, deps->items[i].target_qn);
        cbm_free(CBM_MEM_CLASS_OTHER, deps->items[i].chart_rel);
        cbm_free(CBM_MEM_CLASS_OTHER, deps->items[i].props);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, deps->items);
    *deps = (k8s_local_deps_t){0};
}

static void k8s_link_local_deps(cbm_pipeline_ctx_t *ctx, k8s_local_deps_t *deps) {
    for (int i = 0; i < deps->count; i++) {
        const k8s_local_dep_t *d = &deps->items[i];
        const cbm_gbuf_node_t *target =
            d->target_qn ? cbm_gbuf_find_by_qn(ctx->gbuf, d->target_qn) : NULL;
        if (target && target->label && strcmp(target->label, "Chart") == 0 &&
            target->id != d->chart_id) {
            cbm_gbuf_insert_edge(ctx->gbuf, d->chart_id, target->id, "DEPENDS_ON", d->props);
        } else if (d->chart_rel) {
            /* a file:// directory this repository cannot resolve names no
             * publisher: the per-project external Chart */
            (void)k8s_external_dep(ctx, d->chart_id, d->name, NULL, d->chart_rel, d->props);
        }
    }
    k8s_local_deps_free(deps);
}

/* Emit a Chart node for a Chart.yaml and a DEPENDS_ON edge per declared
 * dependency (#338): to the chart in this repository for a resolvable
 * `file://` dependency (once all charts are read), otherwise to the external
 * Chart node of the dependency's repository and name, the repository also on
 * the edge. */
/* The Chart node `name` defined by chart file `rel_path` (a Chart.yaml, a
 * werf.yaml), its File node DEFINES it; 0 on failure. */
static int64_t k8s_chart_upsert(cbm_pipeline_ctx_t *ctx, const char *rel_path, const char *name,
                                const char *props) {
    char *chart_qn = cbm_infra_qn(ctx->project_name, rel_path, "helm-chart", NULL);
    if (!chart_qn) {
        return 0;
    }
    int64_t chart_id =
        cbm_gbuf_upsert_node(ctx->gbuf, "Chart", name, chart_qn, rel_path, SKIP_ONE, 0, props);
    free(chart_qn);

    char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel_path, "__file__");
    const cbm_gbuf_node_t *file_node = file_qn ? cbm_gbuf_find_by_qn(ctx->gbuf, file_qn) : NULL;
    if (file_node && chart_id > 0) {
        cbm_gbuf_insert_edge(ctx->gbuf, file_node->id, chart_id, "DEFINES", "{}");
    }
    free(file_qn);
    return chart_id > 0 ? chart_id : 0;
}

static void handle_helm_chart(cbm_pipeline_ctx_t *ctx, const char *rel_path, const char *source,
                              k8s_local_deps_t *local_deps) {
    cbm_helm_chart_t hc;
    if (cbm_parse_helm_chart(source, &hc) != 0) {
        cbm_helm_chart_free(&hc);
        return;
    }

    const char *cname = hc.chart_name[0] ? hc.chart_name : k8s_basename(rel_path);
    int64_t chart_id = k8s_chart_upsert(ctx, rel_path, cname, "{\"source\":\"helm\"}");
    if (chart_id <= 0) {
        cbm_helm_chart_free(&hc);
        return;
    }

    int dep_edges = 0;
    for (int i = 0; i < hc.dep_count; i++) {
        const cbm_helm_dep_t *dep = &hc.deps[i];
        if (k8s_is_file_repository(dep->repository)) {
            k8s_local_deps_push(local_deps, chart_id, rel_path, ctx->project_name, dep);
            continue;
        }
        char *props = dep->repository[0] ? k8s_dep_props(dep->repository) : NULL;
        dep_edges += k8s_external_dep(ctx, chart_id, dep->name, dep->repository, rel_path, props);
        cbm_free(CBM_MEM_CLASS_OTHER, props);
    }
    cbm_helm_chart_free(&hc);
    cbm_log_info("pass.k8s.helm", "file", rel_path, "deps", itoa_k8s(dep_edges));
}

/* ── werf chart handler ──────────────────────────────────────────── */

/* werf (werf.io) deploys the Helm chart in the `.helm` directory beside its
 * werf.yaml; that chart needs no Chart.yaml, werf names it after its project.
 * A `.helm` with a Chart.yaml is an ordinary chart. A custom
 * deploy.helmChartDir is not followed. */
static bool is_werf_file(const char *base) {
    return strcmp(base, "werf.yaml") == 0 || strcmp(base, "werf.yml") == 0;
}

bool cbm_werf_helm_dir(const char *repo_path, const char *dir) {
    const char *base = k8s_basename(dir);
    if (!repo_path || strcmp(base, ".helm") != 0) {
        return false;
    }
    char path[CBM_SZ_4K];
    snprintf(path, sizeof(path), "%s/%s", repo_path, dir);
    if (!cbm_is_dir(path)) {
        return false;
    }
    static const char *const charts[] = {"Chart.yaml", "Chart.yml"};
    for (size_t i = 0; i < sizeof(charts) / sizeof(charts[0]); i++) {
        snprintf(path, sizeof(path), "%s/%s/%s", repo_path, dir, charts[i]);
        if (cbm_file_exists(path)) {
            return false;
        }
    }
    size_t parent = (size_t)(base - dir);
    static const char *const werfs[] = {"werf.yaml", "werf.yml"};
    for (size_t i = 0; i < sizeof(werfs) / sizeof(werfs[0]); i++) {
        snprintf(path, sizeof(path), "%s/%.*s%s", repo_path, (int)parent, dir, werfs[i]);
        if (cbm_file_exists(path)) {
            return true;
        }
    }
    return false;
}

/* The literal top-level `project:` of a werf.yaml's first document into out,
 * "" when there is none or it is templated. */
static void k8s_werf_project(const char *src, char *out, size_t out_sz) {
    out[0] = '\0';
    bool seen_key = false;
    for (const char *p = src; p && *p;) {
        const char *eol = strchr(p, '\n');
        size_t len = eol ? (size_t)(eol - p) : strlen(p);
        if (len >= K8S_SLEN("---") && strncmp(p, "---", K8S_SLEN("---")) == 0 && seen_key) {
            return;
        }
        if (isalpha((unsigned char)p[0])) {
            seen_key = true;
        }
        if (len > K8S_SLEN("project:") && strncmp(p, "project:", K8S_SLEN("project:")) == 0) {
            /* the value: [b, e) of the line, a comment and quotes removed */
            size_t b = K8S_SLEN("project:");
            size_t e = len;
            while (b < e && (p[b] == ' ' || p[b] == '\t')) {
                b++;
            }
            for (size_t i = b; i < e; i++) {
                if (p[i] == '#') {
                    e = i;
                }
            }
            while (e > b && isspace((unsigned char)p[e - SKIP_ONE])) {
                e--;
            }
            if (e >= b + PAIR_LEN && (p[b] == '"' || p[b] == '\'') && p[e - SKIP_ONE] == p[b]) {
                b++;
                e--;
            }
            if (e > b && e - b < out_sz && !memchr(p + b, '{', e - b)) {
                memcpy(out, p + b, e - b);
                out[e - b] = '\0';
            }
            return;
        }
        p = eol ? eol + SKIP_ONE : NULL;
    }
}

/* A werf.yaml beside a `.helm` chart dir: a Chart named after its project
 * {source: werf}, defined by werf.yaml, and the chart dir into `chart_dir`.
 * False when it has no such chart. */
static bool handle_werf_chart(cbm_pipeline_ctx_t *ctx, const char *rel_path, const char *source,
                              char *chart_dir, size_t chart_dir_sz) {
    char dir[CBM_SZ_1K];
    k8s_dirname(rel_path, dir, sizeof(dir));
    snprintf(chart_dir, chart_dir_sz, "%s%s.helm", dir, dir[0] ? "/" : "");
    if (!cbm_werf_helm_dir(ctx->repo_path, chart_dir)) {
        return false;
    }
    char project[CBM_HELM_NAME_MAX];
    k8s_werf_project(source, project, sizeof(project));
    const char *cname = project[0] ? project : k8s_basename(rel_path);
    int64_t chart_id = k8s_chart_upsert(ctx, rel_path, cname, "{\"source\":\"werf\"}");
    cbm_log_info("pass.k8s.werf", "file", rel_path, "chart", cname);
    return chart_id > 0;
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
typedef enum { K8S_SKIP = 0, K8S_DEP, K8S_HELM, K8S_WERF, K8S_MANIFEST, K8S_KUSTOMIZE } k8s_kind_t;

typedef struct {
    k8s_kind_t kind;
    char *source; /* DEP, HELM, MANIFEST */
    int src_len;
    CBMFileResult *res; /* MANIFEST */
} k8s_prep_t;

typedef struct {
    cbm_pipeline_ctx_t *ctx;
    const cbm_file_info_t *files;
    k8s_prep_t *prep; /* indexed from `begin` */
    int begin;
    int end;
    _Atomic int next;
} k8s_prep_job_t;

enum { K8S_CHUNK = 1024, K8S_TS_PAD = 16 };

/* ── Templated manifests (PHASE 2 P2.3) ────────────────────────────── */

/* A Helm chart template or Go-template addon is YAML only once rendered:
 * `{{- if .Values.x }}` opens the document, `name: {{ include "app.fullname"
 * . }}` is a flow mapping to a YAML parser. The skeleton is the text with
 * every action rewritten, line for line (document lines stay exact): a line
 * holding nothing but actions is blanked; an action inside text becomes a
 * symbolic scalar -- `<app.fullname>` for an include/template call, the
 * first `.`/`$` path otherwise (`<.Values.image.tag>`), else `<tpl>`. Objects
 * then get the chart-scoped symbolic names IDENTITY.md describes, and a
 * reference written with the same expression meets them in the chart. */

static bool k8s_tpl_ident(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
}

/* Does the action text at q (before e) call a named template: `include` /
 * `template` then a quoted name? Fills `out` with `<name>`. */
static bool k8s_action_call(const char *a, const char *q, const char *e, char *out, size_t out_sz) {
    size_t kl = strncmp(q, "include", K8S_SLEN("include")) == 0     ? K8S_SLEN("include")
                : strncmp(q, "template", K8S_SLEN("template")) == 0 ? K8S_SLEN("template")
                                                                    : 0;
    if (!kl || q + kl >= e || q[kl] != ' ' || (q > a && k8s_tpl_ident(q[-SKIP_ONE]))) {
        return false;
    }
    const char *r = q + kl;
    while (r < e && *r == ' ') {
        r++;
    }
    const char *ne =
        r < e && *r == '"' ? memchr(r + SKIP_ONE, '"', (size_t)(e - r - SKIP_ONE)) : NULL;
    if (!ne || ne == r + SKIP_ONE) {
        return false;
    }
    snprintf(out, out_sz, "<%.*s>", (int)(ne - r - SKIP_ONE), r + SKIP_ONE);
    return true;
}

/* The symbolic token of the action text [a, e) into out (NUL-terminated). */
static void k8s_action_token(const char *a, const char *e, char *out, size_t out_sz) {
    for (const char *q = a; q < e; q++) {
        if (k8s_action_call(a, q, e, out, out_sz)) {
            return;
        }
    }
    for (const char *q = a; q < e; q++) {
        if ((*q == '.' || *q == '$') && (q == a || q[-SKIP_ONE] == ' ' || q[-SKIP_ONE] == '(')) {
            const char *r = q + SKIP_ONE;
            while (r < e && (k8s_tpl_ident(*r) || *r == '.' || *r == '$')) {
                r++;
            }
            if (r > q + SKIP_ONE || *q == '$') {
                snprintf(out, out_sz, "<%.*s>", (int)(r - q), q);
                return;
            }
        }
    }
    snprintf(out, out_sz, "<tpl>");
}

/* The end of the logical line starting at ls: its newline, or -- when an
 * action starting on it spans lines -- the newline after that action. Sets
 * *only_actions when the line holds actions and nothing else but blanks. */
static const char *k8s_logical_line(const char *ls, const char *end, bool *only_actions) {
    const char *le = memchr(ls, '\n', (size_t)(end - ls));
    le = le ? le : end;
    bool blank = true;
    bool any = false;
    for (const char *p = ls; p < le;) {
        if (p + SKIP_ONE < le && p[0] == '{' && p[SKIP_ONE] == '{') {
            const char *ae = strstr(p + PAIR_LEN, "}}");
            if (!ae || ae >= end) {
                blank = false;
                break;
            }
            any = true;
            p = ae + PAIR_LEN;
            if (p > le) {
                const char *nl = memchr(p, '\n', (size_t)(end - p));
                le = nl ? nl : end;
            }
            continue;
        }
        blank = blank && (*p == ' ' || *p == '\t' || *p == '\r');
        p++;
    }
    *only_actions = any && blank;
    return le;
}

/* The skeleton of templated text (cbm_alloc'd, *out_len set); NULL on OOM. */
static char *k8s_skeleton(const char *src, int len, int *out_len) {
    size_t cap = (size_t)len * PAIR_LEN + CBM_SZ_64 + K8S_TS_PAD;
    char *out = cbm_alloc(CBM_MEM_CLASS_OTHER, cap);
    if (!out) {
        return NULL;
    }
    size_t w = 0;
    const char *end = src + len;
    for (const char *ls = src; ls < end;) {
        bool only_actions = false;
        const char *le = k8s_logical_line(ls, end, &only_actions);
        for (const char *p = ls; p < le;) {
            const char *ae = p + SKIP_ONE < le && p[0] == '{' && p[SKIP_ONE] == '{'
                                 ? strstr(p + PAIR_LEN, "}}")
                                 : NULL;
            if (!ae || ae >= end) {
                if (!only_actions) {
                    out[w++] = *p;
                }
                p++;
                continue;
            }
            if (!only_actions) {
                char tok[CBM_SZ_256];
                k8s_action_token(p + PAIR_LEN, ae, tok, sizeof(tok));
                size_t tl = strlen(tok);
                memcpy(out + w, tok, tl);
                w += tl;
            }
            for (const char *q = p; q < ae; q++) { /* the action's own newlines stay */
                if (*q == '\n') {
                    out[w++] = '\n';
                }
            }
            p = ae + PAIR_LEN;
        }
        if (le < end) {
            out[w++] = '\n';
        }
        ls = le < end ? le + SKIP_ONE : end;
    }
    memset(out + w, 0, K8S_TS_PAD); /* lexer lookahead, as k8s_read_file pads */
    *out_len = (int)w;
    return out;
}

static void k8s_prep_file(const k8s_prep_job_t *job, int i, k8s_prep_t *out) {
    cbm_pipeline_ctx_t *ctx = job->ctx;
    const char *path = job->files[i].path;
    const char *rel = job->files[i].rel_path;
    CBMLanguage lang = job->files[i].language;
    const char *base = k8s_basename(rel);

    if (is_gomod_file(base) || lang == CBM_LANG_GOMOD || is_requirements_file(base)) {
        out->source = k8s_read_file(path, &out->src_len);
        out->kind = out->source ? K8S_DEP : K8S_SKIP;
    } else if (lang == CBM_LANG_KUSTOMIZE) {
        /* Its `namespace:` is the namespace of the objects it builds; its
         * patches (PATCH facts) change some of them. */
        out->source = k8s_read_file(path, &out->src_len);
        out->kind = out->source ? K8S_KUSTOMIZE : K8S_SKIP;
        if (out->source) {
            out->res = cbm_extract_file(out->source, out->src_len, CBM_LANG_KUSTOMIZE,
                                        ctx->project_name, rel, CBM_EXTRACT_BUDGET, NULL, NULL);
        }
    } else if (lang == CBM_LANG_YAML || lang == CBM_LANG_K8S) {
        /* Read source once to classify (and reuse for uncached extraction). */
        out->source = k8s_read_file(path, &out->src_len);
        if (!out->source) {
            return;
        }
        if (is_helm_chart_file(base)) {
            out->kind = K8S_HELM;
        } else if (is_werf_file(base)) {
            out->kind = K8S_WERF;
        } else if (cbm_is_k8s_manifest(base, out->source)) {
            /* Always re-extract with CBM_LANG_K8S regardless of any cached
             * result: cached results were produced during the parallel YAML
             * pass and contain no "Resource" definitions. */
            out->kind = K8S_MANIFEST;
            if (strstr(out->source, "{{")) {
                int sk_len = 0;
                char *sk = k8s_skeleton(out->source, out->src_len, &sk_len);
                if (sk) {
                    cbm_free(CBM_MEM_CLASS_OTHER, out->source);
                    out->source = sk;
                    out->src_len = sk_len;
                }
            }
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

/* ── Helm includes (PHASE 2 P2.1) ──────────────────────────────────── */

/* A chart template's `include "name"` / `template "name"` calls a named
 * template (`define "name"`, a Function of a .tpl file). Helm loads a chart's
 * templates with those of every chart below it into one namespace, so the
 * name resolves among the defines of the chart family -- the outermost
 * enclosing chart and the charts below it -- and of the families its members
 * depend on locally (file://). A dynamic name draws nothing. */

/* The nearest chart dir enclosing file `path` ("" for a root chart). */
static bool k8s_chart_of(k8s_bundles_t *b, const char *path, char *out, size_t out_sz) {
    char dir[CBM_SZ_1K];
    k8s_dirname(path, dir, sizeof(dir));
    for (;;) {
        if (cbm_ht_get(b->charts, dir)) {
            snprintf(out, out_sz, "%s", dir);
            return true;
        }
        if (!dir[0]) {
            return false;
        }
        char parent[CBM_SZ_1K];
        k8s_dirname(dir, parent, sizeof(parent));
        snprintf(dir, sizeof(dir), "%s", parent);
    }
}

/* The outermost chart dir enclosing chart dir `chart` (itself when none). */
static void k8s_chart_family(k8s_bundles_t *b, const char *chart, char *out, size_t out_sz) {
    snprintf(out, out_sz, "%s", chart);
    char dir[CBM_SZ_1K];
    snprintf(dir, sizeof(dir), "%s", chart);
    while (dir[0]) {
        char parent[CBM_SZ_1K];
        k8s_dirname(dir, parent, sizeof(parent));
        snprintf(dir, sizeof(dir), "%s", parent);
        if (cbm_ht_get(b->charts, dir)) {
            snprintf(out, out_sz, "%s", dir);
        }
    }
}

static bool k8s_has_suffix(const char *s, const char *suffix) {
    size_t n = strlen(s);
    size_t m = strlen(suffix);
    return n >= m && strcmp(s + n - m, suffix) == 0;
}

static bool k8s_ident_char(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
}

typedef struct {
    cbm_pipeline_ctx_t *ctx;
    k8s_bundles_t *b;
    CBMHashTable *defines;    /* "<family>\x1f<name>" -> define Functions */
    CBMHashTable *deps;       /* "<family>" -> Chart nodes its members depend on */
    CBMHashTable *resources;  /* file -> its Resource nodes */
    CBMHashTable *file_nodes; /* file -> its File node */
    CBMHashTable *yaml_defs;  /* template file -> the defines written in it */
    CBMArena arena;           /* define QNs */
    const char *file;
    char family[CBM_SZ_1K]; /* the chart family of `file` */
    k8s_ids_t found;
    int edges;
    int unresolved;
} k8s_inc_t;

/* The node a call on `line` of the file leaves from: the innermost document
 * (Resource) spanning it, else the File. */
static int64_t k8s_inc_source(k8s_inc_t *s, int line) {
    const cbm_gbuf_node_t *best = NULL;
    for (int pass = 0; pass < PAIR_LEN && !best; pass++) {
        const k8s_ids_t *res = cbm_ht_get(pass == 0 ? s->yaml_defs : s->resources, s->file);
        for (int i = 0; res && i < res->count; i++) {
            const cbm_gbuf_node_t *n = cbm_gbuf_find_by_id(s->ctx->gbuf, res->items[i]);
            if (n && n->start_line <= line && line <= n->end_line &&
                (!best || n->end_line - n->start_line < best->end_line - best->start_line)) {
                best = n;
            }
        }
    }
    if (best) {
        return best->id;
    }
    const k8s_ids_t *f = cbm_ht_get(s->file_nodes, s->file);
    return f && f->count > 0 ? f->items[0] : 0;
}

static void k8s_inc_lookup(k8s_inc_t *s, const char *family, const char *name) {
    char key[CBM_SZ_1K];
    snprintf(key, sizeof(key), "%s\x1f%s", family, name);
    const k8s_ids_t *v = cbm_ht_get(s->defines, key);
    for (int i = 0; v && i < v->count; i++) {
        k8s_ids_push(&s->found, v->items[i]);
    }
}

/* One `include "name"` / `template "name"` call on `line`. */
static void k8s_inc_call(k8s_inc_t *s, const char *name, int line, const char *via) {
    s->found.count = 0;
    k8s_inc_lookup(s, s->family, name);
    const k8s_ids_t *deps = cbm_ht_get(s->deps, s->family);
    for (int d = 0; deps && d < deps->count; d++) {
        const cbm_gbuf_node_t *dep = cbm_gbuf_find_by_id(s->ctx->gbuf, deps->items[d]);
        if (dep && dep->file_path) {
            char chart[CBM_SZ_1K];
            char family[CBM_SZ_1K];
            k8s_dirname(dep->file_path, chart, sizeof(chart));
            k8s_chart_family(s->b, chart, family, sizeof(family));
            k8s_inc_lookup(s, family, name);
        }
    }
    k8s_ids_sort_unique(&s->found);
    atomic_fetch_add_explicit(&g_k8s_candidates, (uint64_t)s->found.count, memory_order_relaxed);
    int64_t src = s->found.count > 0 ? k8s_inc_source(s, line) : 0;
    if (src <= 0) {
        s->unresolved++;
        return;
    }
    char esc[CBM_SZ_512];
    cbm_json_escape(esc, (int)sizeof(esc), name);
    char props[CBM_SZ_1K];
    snprintf(props, sizeof(props),
             "{\"callee\":\"%s\",\"strategy\":\"helm_include\",\"via\":\"%s\"}", esc, via);
    for (int i = 0; i < s->found.count; i++) {
        if (s->found.items[i] != src) {
            cbm_gbuf_insert_edge(s->ctx->gbuf, src, s->found.items[i], "CALLS", props);
            s->edges++;
        }
    }
}

/* Every literal `include "name"` / `template "name"` inside the `{{ }}`
 * actions of a template's text, with its line. */
static void k8s_scan_includes(k8s_inc_t *s, const char *src) {
    int line = SKIP_ONE;
    const char *at = src;
    for (const char *p = strstr(src, "{{"); p;) {
        const char *e = strstr(p + PAIR_LEN, "}}");
        if (!e) {
            break;
        }
        for (const char *q = p + PAIR_LEN; q < e; q++) {
            const char *via = strncmp(q, "include", K8S_SLEN("include")) == 0     ? "include"
                              : strncmp(q, "template", K8S_SLEN("template")) == 0 ? "template"
                                                                                  : NULL;
            if (!via || k8s_ident_char(q[-SKIP_ONE]) || q[-SKIP_ONE] == '.' ||
                q[-SKIP_ONE] == '$') {
                continue;
            }
            const char *r = q + strlen(via);
            if (r >= e || (*r != ' ' && *r != '\t' && *r != '\n' && *r != '\r')) {
                continue;
            }
            while (r < e && (*r == ' ' || *r == '\t' || *r == '\n' || *r == '\r')) {
                r++;
            }
            const char *ne =
                r < e && *r == '"' ? memchr(r + SKIP_ONE, '"', (size_t)(e - r - SKIP_ONE)) : NULL;
            if (!ne || ne == r + SKIP_ONE) {
                continue;
            }
            for (; at < q; at++) {
                line += *at == '\n';
            }
            char name[CBM_SZ_256];
            snprintf(name, sizeof(name), "%.*s", (int)(ne - r - SKIP_ONE), r + SKIP_ONE);
            k8s_inc_call(s, name, line, via);
            q = ne;
        }
        p = strstr(e + PAIR_LEN, "{{");
    }
}

static CBMHashTable *k8s_index_by_file(cbm_pipeline_ctx_t *ctx, const char *label) {
    CBMHashTable *ix = cbm_ht_create(0);
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (!ix || cbm_gbuf_find_by_label(ctx->gbuf, label, &nodes, &count) != 0) {
        return ix;
    }
    for (int i = 0; i < count; i++) {
        if (nodes[i]->file_path) {
            k8s_index_add(ix, nodes[i]->file_path, nodes[i]->id);
        }
    }
    return ix;
}

static void k8s_free_index(CBMHashTable *ix) {
    if (ix) {
        cbm_ht_foreach(ix, k8s_free_ids_entry, NULL);
        cbm_ht_free(ix);
    }
}

/* Does the action text at `q` start with keyword `kw`? */
static bool k8s_action_is(const char *q, const char *kw) {
    size_t n = strlen(kw);
    return strncmp(q, kw, n) == 0 && !k8s_ident_char(q[n]);
}

/* A `define "name"` written in a template (outside a .tpl file the grammar
 * reads): a Function like a .tpl define, spanning to its matching `end`. */
static void k8s_yaml_define(k8s_inc_t *s, const char *name, int start, int end) {
    char *qn = cbm_fqn_compute(&s->arena, s->ctx->project_name, s->file, name);
    if (!qn) {
        return;
    }
    int64_t id = cbm_gbuf_upsert_node(s->ctx->gbuf, "Function", name, qn, s->file, start, end,
                                      "{\"source\":\"helm_template\"}");
    if (id <= 0) {
        return;
    }
    const k8s_ids_t *f = cbm_ht_get(s->file_nodes, s->file);
    if (f && f->count > 0) {
        cbm_gbuf_insert_edge(s->ctx->gbuf, f->items[0], id, "DEFINES", "{}");
    }
    char key[CBM_SZ_1K];
    snprintf(key, sizeof(key), "%s\x1f%s", s->family, name);
    k8s_index_add(s->defines, key, id);
    k8s_index_add(s->yaml_defs, s->file, id);
}

/* Every `define "name"` ... `end` of a template's text: block actions (if,
 * range, with, block, define) nest until their `end`. */
static void k8s_scan_defines(k8s_inc_t *s, const char *src) {
    int line = SKIP_ONE;
    const char *at = src;
    int depth = 0;
    int start = 0;
    char name[CBM_SZ_256] = "";
    for (const char *p = strstr(src, "{{"); p;) {
        const char *e = strstr(p + PAIR_LEN, "}}");
        if (!e) {
            break;
        }
        for (; at < p; at++) {
            line += *at == '\n';
        }
        const char *q = p + PAIR_LEN;
        q += *q == '-';
        while (q < e && (*q == ' ' || *q == '\t' || *q == '\n' || *q == '\r')) {
            q++;
        }
        if (depth == 0 && k8s_action_is(q, "define")) {
            const char *r = q + K8S_SLEN("define");
            while (r < e && (*r == ' ' || *r == '\t')) {
                r++;
            }
            const char *ne =
                r < e && *r == '"' ? memchr(r + SKIP_ONE, '"', (size_t)(e - r - SKIP_ONE)) : NULL;
            if (ne && ne > r + SKIP_ONE) {
                snprintf(name, sizeof(name), "%.*s", (int)(ne - r - SKIP_ONE), r + SKIP_ONE);
                depth = SKIP_ONE;
                start = line;
            }
        } else if (depth > 0 && (k8s_action_is(q, "if") || k8s_action_is(q, "range") ||
                                 k8s_action_is(q, "with") || k8s_action_is(q, "block") ||
                                 k8s_action_is(q, "define"))) {
            depth++;
        } else if (depth > 0 && k8s_action_is(q, "end")) {
            depth--;
            if (depth == 0) {
                for (; at < e; at++) {
                    line += *at == '\n';
                }
                k8s_yaml_define(s, name, start, line);
            }
        }
        p = strstr(e + PAIR_LEN, "{{");
    }
}

/* A chart template file: .yaml / .yml / .json under a chart's templates/;
 * fills its chart's family. */
static bool k8s_template_family(k8s_bundles_t *b, const char *rel, char *family, size_t sz) {
    char chart[CBM_SZ_1K];
    if (!rel ||
        !(k8s_has_suffix(rel, ".yaml") || k8s_has_suffix(rel, ".yml") ||
          k8s_has_suffix(rel, ".json")) ||
        !k8s_chart_of(b, rel, chart, sizeof(chart))) {
        return false;
    }
    size_t cl = strlen(chart);
    const char *sub = cl ? rel + cl + SKIP_ONE : rel;
    if (strncmp(sub, "templates/", K8S_SLEN("templates/")) != 0) {
        return false;
    }
    k8s_chart_family(b, chart, family, sz);
    return true;
}

/* Index the defines and dependencies, then scan every template of a chart. */
static void k8s_includes_run(k8s_inc_t *s, const cbm_file_info_t *files, int file_count) {
    /* defines by chart family: Functions of the charts' Go template files */
    const cbm_gbuf_node_t **fns = NULL;
    int fn_n = 0;
    if (cbm_gbuf_find_by_label(s->ctx->gbuf, "Function", &fns, &fn_n) == 0) {
        for (int i = 0; i < fn_n; i++) {
            char chart[CBM_SZ_1K];
            char family[CBM_SZ_1K];
            char key[CBM_SZ_1K];
            const char *fp = fns[i]->file_path;
            if (!fp || !fns[i]->name ||
                !(k8s_has_suffix(fp, ".tpl") || k8s_has_suffix(fp, ".gotmpl")) ||
                !k8s_chart_of(s->b, fp, chart, sizeof(chart))) {
                continue;
            }
            k8s_chart_family(s->b, chart, family, sizeof(family));
            snprintf(key, sizeof(key), "%s\x1f%s", family, fns[i]->name);
            k8s_index_add(s->defines, key, fns[i]->id);
        }
    }
    /* local dependencies: Chart -DEPENDS_ON-> Chart, by the source's family */
    const cbm_gbuf_edge_t **deps = NULL;
    int dep_n = 0;
    if (cbm_gbuf_find_edges_by_type(s->ctx->gbuf, "DEPENDS_ON", &deps, &dep_n) == 0) {
        for (int i = 0; i < dep_n; i++) {
            const cbm_gbuf_node_t *src = cbm_gbuf_find_by_id(s->ctx->gbuf, deps[i]->source_id);
            const cbm_gbuf_node_t *tgt = cbm_gbuf_find_by_id(s->ctx->gbuf, deps[i]->target_id);
            if (!src || !tgt || !src->label || !tgt->label || strcmp(src->label, "Chart") != 0 ||
                strcmp(tgt->label, "Chart") != 0 || !src->file_path || !tgt->file_path) {
                continue;
            }
            char chart[CBM_SZ_1K];
            char family[CBM_SZ_1K];
            k8s_dirname(src->file_path, chart, sizeof(chart));
            k8s_chart_family(s->b, chart, family, sizeof(family));
            k8s_index_add(s->deps, family, tgt->id);
        }
    }
    /* every template of a chart: its own defines, then its includes */
    for (int pass = 0; pass < PAIR_LEN; pass++) {
        for (int i = 0; i < file_count; i++) {
            const char *rel = files[i].rel_path;
            if (!k8s_template_family(s->b, rel, s->family, sizeof(s->family))) {
                continue;
            }
            char path[CBM_SZ_4K];
            snprintf(path, sizeof(path), "%s/%s", s->ctx->repo_path, rel);
            int len = 0;
            char *src = k8s_read_file(path, &len);
            if (!src) {
                continue;
            }
            s->file = rel;
            if (pass == 0) {
                k8s_scan_defines(s, src);
            } else {
                k8s_scan_includes(s, src);
            }
            cbm_free(CBM_MEM_CLASS_OTHER, src);
        }
    }
}

static void k8s_link_includes(cbm_pipeline_ctx_t *ctx, k8s_bundles_t *b,
                              const cbm_file_info_t *files, int file_count) {
    if (!b->charts || !ctx->repo_path) {
        return;
    }
    k8s_inc_t s = {.ctx = ctx,
                   .b = b,
                   .defines = cbm_ht_create(0),
                   .deps = cbm_ht_create(0),
                   .resources = k8s_index_by_file(ctx, "Resource"),
                   .file_nodes = k8s_index_by_file(ctx, "File"),
                   .yaml_defs = cbm_ht_create(0)};
    cbm_arena_init(&s.arena);
    if (s.defines && s.deps && s.resources && s.file_nodes && s.yaml_defs) {
        k8s_includes_run(&s, files, file_count);
    }
    cbm_arena_destroy(&s.arena);
    cbm_free(CBM_MEM_CLASS_OTHER, s.found.items);
    k8s_free_index(s.defines);
    k8s_free_index(s.deps);
    k8s_free_index(s.resources);
    k8s_free_index(s.file_nodes);
    k8s_free_index(s.yaml_defs);
    if (s.edges + s.unresolved > 0) {
        cbm_log_info("pass.k8s.includes", "edges", itoa_k8s(s.edges), "unresolved",
                     itoa_k8s(s.unresolved));
    }
}

/* ── DEPLOYS (PHASE 3 P3.1) ────────────────────────────────────────── */

/* What a GitOps object deploys, as DEPLOYS edges to existing nodes only:
 * a Flux Kustomization / Argo Application -> the units its path applies (the
 * outermost kustomizations below it -- their Kustomization node -- and the
 * manifests outside any, their File node; a chart dir: its Chart); a
 * HelmRelease -> its chart (a repository chart dir of a Git/Bucket source,
 * else the external Chart of that name). */

typedef struct {
    cbm_pipeline_ctx_t *ctx;
    CBMHashTable *files;  /* file -> File node */
    CBMHashTable *charts; /* Chart.yaml path -> Chart node */
    CBMHashTable *kusts;  /* kustomization path -> its Kustomization Resource */
    int edges;
} k8s_dep_link_t;

static int64_t k8s_first_id(CBMHashTable *ix, const char *key) {
    const k8s_ids_t *v = cbm_ht_get(ix, key);
    return v && v->count > 0 ? v->items[0] : 0;
}

static void k8s_deploys_edge(k8s_dep_link_t *l, int64_t src, int64_t tgt, const char *props) {
    if (src > 0 && tgt > 0 && src != tgt) {
        cbm_gbuf_insert_edge(l->ctx->gbuf, src, tgt, "DEPLOYS", props);
        l->edges++;
    }
}

/* The Chart node of chart dir `dir`, 0 when it is none: its Chart.yaml's, or
 * for a werf `.helm` dir the one its werf.yaml defines. */
static int64_t k8s_chart_node(k8s_dep_link_t *l, const char *dir) {
    static const char *const names[] = {"Chart.yaml", "Chart.yml"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        char path[CBM_SZ_1K];
        snprintf(path, sizeof(path), "%s%s%s", dir, dir[0] ? "/" : "", names[i]);
        int64_t id = k8s_first_id(l->charts, path);
        if (id > 0) {
            return id;
        }
    }
    const char *base = k8s_basename(dir);
    if (strcmp(base, ".helm") != 0) {
        return 0;
    }
    static const char *const werfs[] = {"werf.yaml", "werf.yml"};
    for (size_t i = 0; i < sizeof(werfs) / sizeof(werfs[0]); i++) {
        char path[CBM_SZ_1K];
        snprintf(path, sizeof(path), "%.*s%s", (int)(base - dir), dir, werfs[i]);
        int64_t id = k8s_first_id(l->charts, path);
        if (id > 0) {
            return id;
        }
    }
    return 0;
}

/* The external Chart of `name` in repository `url` (NULL: unknown), the one
 * chart dependencies use. */
static int64_t k8s_external_chart(k8s_dep_link_t *l, const char *name, const char *url,
                                  const char *file) {
    return cbm_pipeline_external_chart(l->ctx, name, url, file);
}

/* ── A chart's own edges (PHASE 2 P2.4) ────────────────────────────── */

bool cbm_helm_values_name(const char *base) {
    size_t vl = K8S_SLEN("values");
    if (!base || strncmp(base, "values", vl) != 0) {
        return false;
    }
    const char *r = base + vl;
    if (*r == '-') {
        const char *dot = strrchr(r, '.');
        if (!dot || dot == r + SKIP_ONE) {
            return false;
        }
        r = dot;
    }
    return strcmp(r, ".yaml") == 0 || strcmp(r, ".yml") == 0;
}

/* Is `rel` a values file directly in chart dir `dir`? */
static bool k8s_chart_values_file(const char *rel, const char *dir) {
    size_t dl = strlen(dir);
    if (dl && (strncmp(rel, dir, dl) != 0 || rel[dl] != '/')) {
        return false;
    }
    const char *base = rel + (dl ? dl + SKIP_ONE : 0);
    return !strchr(base, '/') && cbm_helm_values_name(base);
}

/* A chart deploys what its templates render: Chart -DEPLOYS {via: chart}->
 * each Resource of <chart>/templates/ -- its nearest enclosing chart, so a
 * subchart's objects are the subchart's -- and each CRD of <chart>/crds/,
 * which Helm installs first ({via: chart, phase: crds}). Its values files
 * (values.yaml, values-<x>.yaml beside Chart.yaml) CONFIGURE {strategy:
 * helm_values} it. */
static void k8s_link_chart_contents(cbm_pipeline_ctx_t *ctx, k8s_bundles_t *b) {
    if (!b->charts || cbm_ht_count(b->charts) == 0) {
        return;
    }
    k8s_dep_link_t l = {.ctx = ctx, .charts = k8s_index_by_file(ctx, "Chart")};
    if (!l.charts) {
        return;
    }
    int configures = 0;
    static const char *const labels[] = {"Resource", "File"};
    for (size_t k = 0; k < sizeof(labels) / sizeof(labels[0]); k++) {
        const cbm_gbuf_node_t **nodes = NULL;
        int n = 0;
        if (cbm_gbuf_find_by_label(ctx->gbuf, labels[k], &nodes, &n) != 0) {
            continue;
        }
        for (int i = 0; i < n; i++) {
            const char *fp = nodes[i]->file_path;
            char dir[CBM_SZ_1K];
            int64_t chart = 0;
            if (fp && k8s_chart_of(b, fp, dir, sizeof(dir))) {
                chart = k8s_chart_node(&l, dir);
            }
            if (chart <= 0) {
                continue;
            }
            if (k == 0) {
                char tpl[CBM_SZ_1K];
                char crd[CBM_SZ_1K];
                snprintf(tpl, sizeof(tpl), "%s%stemplates/", dir, dir[0] ? "/" : "");
                snprintf(crd, sizeof(crd), "%s%scrds/", dir, dir[0] ? "/" : "");
                if (strncmp(fp, tpl, strlen(tpl)) == 0) {
                    k8s_deploys_edge(&l, chart, nodes[i]->id, "{\"via\":\"chart\"}");
                } else if (strncmp(fp, crd, strlen(crd)) == 0) {
                    k8s_deploys_edge(&l, chart, nodes[i]->id,
                                     "{\"via\":\"chart\",\"phase\":\"crds\"}");
                }
            } else if (k8s_chart_values_file(fp, dir)) {
                cbm_gbuf_insert_edge(ctx->gbuf, nodes[i]->id, chart, "CONFIGURES",
                                     "{\"strategy\":\"helm_values\"}");
                configures++;
            }
        }
    }
    k8s_free_index(l.charts);
    if (l.edges + configures > 0) {
        cbm_log_info("pass.k8s.chart_contents", "deploys", itoa_k8s(l.edges), "values",
                     itoa_k8s(configures));
    }
}

/* ── Helm template images (PHASE 2 P2.5) ─────────────────────────────
 *
 * A chart template's container image is a placeholder in the skeleton
 * (`<.Values.image.repository>`): it is rendered here (helm_template.c) from
 * the chart's own values.yaml, its Chart.yaml (.Chart) and the defines of its
 * chart family -- what `helm template` writes with the chart's defaults. The
 * object then USES the Image {ref: image, default_of: the values keys read},
 * an image nothing else in the repository changes but a release can. A
 * parent chart that changes it for its subchart (its `<sub>:` values, its
 * `global:`) DEPLOYS the Image it makes {via: chart, ref: image,
 * override_of}. A value that does not render (helm_template.h: UNKNOWN) or
 * is no image reference names none. */

typedef struct {
    int64_t node;
    char *file;
    char *container; /* NULL: unnamed */
    int start_line;
    int end_line;
} k8s_timg_t;

typedef struct {
    char *dir;
    int64_t chart;
    cbm_helm_chart_meta_t meta;
    yyjson_mut_val *chart_yaml; /* NULL: none */
    yyjson_mut_val *values;     /* NULL: values.yaml does not parse (nothing renders) */
} k8s_hchart_t;

/* A template file of a chart family (its defines). */
typedef struct {
    char *file;
    char *family;
} k8s_htpl_t;

/* One rendered image of an object (or a chart / release). */
typedef struct {
    int64_t src;
    int64_t img;
    const char *ref;
    const char *container;
    const char *keys; /* JSON array; NULL: none */
} k8s_himg_t;

typedef struct {
    k8s_himg_t *v;
    int n;
    int cap;
    CBMHashTable *seen; /* "src:img" of each v (keys cbm_alloc'd): one look-up a push */
} k8s_himgs_t;

typedef struct {
    yyjson_mut_doc *doc;
    CBMArena a;
    CBMHashTable *charts;  /* chart dir -> k8s_hchart_t* */
    CBMHashTable *defines; /* family dir -> cbm_helm_defines_t* */
    k8s_htpl_t *tpls;
    int ntpl;
    bool tpls_read;
    k8s_timg_t *t;
    int nt;
    int tcap;
    k8s_himgs_t dflt; /* the containers' images with the defaults */
    int edges;
} k8s_helm_t;

/* The templated images of the facts, kept past them. */
static void k8s_helm_collect(cbm_pipeline_ctx_t *ctx, const k8s_facts_t *f, k8s_helm_t *h) {
    for (int i = 0; i < f->img_n; i++) {
        const k8s_img_t *im = &f->imgs[i];
        const cbm_gbuf_node_t *n =
            strchr(im->image, '<') ? cbm_gbuf_find_by_id(ctx->gbuf, im->node) : NULL;
        if (!n || !n->file_path || im->start_line <= 0) {
            continue;
        }
        if (!k8s_grow((void **)&h->t, &h->tcap, h->nt, sizeof(*h->t))) {
            return;
        }
        h->t[h->nt++] = (k8s_timg_t){im->node, cbm_arena_strdup(&h->a, n->file_path),
                                     im->container ? cbm_arena_strdup(&h->a, im->container) : NULL,
                                     im->start_line, im->end_line};
    }
}

/* Repository file `rel`, read only when discovery took it in: a values file,
 * a Chart.yaml, a kustomize valuesFile named by path is never a symlink or
 * under one -- nothing is read from outside the repository. */
static char *k8s_repo_read(cbm_pipeline_ctx_t *ctx, const char *rel, int *len) {
    if (!cbm_pipeline_file_node(ctx->gbuf, ctx->project_name, rel)) {
        return NULL;
    }
    char path[CBM_SZ_4K];
    int n = snprintf(path, sizeof(path), "%s/%s", ctx->repo_path, rel);
    if (n <= 0 || (size_t)n >= sizeof(path)) {
        return NULL;
    }
    char *buf = k8s_read_file(path, len);
    cbm_path_info_t info;
    if (!buf && cbm_path_info_utf8(path, &info) == CBM_PATH_INFO_OK && info.is_regular &&
        !info.is_symlink && info.size == 0) {
        /* an empty file is read: empty text (padded like every read) */
        enum { CBM_TS_EMPTY_PAD = 16 };
        buf = cbm_calloc(CBM_MEM_CLASS_OTHER, CBM_TS_EMPTY_PAD);
        *len = 0;
    }
    return buf;
}

/* Is repository path `rel` there, or can that not be told (a link is
 * reported, never followed; only a path proven absent is not there)? */
static bool k8s_repo_has(cbm_pipeline_ctx_t *ctx, const char *rel) {
    char path[CBM_SZ_4K];
    int n = snprintf(path, sizeof(path), "%s/%s", ctx->repo_path, rel);
    cbm_path_info_t info;
    return n > 0 && (size_t)n < sizeof(path) &&
           cbm_path_info_utf8(path, &info) != CBM_PATH_INFO_ABSENT;
}

/* A file of `dir` (one of `names`), parsed as values; *found when one exists.
 * *unread when one is there but is not read (a symlink, too large): what it
 * holds is unknown, which is not the same as no file. */
static yyjson_mut_val *k8s_helm_yaml(cbm_pipeline_ctx_t *ctx, k8s_helm_t *h, const char *dir,
                                     const char *const *names, bool *found, bool *unread) {
    *found = false;
    *unread = false;
    for (int i = 0; names[i]; i++) {
        char rel[CBM_SZ_1K];
        snprintf(rel, sizeof(rel), "%s%s%s", dir, dir[0] ? "/" : "", names[i]);
        int len = 0;
        char *src = k8s_repo_read(ctx, rel, &len);
        if (!src) {
            if (k8s_repo_has(ctx, rel)) {
                *found = true;
                *unread = true;
                return NULL;
            }
            continue;
        }
        *found = true;
        yyjson_mut_val *v = cbm_helm_values_parse(h->doc, src, (size_t)len);
        cbm_free(CBM_MEM_CLASS_OTHER, src);
        return v;
    }
    return NULL;
}

static const char *k8s_helm_meta_str(yyjson_mut_val *obj, const char *key) {
    yyjson_mut_val *v = yyjson_mut_is_obj(obj) ? yyjson_mut_obj_get(obj, key) : NULL;
    return yyjson_mut_is_str(v) ? yyjson_mut_get_str(v) : NULL;
}

/* The chart of chart dir `dir` with its metadata and default values. */
static k8s_hchart_t *k8s_helm_chart(cbm_pipeline_ctx_t *ctx, k8s_helm_t *h, k8s_dep_link_t *l,
                                    const char *dir) {
    k8s_hchart_t *c = cbm_ht_get(h->charts, dir);
    if (c) {
        return c;
    }
    c = cbm_arena_calloc(&h->a, sizeof(*c));
    if (!c) {
        return NULL;
    }
    c->dir = cbm_arena_strdup(&h->a, dir);
    c->chart = k8s_chart_node(l, dir);
    static const char *const CHART[] = {"Chart.yaml", "Chart.yml", NULL};
    static const char *const VALUES[] = {"values.yaml", "values.yml", NULL};
    bool found = false;
    bool chart_unread = false;
    bool values_unread = false;
    c->chart_yaml = k8s_helm_yaml(ctx, h, dir, CHART, &found, &chart_unread);
    yyjson_mut_val *vals = k8s_helm_yaml(ctx, h, dir, VALUES, &found, &values_unread);
    /* a Chart.yaml or values file there but not read: the chart's values are
     * unknown (it renders nothing), never empty */
    c->values = chart_unread || values_unread ? NULL : found ? vals : yyjson_mut_obj(h->doc);
    const cbm_gbuf_node_t *cn = c->chart > 0 ? cbm_gbuf_find_by_id(ctx->gbuf, c->chart) : NULL;
    const char *name = k8s_helm_meta_str(c->chart_yaml, "name");
    c->meta.name = name ? name : cn && cn->name ? cbm_arena_strdup(&h->a, cn->name) : NULL;
    c->meta.version = k8s_helm_meta_str(c->chart_yaml, "version");
    c->meta.app_version = k8s_helm_meta_str(c->chart_yaml, "appVersion");
    cbm_ht_set(h->charts, c->dir, c);
    return c;
}

static bool k8s_helm_tpl_ext(const char *p) {
    static const char *const EXT[] = {".yaml", ".yml", ".tpl", ".gotmpl", ".txt", ".json"};
    for (size_t i = 0; i < sizeof(EXT) / sizeof(EXT[0]); i++) {
        if (k8s_has_suffix(p, EXT[i])) {
            return true;
        }
    }
    return false;
}

/* The defines of chart family `family`: every template (<chart>/templates/)
 * of its charts, read from disk. */
static const cbm_helm_defines_t *k8s_helm_defines(cbm_pipeline_ctx_t *ctx, k8s_bundles_t *b,
                                                  k8s_helm_t *h, const char *family) {
    cbm_helm_defines_t *d = cbm_ht_get(h->defines, family);
    if (d) {
        return d;
    }
    if (!h->tpls_read) {
        h->tpls_read = true;
        const cbm_gbuf_node_t **files = NULL;
        int n = 0;
        if (cbm_gbuf_find_by_label(ctx->gbuf, "File", &files, &n) == 0 && n > 0) {
            h->tpls = cbm_arena_alloc(&h->a, (size_t)n * sizeof(*h->tpls));
            for (int i = 0; h->tpls && i < n; i++) {
                const char *fp = files[i]->file_path;
                char dir[CBM_SZ_1K];
                char fam[CBM_SZ_1K];
                char tdir[CBM_SZ_1K];
                if (!fp || !k8s_helm_tpl_ext(fp) || !k8s_chart_of(b, fp, dir, sizeof(dir))) {
                    continue;
                }
                snprintf(tdir, sizeof(tdir), "%s%stemplates/", dir, dir[0] ? "/" : "");
                if (strncmp(fp, tdir, strlen(tdir)) != 0) {
                    continue;
                }
                k8s_chart_family(b, dir, fam, sizeof(fam));
                h->tpls[h->ntpl++] =
                    (k8s_htpl_t){cbm_arena_strdup(&h->a, fp), cbm_arena_strdup(&h->a, fam)};
            }
        }
    }
    d = cbm_helm_defines_new();
    if (!d) {
        return NULL;
    }
    for (int i = 0; i < h->ntpl; i++) {
        if (strcmp(h->tpls[i].family, family) != 0) {
            continue;
        }
        int len = 0;
        char *src = k8s_repo_read(ctx, h->tpls[i].file, &len);
        if (src) {
            cbm_helm_defines_add(d, h->tpls[i].file, src, (size_t)len);
            cbm_free(CBM_MEM_CLASS_OTHER, src);
        }
    }
    cbm_ht_set(h->defines, cbm_arena_strdup(&h->a, family), d);
    return d;
}

static void k8s_helm_free_defines(const char *key, void *value, void *ud) {
    (void)key;
    (void)ud;
    cbm_helm_defines_free(value);
}

static int k8s_timg_cmp(const void *x, const void *y) {
    const k8s_timg_t *a = x;
    const k8s_timg_t *b = y;
    int c = strcmp(a->file, b->file);
    return c ? c : a->start_line - b->start_line;
}

/* Does template line `line` (1-based) of src hold an `image:` key? */
static bool k8s_helm_image_line(const char *src, size_t len, const uint32_t *starts, int line) {
    size_t k = starts[line - SKIP_ONE];
    while (k < len && (src[k] == ' ' || src[k] == '\t')) {
        k++;
    }
    if (k + SKIP_ONE < len && src[k] == '-' && src[k + SKIP_ONE] == ' ') {
        k++;
        while (k < len && src[k] == ' ') {
            k++;
        }
    }
    return k + strlen("image:") <= len && memcmp(src + k, "image:", strlen("image:")) == 0;
}

/* The probes of a template's templated images [from, to): each `image:`
 * line inside a container (both branches of an if / else among them). */
static int k8s_helm_probes(k8s_helm_t *h, const k8s_timg_t *t, int from, int to, const char *src,
                           size_t len, cbm_helm_probe_t **out) {
    int nl = SKIP_ONE;
    for (size_t i = 0; i < len; i++) {
        nl += src[i] == '\n';
    }
    uint32_t *starts = cbm_arena_alloc(&h->a, (size_t)nl * sizeof(*starts));
    if (!starts) {
        return 0;
    }
    int k = 0;
    starts[k++] = 0;
    for (size_t i = 0; i < len && k < nl; i++) {
        if (src[i] == '\n') {
            starts[k++] = (uint32_t)(i + SKIP_ONE);
        }
    }
    int cap = 0;
    for (int i = from; i < to; i++) {
        cap += t[i].end_line - t[i].start_line + SKIP_ONE;
    }
    cbm_helm_probe_t *p = cap ? cbm_arena_calloc(&h->a, (size_t)cap * sizeof(*p)) : NULL;
    int np = 0;
    for (int i = from; p && i < to; i++) {
        for (int line = t[i].start_line; line <= t[i].end_line && line <= k; line++) {
            bool dup = false;
            for (int j = 0; j < np && !dup; j++) {
                dup = p[j].line == line;
            }
            if (!dup && k8s_helm_image_line(src, len, starts, line)) {
                p[np++].line = line;
            }
        }
    }
    *out = p;
    return np;
}

static yyjson_mut_val *k8s_helm_dig(yyjson_mut_val *v, const char *path) {
    const char *p = path;
    while (v && *p) {
        const char *dot = strchr(p, '.');
        size_t kl = dot ? (size_t)(dot - p) : strlen(p);
        v = yyjson_mut_is_obj(v) ? yyjson_mut_obj_getn(v, p, kl) : NULL;
        p += kl + (dot ? SKIP_ONE : 0);
    }
    return v;
}

/* Is values key path `key` (dot-joined, `\.` escaped) set in `v`? */
static bool k8s_helm_sets(k8s_helm_t *h, yyjson_mut_val *v, const char *key) {
    char *part = cbm_arena_alloc(&h->a, strlen(key) + SKIP_ONE); /* a key is never cut */
    const char *p = key;
    while (v && part && *p) {
        size_t k = 0;
        while (*p && *p != '.' && *p != '[') {
            if (*p == '\\' && p[1]) {
                p++;
            }
            part[k++] = *p++;
        }
        part[k] = '\0';
        v = yyjson_mut_is_obj(v) ? yyjson_mut_obj_get(v, part) : NULL;
        while (v && *p == '[') {
            long idx = strtol(p + SKIP_ONE, (char **)&p, 10);
            p += *p == ']' ? SKIP_ONE : 0;
            v = yyjson_mut_is_arr(v) ? yyjson_mut_arr_get(v, (size_t)idx) : NULL;
        }
        if (*p == '.') {
            p++;
        }
    }
    return v != NULL;
}

/* The values keys of JSON array `keys` that `overlay` sets (relative to
 * the chart), prefixed by `prefix` -- as a JSON array; NULL: none. */
static const char *k8s_helm_overridden(k8s_helm_t *h, const char *keys, yyjson_mut_val *overlay,
                                       yyjson_mut_val *globals, const char *prefix) {
    if (!keys) {
        return NULL;
    }
    yyjson_doc *doc = yyjson_read(keys, strlen(keys), 0);
    yyjson_val *arr = doc ? yyjson_doc_get_root(doc) : NULL;
    const char *out = NULL;
    if (yyjson_is_arr(arr)) {
        size_t idx, max;
        yyjson_val *k;
        yyjson_arr_foreach(arr, idx, max, k) {
            const char *key = yyjson_get_str(k);
            bool global = key && strncmp(key, "global.", strlen("global.")) == 0;
            bool set = key && (global ? k8s_helm_sets(h, globals, key + strlen("global."))
                                      : k8s_helm_sets(h, overlay, key));
            if (!set) {
                continue;
            }
            /* JSON escaping writes at most 6 bytes a byte */
            int en = (int)(strlen(key) * 6 + SKIP_ONE);
            char *esc = cbm_arena_alloc(&h->a, (size_t)en);
            if (!esc) {
                out = NULL;
                break;
            }
            cbm_json_escape(esc, en, key);
            out = cbm_arena_sprintf(&h->a, "%s%s\"%s%s%s\"", out ? out : "[", out ? "," : "",
                                    global || !prefix ? "" : prefix, global || !prefix ? "" : ".",
                                    esc);
        }
    }
    if (doc) {
        yyjson_doc_free(doc);
    }
    return out ? cbm_arena_sprintf(&h->a, "%s]", out) : NULL;
}

static void k8s_himg_push(k8s_himgs_t *l, k8s_himg_t x) {
    char key[CBM_SZ_64];
    snprintf(key, sizeof(key), "%lld:%lld", (long long)x.src, (long long)x.img);
    if (!l->seen) {
        l->seen = cbm_ht_create(0);
    }
    if (!l->seen || cbm_ht_get(l->seen, key)) {
        return; /* the first (in file order) of the same Image stands for it */
    }
    size_t kl = strlen(key) + SKIP_ONE;
    char *k = cbm_alloc(CBM_MEM_CLASS_OTHER, kl);
    if (!k || !k8s_grow((void **)&l->v, &l->cap, l->n, sizeof(*l->v))) {
        cbm_free(CBM_MEM_CLASS_OTHER, k);
        return;
    }
    memcpy(k, key, kl);
    cbm_ht_set(l->seen, k, (void *)(uintptr_t)SKIP_ONE);
    l->v[l->n++] = x;
}

static void k8s_himg_key_free(const char *key, void *value, void *ud) {
    (void)value;
    (void)ud;
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)key);
}

static void k8s_himgs_free(k8s_himgs_t *l) {
    if (l->seen) {
        cbm_ht_foreach(l->seen, k8s_himg_key_free, NULL);
        cbm_ht_free(l->seen);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, l->v);
    *l = (k8s_himgs_t){0};
}

/* Does `src` USE Image `img` already by a literal reference? */
static bool k8s_helm_literal_use(cbm_pipeline_ctx_t *ctx, int64_t src, int64_t img) {
    const cbm_gbuf_edge_t **es = NULL;
    int n = 0;
    if (cbm_gbuf_find_edges_by_source_type(ctx->gbuf, src, "USAGE", &es, &n) != 0) {
        return false;
    }
    for (int i = 0; i < n; i++) {
        if (es[i]->target_id == img && es[i]->properties_json &&
            strstr(es[i]->properties_json, "\"ref\":\"image\"")) {
            return true;
        }
    }
    return false;
}

/* The captures of probes within [first, last] lines, as images of `src`. */
static void k8s_helm_take(cbm_pipeline_ctx_t *ctx, const cbm_helm_probe_t *p, int np, int first,
                          int last, int64_t src, const char *container, k8s_himgs_t *out) {
    for (int i = 0; i < np; i++) {
        if (p[i].line < first || p[i].line > last) {
            continue;
        }
        for (int c = 0; c < p[i].n; c++) {
            const char *ref = p[i].image[c];
            int64_t img = ref && cbm_image_ref_valid(ref) ? cbm_pipeline_image_node(ctx, ref) : 0;
            if (img > 0) {
                k8s_himg_push(out, (k8s_himg_t){src, img, ref, container, p[i].keys[c]});
            }
        }
    }
}

/* The images of templated containers [from, to) of one template, rendered
 * with `values` (NULL: the chart's own). */
static void k8s_helm_render(cbm_pipeline_ctx_t *ctx, k8s_helm_t *h, k8s_bundles_t *b,
                            const k8s_hchart_t *c, const char *as_name, const char *full,
                            const k8s_timg_t *t, int from, int to, yyjson_mut_val *values,
                            const char *src, size_t len, cbm_helm_probe_t *probes, int np) {
    char fam[CBM_SZ_1K];
    k8s_chart_family(b, c->dir, fam, sizeof(fam));
    const cbm_helm_defines_t *defs = k8s_helm_defines(ctx, b, h, fam);
    /* a subchart a dependency aliases is .Chart.Name = its alias (Helm's
     * getAliasDependency) */
    cbm_helm_chart_meta_t meta = c->meta;
    meta.name = as_name ? as_name : meta.name;
    /* .Template.BasePath: the chart's path (full) and templates/, as Helm */
    meta.base_path = full ? cbm_arena_sprintf(&h->a, "%s/templates", full) : NULL;
    meta.dir = c->dir;
    cbm_helm_render(&h->a, t[from].file, src, len, values ? values : c->values, &meta, defs, probes,
                    np);
    (void)to;
}

static void k8s_helm_emit_uses(cbm_pipeline_ctx_t *ctx, k8s_helm_t *h, const k8s_himgs_t *l) {
    for (int i = 0; i < l->n; i++) {
        const k8s_himg_t *x = &l->v[i];
        if (k8s_helm_literal_use(ctx, x->src, x->img)) {
            continue; /* a literal reference of the object wins */
        }
        char esc[CBM_SZ_1K];
        char cesc[CBM_SZ_512];
        cbm_json_escape(esc, (int)sizeof(esc), x->ref);
        cbm_json_escape(cesc, (int)sizeof(cesc), x->container ? x->container : "-");
        const char *props = cbm_arena_sprintf(
            &h->a, "{\"ref\":\"image\",\"image\":\"%s\",\"container\":\"%s\"%s%s}", esc, cesc,
            x->keys ? ",\"default_of\":" : "", x->keys ? x->keys : "");
        if (props) {
            cbm_gbuf_insert_edge(ctx->gbuf, x->src, x->img, "USAGE", props);
            h->edges++;
        }
    }
}

/* Chart `from` -DEPLOYS {via, ref: image, image, override_of}-> the images an
 * overlay changed: rendered there, absent from the defaults' images. */
static void k8s_helm_emit_overrides(cbm_pipeline_ctx_t *ctx, k8s_helm_t *h, int64_t from,
                                    const char *via, const k8s_himgs_t *dflt,
                                    const k8s_himgs_t *got, yyjson_mut_val *overlay,
                                    yyjson_mut_val *globals, const char *prefix,
                                    k8s_himgs_t *emitted) {
    for (int i = 0; i < got->n; i++) {
        const k8s_himg_t *x = &got->v[i];
        bool same = false;
        for (int j = 0; j < dflt->n && !same; j++) {
            same = dflt->v[j].src == x->src && dflt->v[j].img == x->img &&
                   strcmp(dflt->v[j].ref, x->ref) == 0;
        }
        const char *ovr = k8s_helm_overridden(h, x->keys, overlay, globals, prefix);
        if (same || !ovr || from <= 0) {
            continue;
        }
        k8s_himg_t e = {from, x->img, x->ref, NULL, ovr};
        int before = emitted->n;
        k8s_himg_push(emitted, e);
        if (emitted->n == before) {
            continue;
        }
        char esc[CBM_SZ_1K];
        cbm_json_escape(esc, (int)sizeof(esc), x->ref);
        const char *props = cbm_arena_sprintf(
            &h->a, "{\"via\":\"%s\",\"ref\":\"image\",\"image\":\"%s\",\"override_of\":%s}", via,
            esc, ovr);
        if (props) {
            cbm_gbuf_insert_edge(ctx->gbuf, from, x->img, "DEPLOYS", props);
            h->edges++;
        }
    }
}

/* The chart dirs enclosing chart dir `dir`, nearest first (arena). */
static int k8s_helm_ancestors(k8s_helm_t *h, k8s_bundles_t *b, const char *dir, const char ***out) {
    int n = 0;
    int cap = 0;
    const char **v = NULL;
    char cur[CBM_SZ_1K];
    snprintf(cur, sizeof(cur), "%s", dir);
    while (cur[0]) {
        char up[CBM_SZ_1K];
        if (!k8s_chart_of(b, cur, up, sizeof(up))) {
            break;
        }
        if (n == cap) {
            cap = cap ? cap * PAIR_LEN : CBM_SZ_16;
            const char **g = cbm_arena_alloc(&h->a, (size_t)cap * sizeof(*g));
            if (!g) {
                break;
            }
            if (n) {
                memcpy(g, v, (size_t)n * sizeof(*g));
            }
            v = g;
        }
        v[n++] = cbm_arena_strdup(&h->a, up);
        snprintf(cur, sizeof(cur), "%s", up);
    }
    *out = v;
    return n;
}

/* The dependency of `parent`'s Chart.yaml naming `sub` (by name), NULL: none. */
static yyjson_mut_val *k8s_helm_dep(const k8s_hchart_t *parent, const k8s_hchart_t *sub) {
    yyjson_mut_val *deps = yyjson_mut_is_obj(parent->chart_yaml)
                               ? yyjson_mut_obj_get(parent->chart_yaml, "dependencies")
                               : NULL;
    if (yyjson_mut_is_arr(deps) && sub->meta.name) {
        size_t idx, max;
        yyjson_mut_val *d;
        yyjson_mut_arr_foreach(deps, idx, max, d) {
            const char *name = k8s_helm_meta_str(d, "name");
            if (name && strcmp(name, sub->meta.name) == 0) {
                return d;
            }
        }
    }
    return NULL;
}

/* Is the subchart a dependency disables in the parent's values `vals` (its
 * `condition`: the first of its comma-separated paths that holds a bool)? */
static bool k8s_helm_disabled(k8s_helm_t *h, yyjson_mut_val *dep, yyjson_mut_val *vals) {
    const char *cond = dep ? k8s_helm_meta_str(dep, "condition") : NULL;
    for (const char *p = cond; p && *p;) {
        const char *comma = strchr(p, ',');
        size_t l = comma ? (size_t)(comma - p) : strlen(p);
        char *path = cbm_arena_strndup(&h->a, p, l);
        yyjson_mut_val *v = path ? k8s_helm_dig(vals, path) : NULL;
        if (yyjson_mut_is_bool(v)) {
            return !yyjson_mut_get_bool(v);
        }
        p += l + (comma ? SKIP_ONE : 0);
    }
    return false;
}

/* The values chart dir `dir` renders with when chart dir `top` deploys it
 * with values `top_vals` (NULL: top's defaults): down the charts between
 * them, each subchart's defaults with its parent's `<alias>:` values and
 * `global:` coalesced over them (Helm's coalesce; a subchart always has a
 * `global`). *path gets dir's key path from top ("" for top itself), *as_name
 * the name dir's chart is included under (its alias; NULL for top), *full its
 * chart path as Helm's ChartFullPath (top's name, then `/charts/<as_name>`
 * per chart down; NULL: not known). NULL: a dependency condition disables it,
 * or a values file does not parse. */
static yyjson_mut_val *k8s_helm_context(cbm_pipeline_ctx_t *ctx, k8s_helm_t *h, k8s_bundles_t *b,
                                        k8s_dep_link_t *l, const char *dir, const char *top,
                                        yyjson_mut_val *top_vals, const char **path,
                                        const char **as_name, const char **full) {
    const char **anc = NULL;
    int na = k8s_helm_ancestors(h, b, dir, &anc);
    int ti = -SKIP_ONE; /* top's index among the ancestors; na: dir itself */
    if (strcmp(dir, top) == 0) {
        ti = na;
    } else {
        for (int i = 0; i < na && ti < 0; i++) {
            ti = strcmp(anc[i], top) == 0 ? i : ti;
        }
    }
    *path = "";
    *as_name = NULL;
    *full = NULL;
    if (ti < 0) {
        return NULL;
    }
    k8s_hchart_t *cur_chart = k8s_helm_chart(ctx, h, l, top);
    if (!cur_chart || !cur_chart->values) {
        return NULL;
    }
    *full = cur_chart->meta.name;
    yyjson_mut_val *cur = top_vals ? cbm_helm_values_coalesce(h->doc, cur_chart->values, top_vals)
                                   : cur_chart->values;
    for (int i = (ti == na ? na : ti) - SKIP_ONE; i >= -SKIP_ONE && ti != na; i--) {
        k8s_hchart_t *child = k8s_helm_chart(ctx, h, l, i >= 0 ? anc[i] : dir);
        if (!child || !child->values) {
            return NULL;
        }
        yyjson_mut_val *dep = k8s_helm_dep(cur_chart, child);
        if (k8s_helm_disabled(h, dep, cur)) {
            return NULL;
        }
        const char *alias = dep ? k8s_helm_meta_str(dep, "alias") : NULL;
        alias = alias ? alias : child->meta.name;
        if (!alias) {
            return NULL;
        }
        *path = (*path)[0] ? cbm_arena_sprintf(&h->a, "%s.%s", *path, alias)
                           : cbm_arena_strdup(&h->a, alias);
        *as_name = alias;
        *full = *full ? cbm_arena_sprintf(&h->a, "%s/charts/%s", *full, alias) : NULL;
        yyjson_mut_val *sub = yyjson_mut_is_obj(cur) ? yyjson_mut_obj_get(cur, alias) : NULL;
        yyjson_mut_val *next = cbm_helm_values_coalesce(h->doc, child->values, sub);
        yyjson_mut_val *pg = yyjson_mut_is_obj(cur) ? yyjson_mut_obj_get(cur, "global") : NULL;
        yyjson_mut_val *cg = next ? yyjson_mut_obj_get(next, "global") : NULL;
        yyjson_mut_val *g = cbm_helm_values_coalesce(h->doc, cg, pg); /* the parent's win */
        if (!next || !g) {
            return NULL;
        }
        (void)yyjson_mut_obj_remove_key(next, "global");
        yyjson_mut_obj_add(next, yyjson_mut_strcpy(h->doc, "global"), g);
        cur = next;
        cur_chart = child;
    }
    return cur;
}

/* Render the templated containers [from, to) of one template with `vals`
 * and take each container's images. */
static void k8s_helm_images_of(cbm_pipeline_ctx_t *ctx, k8s_helm_t *h, k8s_bundles_t *b,
                               const k8s_hchart_t *c, const char *as_name, const char *full,
                               int from, int to, yyjson_mut_val *vals, const char *src, size_t len,
                               cbm_helm_probe_t *probes, int np, k8s_himgs_t *out) {
    k8s_helm_render(ctx, h, b, c, as_name, full, h->t, from, to, vals, src, len, probes, np);
    for (int i = from; i < to; i++) {
        k8s_helm_take(ctx, probes, np, h->t[i].start_line, h->t[i].end_line, h->t[i].node,
                      h->t[i].container, out);
    }
}

/* Every templated container's images with its chart family's defaults --
 * what `helm template <family root>` renders. */
static void k8s_helm_link_defaults(cbm_pipeline_ctx_t *ctx, k8s_bundles_t *b, k8s_helm_t *h) {
    if (h->nt == 0 || !ctx->repo_path) {
        return;
    }
    qsort(h->t, (size_t)h->nt, sizeof(*h->t), k8s_timg_cmp);
    k8s_dep_link_t l = {.ctx = ctx, .charts = k8s_index_by_file(ctx, "Chart")};
    k8s_himgs_t uses = {0};
    for (int from = 0; from < h->nt;) {
        int to = from + SKIP_ONE;
        while (to < h->nt && strcmp(h->t[to].file, h->t[from].file) == 0) {
            to++;
        }
        char dir[CBM_SZ_1K];
        char fam[CBM_SZ_1K];
        k8s_hchart_t *c = k8s_chart_of(b, h->t[from].file, dir, sizeof(dir))
                              ? k8s_helm_chart(ctx, h, &l, dir)
                              : NULL;
        const char *path = NULL;
        const char *as_name = NULL;
        const char *full = NULL;
        yyjson_mut_val *vals = NULL;
        if (c) {
            k8s_chart_family(b, c->dir, fam, sizeof(fam));
            vals = k8s_helm_context(ctx, h, b, &l, c->dir, fam, NULL, &path, &as_name, &full);
        }
        int len = 0;
        char *src = vals ? k8s_repo_read(ctx, h->t[from].file, &len) : NULL;
        cbm_helm_probe_t *probes = NULL;
        int np = src ? k8s_helm_probes(h, h->t, from, to, src, (size_t)len, &probes) : 0;
        if (np > 0) {
            k8s_helm_images_of(ctx, h, b, c, as_name, full, from, to, vals, src, (size_t)len,
                               probes, np, &uses);
        }
        cbm_free(CBM_MEM_CLASS_OTHER, src);
        from = to;
    }
    k8s_helm_emit_uses(ctx, h, &uses);
    h->dflt = uses; /* the release views compare against these */
    k8s_free_index(l.charts);
}

/* ── Release images ───────────────────────────────────────────────────
 *
 * A release (a Flux HelmRelease's spec.values, a kustomize helmCharts
 * entry's valuesFile + valuesInline, a Terraform helm_release's values +
 * set) renders its chart with its values over the defaults. Of a chart in
 * the repository the templates render again: each image that differs from
 * the defaults' is the release's own -- release -DEPLOYS {via, ref: image,
 * image, override_of: the keys it set that the image read}-> Image. A chart
 * outside the repository has no templates here: its images are read from the
 * release values by convention -- a mapping with `repository` (and `tag` /
 * `digest` / `registry`) whose reference names a registry host; one without
 * a host names none (no host is invented) -- {inferred: values, values_key}. */

/* The chart dir of local Chart node `cn` (a werf chart: its `.helm`). */
static bool k8s_helm_chart_dir(const cbm_gbuf_node_t *cn, char *out, size_t sz) {
    if (!cn->file_path) {
        return false;
    }
    k8s_dirname(cn->file_path, out, sz);
    const char *base = k8s_basename(cn->file_path);
    if (strcmp(base, "werf.yaml") == 0 || strcmp(base, "werf.yml") == 0) {
        size_t l = strlen(out);
        snprintf(out + l, sz - l, "%s.helm", l ? "/" : "");
    }
    return true;
}

/* A local chart's images rendered with release values `rv`, against the
 * defaults. */
static void k8s_helm_release_local(cbm_pipeline_ctx_t *ctx, k8s_bundles_t *b, k8s_helm_t *h,
                                   k8s_dep_link_t *l, int64_t src, const char *via, const char *top,
                                   yyjson_mut_val *rv, k8s_himgs_t *emitted) {
    size_t tl = strlen(top);
    for (int from = 0; from < h->nt;) {
        int to = from + SKIP_ONE;
        while (to < h->nt && strcmp(h->t[to].file, h->t[from].file) == 0) {
            to++;
        }
        char dir[CBM_SZ_1K];
        bool in =
            k8s_chart_of(b, h->t[from].file, dir, sizeof(dir)) &&
            (strcmp(dir, top) == 0 || (tl == 0 || (strncmp(dir, top, tl) == 0 && dir[tl] == '/')));
        k8s_hchart_t *c = in ? k8s_helm_chart(ctx, h, l, dir) : NULL;
        const char *path = NULL;
        const char *as_name = NULL;
        const char *full = NULL;
        yyjson_mut_val *vals =
            c ? k8s_helm_context(ctx, h, b, l, dir, top, rv, &path, &as_name, &full) : NULL;
        int len = 0;
        char *srcs = vals ? k8s_repo_read(ctx, h->t[from].file, &len) : NULL;
        cbm_helm_probe_t *probes = NULL;
        int np = srcs ? k8s_helm_probes(h, h->t, from, to, srcs, (size_t)len, &probes) : 0;
        if (np > 0) {
            k8s_himgs_t got = {0};
            k8s_helm_images_of(ctx, h, b, c, as_name, full, from, to, vals, srcs, (size_t)len,
                               probes, np, &got);
            yyjson_mut_val *overlay = path && path[0] ? k8s_helm_dig(rv, path) : rv;
            yyjson_mut_val *globals = yyjson_mut_obj_get(rv, "global");
            k8s_helm_emit_overrides(ctx, h, src, via, &h->dflt, &got, overlay, globals,
                                    path && path[0] ? path : NULL, emitted);
            k8s_himgs_free(&got);
        }
        cbm_free(CBM_MEM_CLASS_OTHER, srcs);
        from = to;
    }
}

/* Does image reference `ref` name a registry host (its first path part has
 * a `.` or `:`, or is localhost)? */
static bool k8s_helm_has_host(const char *ref) {
    const char *slash = strchr(ref, '/');
    if (!slash) {
        return false;
    }
    size_t fl = (size_t)(slash - ref);
    return memchr(ref, '.', fl) || memchr(ref, ':', fl) ||
           (fl == strlen("localhost") && strncmp(ref, "localhost", fl) == 0);
}

/* The images release values `v` name by convention (see above). */
static void k8s_helm_convention(cbm_pipeline_ctx_t *ctx, k8s_helm_t *h, int64_t src,
                                const char *via, yyjson_mut_val *v, const char *path,
                                k8s_himgs_t *emitted) {
    size_t idx, max;
    if (yyjson_mut_is_arr(v)) {
        yyjson_mut_val *it;
        yyjson_mut_arr_foreach(v, idx, max, it) {
            k8s_helm_convention(ctx, h, src, via, it,
                                cbm_arena_sprintf(&h->a, "%s[%zu]", path ? path : "", idx),
                                emitted);
        }
        return;
    }
    if (!yyjson_mut_is_obj(v)) {
        return;
    }
    yyjson_mut_val *repo = yyjson_mut_obj_get(v, "repository");
    if (yyjson_mut_is_str(repo) && yyjson_mut_get_len(repo) > 0) {
        char reg[CBM_SZ_512] = "";
        char tag[CBM_SZ_256] = "";
        char dig[CBM_SZ_256] = "";
        yyjson_mut_val *r = yyjson_mut_obj_get(v, "registry");
        yyjson_mut_val *tg = yyjson_mut_obj_get(v, "tag");
        yyjson_mut_val *dg = yyjson_mut_obj_get(v, "digest");
        bool ok = (!r || yyjson_mut_is_null(r) || cbm_helm_scalar_text(r, reg, sizeof(reg))) &&
                  (!tg || yyjson_mut_is_null(tg) || cbm_helm_scalar_text(tg, tag, sizeof(tag))) &&
                  (!dg || yyjson_mut_is_null(dg) || cbm_helm_scalar_text(dg, dig, sizeof(dig)));
        const char *ref = ok ? cbm_arena_sprintf(&h->a, "%s%s%s%s%s", reg, reg[0] ? "/" : "",
                                                 yyjson_mut_get_str(repo),
                                                 dig[0]   ? "@"
                                                 : tag[0] ? ":"
                                                          : "",
                                                 dig[0] ? dig : tag)
                             : NULL;
        int64_t img = ref && k8s_helm_has_host(ref) && cbm_image_ref_valid(ref)
                          ? cbm_pipeline_image_node(ctx, ref)
                          : 0;
        int before = emitted->n;
        if (img > 0) {
            k8s_himg_push(emitted, (k8s_himg_t){src, img, ref, NULL, NULL});
        }
        if (emitted->n > before) {
            char esc[CBM_SZ_1K];
            char kesc[CBM_SZ_1K];
            cbm_json_escape(esc, (int)sizeof(esc), ref);
            cbm_json_escape(kesc, (int)sizeof(kesc), path ? path : "");
            const char *props = cbm_arena_sprintf(
                &h->a,
                "{\"via\":\"%s\",\"ref\":\"image\",\"image\":\"%s\",\"inferred\":\"values\","
                "\"values_key\":\"%s\"}",
                via, esc, kesc);
            if (props) {
                cbm_gbuf_insert_edge(ctx->gbuf, src, img, "DEPLOYS", props);
                h->edges++;
            }
        }
    }
    yyjson_mut_val *k;
    yyjson_mut_val *child;
    yyjson_mut_obj_foreach(v, idx, max, k, child) {
        if (!yyjson_mut_is_obj(child) && !yyjson_mut_is_arr(child)) {
            continue;
        }
        const char *key = yyjson_mut_get_str(k);
        const char *cp = path && path[0] ? cbm_arena_sprintf(&h->a, "%s.%s", path, key)
                                         : cbm_arena_strdup(&h->a, key);
        k8s_helm_convention(ctx, h, src, via, child, cp, emitted);
    }
}

/* One release of Chart node `chart` with values `rv`. */
static void k8s_helm_release(cbm_pipeline_ctx_t *ctx, k8s_bundles_t *b, k8s_helm_t *h,
                             k8s_dep_link_t *l, int64_t src, const char *via, int64_t chart,
                             yyjson_mut_val *rv) {
    const cbm_gbuf_node_t *cn = chart > 0 ? cbm_gbuf_find_by_id(ctx->gbuf, chart) : NULL;
    if (!cn || !yyjson_mut_is_obj(rv) || src <= 0) {
        return;
    }
    k8s_himgs_t emitted = {0};
    char dir[CBM_SZ_1K];
    if (cbm_pipeline_chart_is_external(cn)) {
        k8s_helm_convention(ctx, h, src, via, rv, NULL, &emitted);
    } else if (k8s_helm_chart_dir(cn, dir, sizeof(dir)) && b->charts &&
               cbm_ht_get(b->charts, dir)) {
        k8s_helm_release_local(ctx, b, h, l, src, via, dir, rv, &emitted);
    }
    k8s_himgs_free(&emitted);
}

/* A node's document (lines start_line..end_line of its file) as values. */
static yyjson_mut_val *k8s_helm_node_yaml(cbm_pipeline_ctx_t *ctx, k8s_helm_t *h,
                                          const cbm_gbuf_node_t *n) {
    int len = 0;
    char *src = n->file_path ? k8s_repo_read(ctx, n->file_path, &len) : NULL;
    if (!src) {
        return NULL;
    }
    size_t b = 0;
    size_t e = (size_t)len;
    int line = SKIP_ONE;
    for (size_t i = 0; i < (size_t)len && n->start_line > 0; i++) {
        if (src[i] != '\n') {
            continue;
        }
        line++;
        if (line == n->start_line) {
            b = i + SKIP_ONE;
        }
        if (n->end_line > 0 && line == n->end_line + SKIP_ONE) {
            e = i;
            break;
        }
    }
    yyjson_mut_val *v = b <= e ? cbm_helm_values_parse(h->doc, src + b, e - b) : NULL;
    cbm_free(CBM_MEM_CLASS_OTHER, src);
    return v;
}

/* String property `key` of edge properties `props` into out; false: none. */
static bool k8s_json_str(const char *props, const char *key, char *out, size_t sz) {
    out[0] = '\0';
    yyjson_doc *d = props ? yyjson_read(props, strlen(props), 0) : NULL;
    yyjson_val *v = d ? yyjson_obj_get(yyjson_doc_get_root(d), key) : NULL;
    bool ok = yyjson_is_str(v);
    if (ok) {
        snprintf(out, sz, "%s", yyjson_get_str(v));
    }
    if (d) {
        yyjson_doc_free(d);
    }
    return ok;
}

/* Does field `key` of `obj` give a value (a non-null scalar, a non-empty list
 * or map)? */
static bool k8s_helm_gives(yyjson_mut_val *obj, const char *key) {
    yyjson_mut_val *v = yyjson_mut_is_obj(obj) ? yyjson_mut_obj_get(obj, key) : NULL;
    if (!v || yyjson_mut_is_null(v)) {
        return false;
    }
    if (yyjson_mut_is_arr(v)) {
        return yyjson_mut_arr_size(v) > 0;
    }
    if (yyjson_mut_is_obj(v)) {
        return yyjson_mut_obj_size(v) > 0;
    }
    return !yyjson_mut_is_str(v) || yyjson_mut_get_len(v) > 0;
}

/* A HelmRelease's values: spec.values; NULL when there are none -- or when a
 * source not read also gives some (valuesFrom, the chart's valuesFiles, a
 * postRenderer that may patch the images): its images are then unknown. */
static yyjson_mut_val *k8s_helm_flux_values(yyjson_mut_val *docv) {
    yyjson_mut_val *spec = yyjson_mut_is_obj(docv) ? yyjson_mut_obj_get(docv, "spec") : NULL;
    yyjson_mut_val *chart = yyjson_mut_is_obj(spec) ? yyjson_mut_obj_get(spec, "chart") : NULL;
    yyjson_mut_val *cspec = yyjson_mut_is_obj(chart) ? yyjson_mut_obj_get(chart, "spec") : NULL;
    if (k8s_helm_gives(spec, "valuesFrom") || k8s_helm_gives(spec, "postRenderers") ||
        k8s_helm_gives(cspec, "valuesFiles") || k8s_helm_gives(cspec, "valuesFile")) {
        return NULL;
    }
    return yyjson_mut_is_obj(spec) ? yyjson_mut_obj_get(spec, "values") : NULL;
}

/* A kustomize helmCharts entry's values: valuesFile, then valuesInline over
 * it; NULL when neither -- or when a part is not read (additionalValuesFiles,
 * a valuesMerge other than the default override, a valuesFile that cannot be
 * read): its images are then unknown. */
static yyjson_mut_val *k8s_helm_kustomize_values(cbm_pipeline_ctx_t *ctx, k8s_helm_t *h,
                                                 const cbm_gbuf_node_t *kust, const char *props) {
    char chart[CBM_SZ_512] = "";
    char release[CBM_SZ_512] = "";
    char vfile[CBM_SZ_1K] = "";
    (void)k8s_json_str(props, "chart", chart, sizeof(chart));
    (void)k8s_json_str(props, "release", release, sizeof(release));
    (void)k8s_json_str(props, "values", vfile, sizeof(vfile));
    int len = 0;
    char *src = kust->file_path ? k8s_repo_read(ctx, kust->file_path, &len) : NULL;
    yyjson_mut_val *doc = src ? cbm_helm_values_parse(h->doc, src, (size_t)len) : NULL;
    cbm_free(CBM_MEM_CLASS_OTHER, src);
    /* kustomize runs the kustomization's own transformers over the inflated
     * chart: images / patches / replacements may change what it deploys */
    static const char *const TRANSFORMS[] = {
        "images",       "patches",      "patchesJson6902", "patchesStrategicMerge",
        "replacements", "transformers", "components",      NULL};
    for (int i = 0; TRANSFORMS[i]; i++) {
        if (k8s_helm_gives(doc, TRANSFORMS[i])) {
            return NULL;
        }
    }
    yyjson_mut_val *charts = yyjson_mut_is_obj(doc) ? yyjson_mut_obj_get(doc, "helmCharts") : NULL;
    yyjson_mut_val *inline_vals = NULL;
    size_t idx, max;
    yyjson_mut_val *c;
    yyjson_mut_arr_foreach(charts, idx, max, c) {
        const char *name = k8s_helm_meta_str(c, "name");
        const char *rel = k8s_helm_meta_str(c, "releaseName");
        if (name && strcmp(name, chart) == 0 &&
            (!release[0] || (rel && strcmp(rel, release) == 0))) {
            const char *merge = k8s_helm_meta_str(c, "valuesMerge");
            if (k8s_helm_gives(c, "additionalValuesFiles") ||
                (merge && strcmp(merge, "override") != 0)) {
                return NULL;
            }
            inline_vals = yyjson_mut_obj_get(c, "valuesInline");
            break;
        }
    }
    yyjson_mut_val *file_vals = NULL;
    if (vfile[0]) {
        int fl = 0;
        char *fs = k8s_repo_read(ctx, vfile, &fl);
        file_vals = fs ? cbm_helm_values_parse(h->doc, fs, (size_t)fl) : NULL;
        cbm_free(CBM_MEM_CLASS_OTHER, fs);
        if (!file_vals) {
            return NULL; /* a valuesFile not read: the release's values would be partial */
        }
    }
    if (!inline_vals && !file_vals) {
        return NULL;
    }
    return cbm_helm_values_coalesce(h->doc, file_vals, inline_vals);
}

/* Every release's images: HelmReleases and kustomize helmCharts from their
 * DEPLOYS edges to a Chart, Terraform helm_releases from the context. */
static void k8s_helm_link_releases(cbm_pipeline_ctx_t *ctx, k8s_bundles_t *b, k8s_helm_t *h) {
    k8s_dep_link_t l = {.ctx = ctx, .charts = k8s_index_by_file(ctx, "Chart")};
    const cbm_gbuf_edge_t **es = NULL;
    int n = 0;
    if (ctx->repo_path && cbm_gbuf_find_edges_by_type(ctx->gbuf, "DEPLOYS", &es, &n) == 0) {
        /* collected first: release edges are DEPLOYS too */
        typedef struct {
            int64_t src;
            int64_t tgt;
            const char *props;
        } k8s_rel_t;
        k8s_rel_t *rels = n ? cbm_arena_alloc(&h->a, (size_t)n * sizeof(*rels)) : NULL;
        int nr = 0;
        for (int i = 0; rels && i < n; i++) {
            const char *p = es[i]->properties_json;
            if (p &&
                (strstr(p, "\"via\":\"helmrelease\"") || strstr(p, "\"via\":\"helmcharts\"")) &&
                !strstr(p, "\"ref\":")) {
                rels[nr++] =
                    (k8s_rel_t){es[i]->source_id, es[i]->target_id, cbm_arena_strdup(&h->a, p)};
            }
        }
        for (int i = 0; i < nr; i++) {
            const cbm_gbuf_node_t *src = cbm_gbuf_find_by_id(ctx->gbuf, rels[i].src);
            if (!src) {
                continue;
            }
            bool hr = strstr(rels[i].props, "\"via\":\"helmrelease\"") != NULL;
            yyjson_mut_val *rv = NULL;
            if (hr) {
                rv = k8s_helm_flux_values(k8s_helm_node_yaml(ctx, h, src));
            } else {
                rv = k8s_helm_kustomize_values(ctx, h, src, rels[i].props);
            }
            k8s_helm_release(ctx, b, h, &l, rels[i].src, hr ? "helmrelease" : "helmcharts",
                             rels[i].tgt, rv);
        }
    }
    for (int i = 0; i < ctx->tf_release_n; i++) {
        cbm_tf_release_t *r = &ctx->tf_releases[i];
        if (r->values) {
            yyjson_doc *jd = yyjson_read(r->values, strlen(r->values), 0);
            yyjson_mut_val *rv = jd ? yyjson_val_mut_copy(h->doc, yyjson_doc_get_root(jd)) : NULL;
            if (jd) {
                yyjson_doc_free(jd);
            }
            k8s_helm_release(ctx, b, h, &l, r->src, "terraform", r->chart, rv);
            cbm_free(CBM_MEM_CLASS_OTHER, r->values);
        }
    }
    cbm_free(CBM_MEM_CLASS_OTHER, ctx->tf_releases);
    ctx->tf_releases = NULL;
    ctx->tf_release_n = 0;
    ctx->tf_release_cap = 0;
    k8s_free_index(l.charts);
}

static void k8s_helm_init(k8s_helm_t *h) {
    *h = (k8s_helm_t){0};
    cbm_arena_init_lazy(&h->a, CBM_SZ_4K);
    h->doc = yyjson_mut_doc_new(cbm_json_writer_allocator());
    h->charts = cbm_ht_create(0);
    h->defines = cbm_ht_create(0);
}

static void k8s_helm_free(k8s_helm_t *h) {
    if (h->defines) {
        cbm_ht_foreach(h->defines, k8s_helm_free_defines, NULL);
        cbm_ht_free(h->defines);
    }
    if (h->charts) {
        cbm_ht_free(h->charts);
    }
    if (h->doc) {
        yyjson_mut_doc_free(h->doc);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, h->t);
    k8s_himgs_free(&h->dflt);
    cbm_arena_destroy(&h->a);
    if (h->edges > 0) {
        cbm_log_info("pass.k8s.helm_images", "edges", itoa_k8s(h->edges));
    }
    *h = (k8s_helm_t){0};
}

/* Each Image a chart's objects use (their USAGE {ref: image}, after
 * pass_docker.c moved references onto component Images): Chart -DEPLOYS
 * {via: chart, ref: image}-> it -- {default_of: the values keys} when every
 * object of the chart uses it by a rendered default (the keys unioned), none
 * when one names it literally. Edges are collected first: inserting while
 * walking a chart's own DEPLOYS edges would grow the list being read. */
typedef struct {
    int64_t chart;
    int64_t img;
    bool literal;
    bool lost;  /* a mark that could not be carried whole: no edge, never an unmarked one */
    char *keys; /* JSON array (cbm_alloc); NULL: none yet */
} k8s_cimg_t;

/* JSON array `a` (owned, may be NULL) with the strings of JSON array value
 * `bv` appended once; the result is owned. *lost when one could not be. */
static char *k8s_keys_union(char *a, yyjson_val *bv, bool *lost) {
    size_t idx, max;
    yyjson_val *k;
    yyjson_arr_foreach(bv, idx, max, k) {
        const char *s = yyjson_get_str(k);
        if (!s) {
            continue;
        }
        /* JSON escaping writes at most 6 bytes (\u00XX) a byte */
        size_t qn = strlen(s) * 6 + PAIR_LEN + SKIP_ONE;
        char *quoted = cbm_alloc(CBM_MEM_CLASS_OTHER, qn);
        if (!quoted) {
            *lost = true;
            break;
        }
        quoted[0] = '"';
        cbm_json_escape(quoted + SKIP_ONE, (int)(qn - PAIR_LEN), s);
        size_t ql = strlen(quoted);
        quoted[ql++] = '"';
        quoted[ql] = '\0';
        if (a && strstr(a, quoted)) {
            cbm_free(CBM_MEM_CLASS_OTHER, quoted);
            continue;
        }
        size_t al = a ? strlen(a) : 0;
        char *g = cbm_alloc(CBM_MEM_CLASS_OTHER, al + ql + 3);
        if (!g) {
            cbm_free(CBM_MEM_CLASS_OTHER, quoted);
            *lost = true;
            break;
        }
        if (a) {
            snprintf(g, al + ql + 3, "%.*s,%s]", (int)(al - SKIP_ONE), a, quoted);
        } else {
            snprintf(g, ql + 3, "[%s]", quoted);
        }
        cbm_free(CBM_MEM_CLASS_OTHER, quoted);
        cbm_free(CBM_MEM_CLASS_OTHER, a);
        a = g;
    }
    return a;
}

static void k8s_link_chart_images(cbm_pipeline_ctx_t *ctx) {
    const cbm_gbuf_node_t **charts = NULL;
    int nc = 0;
    if (cbm_gbuf_find_by_label(ctx->gbuf, "Chart", &charts, &nc) != 0 || nc == 0) {
        return;
    }
    k8s_cimg_t *v = NULL;
    int n = 0;
    int cap = 0;
    for (int c = 0; c < nc; c++) {
        const cbm_gbuf_edge_t **dep = NULL;
        int nd = 0;
        if (cbm_gbuf_find_edges_by_source_type(ctx->gbuf, charts[c]->id, "DEPLOYS", &dep, &nd) !=
            0) {
            continue;
        }
        for (int d = 0; d < nd; d++) {
            const char *dp = dep[d]->properties_json;
            if (!dp || !strstr(dp, "\"via\":\"chart\"") || strstr(dp, "\"ref\":")) {
                continue;
            }
            const cbm_gbuf_edge_t **use = NULL;
            int nu = 0;
            if (cbm_gbuf_find_edges_by_source_type(ctx->gbuf, dep[d]->target_id, "USAGE", &use,
                                                   &nu) != 0) {
                continue;
            }
            for (int u = 0; u < nu; u++) {
                const cbm_gbuf_node_t *img = cbm_gbuf_find_by_id(ctx->gbuf, use[u]->target_id);
                const char *up = use[u]->properties_json;
                if (!img || !img->label || strcmp(img->label, "Image") != 0 || !up ||
                    !strstr(up, "\"ref\":\"image\"")) {
                    continue;
                }
                int at = -SKIP_ONE;
                for (int i = n - SKIP_ONE; i >= 0 && at < 0; i--) {
                    at = v[i].chart == charts[c]->id && v[i].img == img->id ? i : at;
                }
                if (at < 0) {
                    if (!k8s_grow((void **)&v, &cap, n, sizeof(*v))) {
                        break;
                    }
                    v[n] = (k8s_cimg_t){charts[c]->id, img->id, false, false, NULL};
                    at = n++;
                }
                /* the USAGE's default_of, read as JSON (a key path may hold `]`) */
                yyjson_doc *ud = yyjson_read(up, strlen(up), 0);
                yyjson_val *dv = ud ? yyjson_obj_get(yyjson_doc_get_root(ud), "default_of") : NULL;
                if (!ud) {
                    v[at].lost = true;
                } else if (!dv) {
                    v[at].literal = true;
                } else if (yyjson_is_arr(dv)) {
                    v[at].keys = k8s_keys_union(v[at].keys, dv, &v[at].lost);
                } else {
                    v[at].lost = true;
                }
                if (ud) {
                    yyjson_doc_free(ud);
                }
            }
        }
    }
    for (int i = 0; i < n; i++) {
        bool marked = !v[i].literal && v[i].keys;
        if (!v[i].literal && v[i].lost) {
            cbm_free(CBM_MEM_CLASS_OTHER, v[i].keys);
            continue; /* the mark is not whole: no edge rather than an unmarked one */
        }
        size_t pn = (marked ? strlen(v[i].keys) : 0) + CBM_SZ_64;
        char *props = cbm_alloc(CBM_MEM_CLASS_OTHER, pn);
        if (props) {
            snprintf(props, pn, "{\"via\":\"chart\",\"ref\":\"image\"%s%s}",
                     marked ? ",\"default_of\":" : "", marked ? v[i].keys : "");
            cbm_gbuf_insert_edge(ctx->gbuf, v[i].chart, v[i].img, "DEPLOYS", props);
            cbm_free(CBM_MEM_CLASS_OTHER, props);
        }
        cbm_free(CBM_MEM_CLASS_OTHER, v[i].keys);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, v);
}

/* A HelmRelease whose chartRef names OCIRepository `oci` (its USAGE edge
 * {"ref":"chartRef"}, resolved with the references) deploys the chart that
 * repository fetches: the external Chart of that OCI reference (its last
 * segment the chart, the rest its repository). */
static void k8s_link_chart_refs(k8s_dep_link_t *l, const k8s_hr_t *oci) {
    const char *url = oci->chart;
    size_t end = strlen(url);
    while (end > 0 && url[end - SKIP_ONE] == '/') {
        end--;
    }
    size_t start = end;
    while (start > 0 && url[start - SKIP_ONE] != '/') {
        start--;
    }
    if (start == 0 || start == end || end - start >= CBM_SZ_256) {
        return;
    }
    char name[CBM_SZ_256];
    snprintf(name, sizeof(name), "%.*s", (int)(end - start), url + start);
    char esc_name[CBM_SZ_512];
    char esc_url[CBM_SZ_1K];
    cbm_json_escape(esc_name, (int)sizeof(esc_name), name);
    cbm_json_escape(esc_url, (int)sizeof(esc_url), url);
    char props[CBM_SZ_2K];
    snprintf(props, sizeof(props),
             "{\"via\":\"helmrelease\",\"chart\":\"%s\",\"external\":true,\"url\":\"%s\"}",
             esc_name, esc_url);
    char repo[CBM_SZ_1K];
    const char *repo_url = cbm_chart_ref_repo(url, repo, sizeof(repo)) ? repo : NULL;
    const cbm_gbuf_edge_t **in = NULL;
    int n = 0;
    if (cbm_gbuf_find_edges_by_target_type(l->ctx->gbuf, oci->node, "USAGE", &in, &n) != 0) {
        return;
    }
    for (int i = 0; i < n; i++) {
        atomic_fetch_add_explicit(&g_k8s_candidates, SKIP_ONE, memory_order_relaxed);
        const char *p = in[i]->properties_json;
        const cbm_gbuf_node_t *hr = cbm_gbuf_find_by_id(l->ctx->gbuf, in[i]->source_id);
        if (p && strstr(p, "\"ref\":\"chartRef\"") && hr && hr->file_path) {
            k8s_deploys_edge(l, hr->id, k8s_external_chart(l, name, repo_url, hr->file_path),
                             props);
        }
    }
}

/* `, "<key>":"<val>"` (escaped) at out + *w when `val` is set. */
static void k8s_prop_add(char *out, size_t out_sz, int *w, const char *key, const char *val) {
    if (!val || *w < 0 || (size_t)*w >= out_sz) {
        return;
    }
    char esc[CBM_SZ_1K];
    cbm_json_escape(esc, (int)sizeof(esc), val);
    *w += snprintf(out + *w, out_sz - (size_t)*w, ",\"%s\":\"%s\"", key, esc);
}

/* A chart a kustomization inflates (`helmCharts`) is deployed with its
 * build: DEPLOYS {via: helmcharts, external, chart, url, version, release,
 * namespace, values, line} from the kustomization (its Kustomization
 * Resource, else its File -- the unit a Flux / Argo deploy reaches) to the
 * external Chart of that repository and name. */
static void k8s_link_kcharts(k8s_dep_link_t *l, k8s_bundles_t *b, const k8s_facts_t *f) {
    for (int i = 0; i < f->kchart_n; i++) {
        const k8s_kchart_t *c = &f->kcharts[i];
        if (!c->chart || strchr(c->chart, '/') || strchr(c->chart, '$')) {
            continue;
        }
        int64_t src = k8s_first_id(l->kusts, c->kust);
        src = src > 0 ? src : k8s_first_id(l->files, c->kust);
        /* No repo: the chart in the kustomization's chartHome (charts/). */
        int64_t local = 0;
        if (!c->url || !c->url[0]) {
            char kdir[CBM_SZ_1K];
            char cdir[CBM_SZ_1K];
            k8s_dirname(c->kust, kdir, sizeof(kdir));
            snprintf(cdir, sizeof(cdir), "%s%scharts/%s", kdir, kdir[0] ? "/" : "", c->chart);
            local = b->charts && cbm_ht_get(b->charts, cdir) ? k8s_chart_node(l, cdir) : 0;
        }
        char props[CBM_SZ_4K];
        int w = snprintf(props, sizeof(props), "{\"via\":\"helmcharts\"%s",
                         local > 0 ? "" : ",\"external\":true");
        k8s_prop_add(props, sizeof(props), &w, "chart", c->chart);
        k8s_prop_add(props, sizeof(props), &w, "url", c->url);
        k8s_prop_add(props, sizeof(props), &w, "version", c->version);
        k8s_prop_add(props, sizeof(props), &w, "release", c->release);
        k8s_prop_add(props, sizeof(props), &w, "namespace", c->ns);
        k8s_prop_add(props, sizeof(props), &w, "values", c->values);
        if (w < 0 || (size_t)w >= sizeof(props) - CBM_SZ_32) {
            continue;
        }
        snprintf(props + w, sizeof(props) - (size_t)w, ",\"line\":%d}", c->line);
        k8s_deploys_edge(
            l, src, local > 0 ? local : k8s_external_chart(l, c->chart, c->url, c->kust), props);
    }
}

/* A container image an object runs: USAGE {ref:image, container} to its
 * Image node -- the node a Dockerfile building or building on that image
 * joins; the container names whose environment runs it. */
static void k8s_link_images(cbm_pipeline_ctx_t *ctx, const k8s_facts_t *f) {
    for (int i = 0; i < f->img_n; i++) {
        const k8s_img_t *im = &f->imgs[i];
        int64_t img = cbm_pipeline_image_node(ctx, im->image);
        if (img <= 0) {
            continue;
        }
        char esc[CBM_SZ_1K];
        cbm_json_escape(esc, (int)sizeof(esc), im->image);
        char cesc[CBM_SZ_512];
        cbm_json_escape(cesc, (int)sizeof(cesc), im->container ? im->container : "-");
        char props[CBM_SZ_2K];
        snprintf(props, sizeof(props), "{\"ref\":\"image\",\"image\":\"%s\",\"container\":\"%s\"}",
                 esc, cesc);
        cbm_gbuf_insert_edge(ctx->gbuf, im->node, img, "USAGE", props);
    }
}

/* The environment variables an object's containers set: container-scoped
 * EnvVar nodes the object CONFIGURES {strategy:env_set}. */
static void k8s_link_envs(cbm_pipeline_ctx_t *ctx, const k8s_facts_t *f) {
    for (int i = 0; i < f->env_n; i++) {
        const k8s_env_t *e = &f->envs[i];
        const cbm_gbuf_node_t *owner = cbm_gbuf_find_by_id(ctx->gbuf, e->node);
        if (owner) {
            cbm_pipeline_env_setter(ctx, owner, e->container ? e->container : "-", e->key,
                                    e->source, e->line);
        }
    }
}

/* ── Kustomize patches (PHASE 5) ───────────────────────────────────── */

static char *k8s_dup_opt(const char *s) {
    return s ? cbm_mem_strdup(CBM_MEM_CLASS_OTHER, s) : NULL;
}

/* The repository path of `rel`, relative to directory `dir`; false when it
 * leaves the repository (or is a URL). */
static bool k8s_repo_path(const char *dir, const char *rel, char *out, size_t out_sz) {
    char joined[CBM_SZ_2K];
    snprintf(joined, sizeof(joined), "%s%s%s", dir, dir[0] ? "/" : "", rel);
    return k8s_norm_dir(joined, out, out_sz);
}

static void k8s_kchart_keep(k8s_facts_t *f, const char *kust, const CBMK8sFact *x,
                            const char *values) {
    if (!k8s_grow((void **)&f->kcharts, &f->kchart_cap, f->kchart_n, sizeof(*f->kcharts))) {
        return;
    }
    f->kcharts[f->kchart_n++] = (k8s_kchart_t){.kust = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, kust),
                                               .chart = k8s_dup_opt(x->name),
                                               .url = k8s_dup_opt(x->url),
                                               .version = k8s_dup_opt(x->version),
                                               .release = k8s_dup_opt(x->release),
                                               .ns = k8s_dup_opt(x->ns),
                                               .values = k8s_dup_opt(values),
                                               .line = x->line};
}

/* Keep a kustomization's facts: its patches (PATCH) and the charts it
 * inflates (DEPLOY via "kustomize"), each file as a repository path -- a
 * patch file outside the repository (or a URL) names no file of it, a values
 * file there is left out. */
static void k8s_kustomize_keep(k8s_facts_t *f, const char *kust, const CBMFileResult *res) {
    char dir[CBM_SZ_1K];
    k8s_dirname(kust, dir, sizeof(dir));
    for (int i = 0; res && i < res->k8s_facts.count; i++) {
        const CBMK8sFact *x = &res->k8s_facts.items[i];
        bool chart = x->fact == CBM_K8S_FACT_DEPLOY && x->via && strcmp(x->via, "kustomize") == 0;
        if (x->fact != CBM_K8S_FACT_PATCH && !chart) {
            continue;
        }
        char path[CBM_SZ_1K];
        bool in_repo = x->path && k8s_repo_path(dir, x->path, path, sizeof(path));
        if (chart) {
            k8s_kchart_keep(f, kust, x, in_repo ? path : NULL);
            continue;
        }
        if ((x->path && !in_repo) ||
            !k8s_grow((void **)&f->patches, &f->patch_cap, f->patch_n, sizeof(*f->patches))) {
            continue;
        }
        f->patches[f->patch_n++] =
            (k8s_patch_t){.kust = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, kust),
                          .path = x->path ? cbm_mem_strdup(CBM_MEM_CLASS_OTHER, path) : NULL,
                          .kind = k8s_dup_opt(x->kind),
                          .group = k8s_dup_opt(x->group),
                          .version = k8s_dup_opt(x->version),
                          .name = k8s_dup_opt(x->name),
                          .ns = k8s_dup_opt(x->ns),
                          .selector = k8s_dup_opt(x->selector),
                          .line = x->line};
    }
}

static int k8s_cmp_patch(const void *a, const void *b) {
    const k8s_patch_t *x = a;
    const k8s_patch_t *y = b;
    int c = strcmp(x->kust, y->kust);
    return c ? c : (x->line > y->line) - (x->line < y->line);
}

/* A Resource's kind and name, from its node name "Kind/name". */
static bool k8s_kind_name(const cbm_gbuf_node_t *n, char *kind, size_t kind_sz, const char **name) {
    const char *slash = n->name ? strchr(n->name, '/') : NULL;
    if (!slash) {
        return false;
    }
    snprintf(kind, kind_sz, "%.*s", (int)(slash - n->name), n->name);
    *name = slash + SKIP_ONE;
    return true;
}

/* What a patch names: kind / group / version / name / ns (NULL: any). A
 * name compares as text: a target's is a regular expression in kustomize,
 * which an object name (DNS characters, `.` matching itself) meets as text
 * alone -- a pattern (`web-.*`) names nothing here. */
typedef struct {
    const char *kind;
    const char *group;
    const char *version;
    const char *name;
    const char *ns;
    const char *selector;
} k8s_want_t;

/* Does label list `labels` ("k=v,k2=v2") carry `item` ("k=v") whole, or --
 * `item` ending in "=" -- any value of that key? */
static bool k8s_labels_have(const char *labels, const char *item) {
    size_t il = strlen(item);
    bool any_value = il > 0 && item[il - SKIP_ONE] == '=';
    for (const char *p = labels; p && *p;) {
        size_t l = strcspn(p, ",");
        if (any_value ? (l >= il && strncmp(p, item, il) == 0)
                      : (l == il && strncmp(p, item, il) == 0)) {
            return true;
        }
        p = p[l] ? p + l + SKIP_ONE : NULL;
    }
    return false;
}

/* Does Resource `t` meet label selector `sel`: every term `k=v` / `k==v`
 * (carried), `k!=v` (not carried), `k` (the key present), `!k` (absent)? A
 * set-based term (`k in (a,b)`) names no key and meets nothing. */
static bool k8s_selector_meets(const cbm_gbuf_node_t *t, const char *sel) {
    char *labels = k8s_node_prop_dup(t, "labels");
    bool ok = true;
    for (const char *p = sel; ok && p && *p;) {
        size_t l = strcspn(p, ",");
        char term[CBM_SZ_512];
        snprintf(term, sizeof(term), "%.*s", (int)l, p);
        char *s = term;
        while (*s == ' ') {
            s++;
        }
        for (char *e = s + strlen(s); e > s && e[-SKIP_ONE] == ' '; e--) {
            e[-SKIP_ONE] = '\0';
        }
        char item[CBM_SZ_512];
        char *eq = strstr(s, "!=");
        if (eq) {
            snprintf(item, sizeof(item), "%.*s=%s", (int)(eq - s), s, eq + PAIR_LEN);
            ok = !k8s_labels_have(labels, item);
        } else if ((eq = strchr(s, '='))) {
            const char *v = eq[SKIP_ONE] == '=' ? eq + PAIR_LEN : eq + SKIP_ONE;
            snprintf(item, sizeof(item), "%.*s=%s", (int)(eq - s), s, v);
            ok = k8s_labels_have(labels, item);
        } else if (s[0] == '!') {
            snprintf(item, sizeof(item), "%s=", s + SKIP_ONE);
            ok = !k8s_labels_have(labels, item);
        } else {
            snprintf(item, sizeof(item), "%s=", s);
            ok = s[0] && k8s_labels_have(labels, item);
        }
        p = p[l] ? p + l + SKIP_ONE : NULL;
    }
    cbm_free(CBM_MEM_CLASS_OTHER, labels);
    return ok;
}

/* Is Resource `t` what `w` names? A namespace meets the target's declared
 * one or its effective one. */
static bool k8s_patch_meets(const cbm_gbuf_node_t *t, const k8s_want_t *w) {
    char tkind[K8S_LABEL_LEN];
    const char *tname = NULL;
    if (!t->label || strcmp(t->label, "Resource") != 0 ||
        !k8s_kind_name(t, tkind, sizeof(tkind), &tname)) {
        return false;
    }
    if (w->kind && strcmp(w->kind, tkind) != 0) {
        return false;
    }
    if (w->name && strcmp(w->name, tname) != 0) {
        return false;
    }
    char prop[K8S_LABEL_LEN];
    if (w->group) {
        k8s_node_prop(t, "group", prop, sizeof(prop));
        if (strcmp(w->group, prop) != 0) {
            return false;
        }
    }
    if (w->version) {
        k8s_node_prop(t, "version", prop, sizeof(prop));
        if (strcmp(w->version, prop) != 0) {
            return false;
        }
    }
    if (w->ns) {
        char decl[K8S_LABEL_LEN];
        k8s_declared_ns(t->qualified_name, decl, sizeof(decl));
        k8s_effective_ns(t, prop, sizeof(prop));
        if (strcmp(w->ns, decl) != 0 && strcmp(w->ns, prop) != 0) {
            return false;
        }
    }
    return !w->selector || k8s_selector_meets(t, w->selector);
}

static void k8s_patch_edge(cbm_pipeline_ctx_t *ctx, int64_t src, int64_t tgt,
                           const k8s_patch_t *p) {
    if (src <= 0 || tgt <= 0 || src == tgt) {
        return;
    }
    char ek[CBM_SZ_2K];
    char ep[CBM_SZ_2K];
    cbm_json_escape(ek, (int)sizeof(ek), p->kust);
    cbm_json_escape(ep, (int)sizeof(ep), p->path ? p->path : "inline");
    char props[CBM_SZ_4K + CBM_SZ_1K];
    snprintf(props, sizeof(props),
             "{\"via\":\"patch\",\"kustomization\":\"%s\",\"patch\":\"%s\",\"line\":%d}", ek, ep,
             p->line);
    cbm_gbuf_insert_edge(ctx->gbuf, src, tgt, "OVERRIDE", props);
}

/* The build a patch acts in: its manifests, their Resources by name
 * (`names`: "Kind/name" -> ids, this build's only -- a repository-wide name
 * would visit every copy of a name for every patch, quadratic in copies) and
 * every Resource by file. */
typedef struct {
    cbm_pipeline_ctx_t *ctx;
    const k8s_paths_t *files;
    CBMHashTable *names;
    CBMHashTable *by_file;
} k8s_build_t;

/* Draw `p` from `src` to each object of the build `w` names. A named kind
 * is looked up by name, anything else walks the build. */
static void k8s_patch_targets(const k8s_build_t *b, const k8s_patch_t *p, int64_t src,
                              const k8s_want_t *w) {
    cbm_gbuf_t *gb = b->ctx->gbuf;
    if (w->kind && w->name) {
        char key[CBM_SZ_1K];
        snprintf(key, sizeof(key), "%s/%s", w->kind, w->name);
        const k8s_ids_t *ids = cbm_ht_get(b->names, key);
        for (int i = 0; ids && i < ids->count; i++) {
            const cbm_gbuf_node_t *t = cbm_gbuf_find_by_id(gb, ids->items[i]);
            atomic_fetch_add_explicit(&g_k8s_candidates, SKIP_ONE, memory_order_relaxed);
            if (t && k8s_patch_meets(t, w)) {
                k8s_patch_edge(b->ctx, src, t->id, p);
            }
        }
        return;
    }
    for (int f = 0; f < b->files->count; f++) {
        const k8s_ids_t *objs = cbm_ht_get(b->by_file, b->files->items[f]);
        for (int o = 0; objs && o < objs->count; o++) {
            const cbm_gbuf_node_t *t = cbm_gbuf_find_by_id(gb, objs->items[o]);
            atomic_fetch_add_explicit(&g_k8s_candidates, SKIP_ONE, memory_order_relaxed);
            if (t && k8s_patch_meets(t, w)) {
                k8s_patch_edge(b->ctx, src, t->id, p);
            }
        }
    }
}

/* The File node of `path` (a kustomization, a patch file: the node kustomize
 * IMPORTS leave from), 0 when none. */
static int64_t k8s_file_of(cbm_pipeline_ctx_t *ctx, const char *path) {
    const cbm_gbuf_node_t *n = cbm_pipeline_file_node(ctx->gbuf, ctx->project_name, path);
    return n ? n->id : 0;
}

/* Patch `p` in build `b`: a path patch without a target -- each of its
 * documents to the object of the same group, kind and name (and namespace,
 * when it declares one); otherwise from its documents -- a file of none (a
 * JSON6902 operation list): from its File; inline: from the kustomization's
 * File `kust_file` -- to what it names. */
static void k8s_patch_in_build(const k8s_build_t *b, const k8s_patch_t *p, int64_t kust_file) {
    const k8s_ids_t *docs = p->path ? cbm_ht_get(b->by_file, p->path) : NULL;
    if (!p->kind && !p->name && !p->selector) {
        for (int d = 0; docs && d < docs->count; d++) {
            const cbm_gbuf_node_t *doc = cbm_gbuf_find_by_id(b->ctx->gbuf, docs->items[d]);
            char kind[K8S_LABEL_LEN];
            char group[K8S_LABEL_LEN];
            char ns[K8S_LABEL_LEN];
            const char *name = NULL;
            if (!doc || !k8s_kind_name(doc, kind, sizeof(kind), &name)) {
                continue;
            }
            k8s_node_prop(doc, "group", group, sizeof(group));
            k8s_declared_ns(doc->qualified_name, ns, sizeof(ns));
            k8s_want_t w = {kind, group[0] ? group : NULL, NULL, name, ns[0] ? ns : NULL, NULL};
            k8s_patch_targets(b, p, doc->id, &w);
        }
        return;
    }
    k8s_want_t w = {p->kind, p->group, p->version, p->name, p->ns, p->selector};
    if (docs && docs->count > 0) {
        for (int d = 0; d < docs->count; d++) {
            k8s_patch_targets(b, p, docs->items[d], &w);
        }
    } else {
        k8s_patch_targets(b, p, p->path ? k8s_file_of(b->ctx, p->path) : kust_file, &w);
    }
}

/* The builds the patches of kustomization `k` act in, into `out`: its own;
 * a component's are those of each kustomization including it (`inc`: the
 * includers of every kustomization, by index). */
static void k8s_patch_builds(const k8s_kusts_t *ks, const k8s_ints_t *inc, int k, k8s_ints_t *out) {
    out->count = 0;
    if (!ks->items[k].component) {
        if (k8s_grow((void **)&out->items, &out->cap, out->count, sizeof(int))) {
            out->items[out->count++] = k;
        }
        return;
    }
    for (int i = 0; i < inc[k].count; i++) {
        if (k8s_grow((void **)&out->items, &out->cap, out->count, sizeof(int))) {
            out->items[out->count++] = inc[k].items[i];
        }
    }
}

/* Kustomize patches -> OVERRIDE {via: patch, kustomization, patch, line},
 * child -> base: each patch against the build of the kustomization stating
 * it, a component's against the build of each kustomization including it
 * (the component's own resources are part of that build). */
static void k8s_link_patches(cbm_pipeline_ctx_t *ctx, const k8s_kusts_t *ks, k8s_facts_t *f) {
    if (f->patch_n == 0 || ks->count == 0) {
        return;
    }
    CBMHashTable *by_path = cbm_ht_create(0);
    CBMHashTable *by_file = cbm_ht_create(0);
    int *seen = cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)ks->count * sizeof(int));
    k8s_ints_t *inc = cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)ks->count * sizeof(*inc));
    const cbm_gbuf_node_t **res = NULL;
    int res_n = 0;
    if (by_path && by_file && seen && inc &&
        cbm_gbuf_find_by_label(ctx->gbuf, "Resource", &res, &res_n) == 0) {
        for (int i = 0; i < res_n; i++) {
            if (res[i]->file_path) {
                k8s_index_add(by_file, res[i]->file_path, res[i]->id);
            }
        }
        for (int i = 0; i < ks->count; i++) {
            cbm_ht_set(by_path, ks->items[i].path, (void *)(intptr_t)(i + SKIP_ONE));
            for (int c = 0; c < ks->items[i].kid_n; c++) {
                k8s_ints_t *v = &inc[ks->items[i].kids[c]];
                if (k8s_grow((void **)&v->items, &v->cap, v->count, sizeof(int))) {
                    v->items[v->count++] = i;
                }
            }
        }
        k8s_qsort(f->patches, (size_t)f->patch_n, sizeof(*f->patches), k8s_cmp_patch);
        k8s_ints_t builds = {0};
        int mark = 0;
        for (int i = 0; i < f->patch_n;) {
            int j = i;
            while (j < f->patch_n && strcmp(f->patches[j].kust, f->patches[i].kust) == 0) {
                j++;
            }
            int k = k8s_kust_at(by_path, f->patches[i].kust) - SKIP_ONE;
            int64_t kust_file = 0;
            if (k >= 0) {
                k8s_patch_builds(ks, inc, k, &builds);
                kust_file = k8s_file_of(ctx, ks->items[k].path);
            }
            for (int bi = 0; k >= 0 && bi < builds.count; bi++) {
                k8s_paths_t files = {0};
                k8s_kust_closure(ks, builds.items[bi], seen, ++mark, &files);
                k8s_paths_sort_unique(&files);
                CBMHashTable *names = cbm_ht_create(0);
                for (int x = 0; names && x < files.count; x++) {
                    const k8s_ids_t *objs = cbm_ht_get(by_file, files.items[x]);
                    for (int o = 0; objs && o < objs->count; o++) {
                        const cbm_gbuf_node_t *n = cbm_gbuf_find_by_id(ctx->gbuf, objs->items[o]);
                        if (n && n->name) {
                            k8s_index_add(names, n->name, n->id);
                        }
                    }
                }
                k8s_build_t b = {ctx, &files, names, by_file};
                for (int q = i; names && q < j; q++) {
                    k8s_patch_in_build(&b, &f->patches[q], kust_file);
                }
                if (names) {
                    cbm_ht_foreach(names, k8s_free_ids_entry, NULL);
                    cbm_ht_free(names);
                }
                cbm_free(CBM_MEM_CLASS_OTHER, (void *)files.items);
            }
            i = j;
        }
        cbm_free(CBM_MEM_CLASS_OTHER, builds.items);
    }
    for (int i = 0; inc && i < ks->count; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, inc[i].items);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, inc);
    cbm_free(CBM_MEM_CLASS_OTHER, seen);
    if (by_file) {
        cbm_ht_foreach(by_file, k8s_free_ids_entry, NULL);
        cbm_ht_free(by_file);
    }
    if (by_path) {
        cbm_ht_free(by_path);
    }
}

/* kustomize's prefix / suffix transformers leave these names alone
 * (api/internal/builtins/PrefixTransformer.go prefixFieldSpecsToSkip, the
 * same list in SuffixTransformer.go). */
static bool k8s_rename_skips(const char *group, const char *kind) {
    return strcmp(kind, "CustomResourceDefinition") == 0 || strcmp(kind, "Namespace") == 0 ||
           (strcmp(kind, "APIService") == 0 && strcmp(group, "apiregistration.k8s.io") == 0);
}

/* A kustomization on the way down a renaming build: the prefix and suffix
 * the levels above and its own put around its objects' names, and the
 * namespace the outermost level setting one gives them (NULL: their own). */
typedef struct {
    int kust;
    const char *ns;
    char pre[K8S_LABEL_LEN];
    char suf[K8S_LABEL_LEN];
} k8s_rename_frame_t;

/* The variant of object `obj` the renaming build of `root` deploys: a
 * Resource `Kind/<deployed name>` declared by the kustomization (`base`:
 * its File QN without `.__file__`), ecosystem kustomize; the kustomization
 * DEFINES it and it OVERRIDEs {via: namePrefix | nameSuffix, kustomization,
 * line} the object. */
static void k8s_rename_variant(cbm_pipeline_ctx_t *ctx, const k8s_kust_t *root, const char *base,
                               int64_t kust_file, const k8s_rename_frame_t *f,
                               const cbm_gbuf_node_t *obj) {
    char kind[K8S_LABEL_LEN];
    char group[K8S_LABEL_LEN];
    char version[K8S_LABEL_LEN];
    char declared[K8S_LABEL_LEN];
    const char *name = NULL;
    if (!k8s_kind_name(obj, kind, sizeof(kind), &name)) {
        return;
    }
    k8s_node_prop(obj, "group", group, sizeof(group));
    if (!group[0] || k8s_rename_skips(group, kind)) {
        return;
    }
    k8s_node_prop(obj, "version", version, sizeof(version));
    k8s_declared_ns(obj->qualified_name, declared, sizeof(declared));
    const char *ns = k8s_cluster_scoped(kind) || !f->ns ? declared : f->ns;
    char deployed[K8S_LABEL_LEN];
    int dl = snprintf(deployed, sizeof(deployed), "%s%s%s", f->pre, name, f->suf);
    char qn[CBM_SZ_2K];
    char rname[CBM_SZ_256];
    if (dl <= 0 || (size_t)dl >= sizeof(deployed) ||
        snprintf(qn, sizeof(qn), "%s::%s/%s/%s/%s", base, group, kind, ns[0] ? ns : "-",
                 deployed) >= (int)sizeof(qn) ||
        snprintf(rname, sizeof(rname), "%s/%s", kind, deployed) >= (int)sizeof(rname)) {
        return;
    }
    char *props = k8s_resource_props(qn, NULL, version, NULL, "kustomize");
    int64_t id = props ? cbm_gbuf_upsert_node(ctx->gbuf, "Resource", rname, qn, root->path,
                                              root->rename_line, root->rename_line, props)
                       : 0;
    cbm_free(CBM_MEM_CLASS_OTHER, props);
    if (id <= 0 || id == obj->id) {
        return;
    }
    if (kust_file > 0) {
        cbm_gbuf_insert_edge(ctx->gbuf, kust_file, id, "DEFINES", "{}");
    }
    char ek[CBM_SZ_2K];
    cbm_json_escape(ek, (int)sizeof(ek), root->path);
    char eprops[CBM_SZ_2K + CBM_SZ_256];
    snprintf(eprops, sizeof(eprops), "{\"via\":\"%s\",\"kustomization\":\"%s\",\"line\":%d}",
             root->prefix ? "namePrefix" : "nameSuffix", ek, root->rename_line);
    cbm_gbuf_insert_edge(ctx->gbuf, id, obj->id, "OVERRIDE", eprops);
}

/* Every object the build of renaming kustomization `root` reaches gets its
 * variant: down the kustomizations it includes, each level's prefix inside
 * the ones above, its namespace where none above sets one (a component's
 * own transforms are not followed). `by_file`: Resource ids by manifest. */
static void k8s_rename_build(cbm_pipeline_ctx_t *ctx, const k8s_kusts_t *ks, int root,
                             CBMHashTable *by_file, int *seen, int mark) {
    static const char FILE_SUFFIX[] = ".__file__";
    const k8s_kust_t *r = &ks->items[root];
    const cbm_gbuf_node_t *kf = cbm_pipeline_file_node(ctx->gbuf, ctx->project_name, r->path);
    size_t ql = kf && kf->qualified_name ? strlen(kf->qualified_name) : 0;
    size_t sl = sizeof(FILE_SUFFIX) - SKIP_ONE;
    char base[CBM_SZ_1K];
    if (ql <= sl || strcmp(kf->qualified_name + ql - sl, FILE_SUFFIX) != 0 ||
        ql - sl >= sizeof(base)) {
        return;
    }
    snprintf(base, sizeof(base), "%.*s", (int)(ql - sl), kf->qualified_name);
    k8s_rename_frame_t *stack = NULL;
    int depth = 0;
    int cap = 0;
    if (!k8s_grow((void **)&stack, &cap, depth, sizeof(*stack))) {
        return;
    }
    stack[depth] = (k8s_rename_frame_t){.kust = root, .ns = r->ns};
    snprintf(stack[depth].pre, sizeof(stack[depth].pre), "%s", r->prefix ? r->prefix : "");
    snprintf(stack[depth].suf, sizeof(stack[depth].suf), "%s", r->suffix ? r->suffix : "");
    depth++;
    while (depth > 0) {
        k8s_rename_frame_t f = stack[--depth];
        if (seen[f.kust] == mark) {
            continue;
        }
        seen[f.kust] = mark;
        const k8s_kust_t *k = &ks->items[f.kust];
        for (int i = 0; i < k->file_n; i++) {
            const k8s_ids_t *objs = cbm_ht_get(by_file, k->files[i]);
            for (int o = 0; objs && o < objs->count; o++) {
                const cbm_gbuf_node_t *n = cbm_gbuf_find_by_id(ctx->gbuf, objs->items[o]);
                if (n) {
                    k8s_rename_variant(ctx, r, base, kf->id, &f, n);
                }
            }
        }
        for (int i = 0; i < k->kid_n; i++) {
            const k8s_kust_t *kid = &ks->items[k->kids[i]];
            const char *kp = kid->component || !kid->prefix ? "" : kid->prefix;
            const char *kx = kid->component || !kid->suffix ? "" : kid->suffix;
            if (!k8s_grow((void **)&stack, &cap, depth, sizeof(*stack))) {
                break;
            }
            k8s_rename_frame_t *c = &stack[depth];
            *c = (k8s_rename_frame_t){.kust = k->kids[i],
                                      .ns = f.ns || kid->component ? f.ns : kid->ns};
            if (snprintf(c->pre, sizeof(c->pre), "%s%s", f.pre, kp) < (int)sizeof(c->pre) &&
                snprintf(c->suf, sizeof(c->suf), "%s%s", kx, f.suf) < (int)sizeof(c->suf)) {
                depth++;
            }
        }
    }
    cbm_free(CBM_MEM_CLASS_OTHER, stack);
}

/* Kustomize namePrefix / nameSuffix -> a variant Resource per object a
 * kustomization renames, with its deployed name, OVERRIDE -> the object.
 * Minted after the joins: references inside a build use the declared names
 * (kustomize rewrites them), so nothing joins a variant. */
static void k8s_link_renames(cbm_pipeline_ctx_t *ctx, const k8s_kusts_t *ks) {
    bool any = false;
    for (int i = 0; i < ks->count && !any; i++) {
        any = !ks->items[i].component && (ks->items[i].prefix || ks->items[i].suffix);
    }
    if (!any) {
        return;
    }
    CBMHashTable *by_file = cbm_ht_create(0);
    int *seen = cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)ks->count * sizeof(int));
    const cbm_gbuf_node_t **res = NULL;
    int res_n = 0;
    if (by_file && seen && cbm_gbuf_find_by_label(ctx->gbuf, "Resource", &res, &res_n) == 0) {
        for (int i = 0; i < res_n; i++) {
            if (res[i]->file_path) {
                k8s_index_add(by_file, res[i]->file_path, res[i]->id);
            }
        }
        int mark = 0;
        for (int i = 0; i < ks->count; i++) {
            if (!ks->items[i].component && (ks->items[i].prefix || ks->items[i].suffix)) {
                k8s_rename_build(ctx, ks, i, by_file, seen, ++mark);
            }
        }
    }
    cbm_free(CBM_MEM_CLASS_OTHER, seen);
    if (by_file) {
        cbm_ht_foreach(by_file, k8s_free_ids_entry, NULL);
        cbm_ht_free(by_file);
    }
}

static void k8s_link_deploys(cbm_pipeline_ctx_t *ctx, k8s_kusts_t *ks, k8s_bundles_t *b,
                             const k8s_facts_t *f) {
    if (f->dep_n == 0 && f->hr_n == 0 && f->src_n == 0 && f->kchart_n == 0) {
        return;
    }
    k8s_dep_link_t l = {.ctx = ctx,
                        .files = k8s_index_by_file(ctx, "File"),
                        .charts = k8s_index_by_file(ctx, "Chart"),
                        .kusts = cbm_ht_create(0)};
    const cbm_gbuf_node_t **res = NULL;
    int rn = 0;
    k8s_paths_t manifests = {0};
    if (l.kusts && cbm_gbuf_find_by_label(ctx->gbuf, "Resource", &res, &rn) == 0) {
        for (int i = 0; i < rn; i++) {
            if (!res[i]->file_path) {
                continue;
            }
            k8s_paths_push(&manifests, res[i]->file_path);
            if (res[i]->name && strcmp(res[i]->name, "Kustomization") == 0) {
                k8s_index_add(l.kusts, res[i]->file_path, res[i]->id);
            }
        }
    }
    k8s_paths_sort_unique(&manifests);
    CBMHashTable *by_path = cbm_ht_create(0);
    for (int i = 0; by_path && i < ks->count; i++) {
        cbm_ht_set(by_path, ks->items[i].path, (void *)(intptr_t)(i + SKIP_ONE));
    }
    int *seen =
        cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)(ks->count ? ks->count : SKIP_ONE) * sizeof(int));
    k8s_paths_t members = {0};
    k8s_paths_t units = {0};
    for (int d = 0; l.files && l.charts && l.kusts && by_path && seen && d < f->dep_n; d++) {
        const k8s_deploy_t *dp = &f->deploys[d];
        char esc[CBM_SZ_1K];
        char props[CBM_SZ_2K];
        cbm_json_escape(esc, (int)sizeof(esc), dp->dir);
        snprintf(props, sizeof(props), "{\"via\":\"%s\",\"path\":\"%s\"}", dp->via, esc);
        int64_t chart = cbm_ht_get(b->charts, dp->dir) ? k8s_chart_node(&l, dp->dir) : 0;
        if (chart > 0) {
            k8s_deploys_edge(&l, dp->node, chart, props);
            continue;
        }
        members.count = 0;
        units.count = 0;
        k8s_dir_members(ks, by_path, &manifests, dp->dir, seen, d + SKIP_ONE, &members, &units);
        k8s_paths_sort_unique(&units);
        for (int u = 0; u < units.count; u++) {
            int64_t tgt = k8s_first_id(l.kusts, units.items[u]);
            k8s_deploys_edge(&l, dp->node, tgt > 0 ? tgt : k8s_first_id(l.files, units.items[u]),
                             props);
        }
    }
    for (int h = 0; l.files && l.charts && h < f->hr_n; h++) {
        const k8s_hr_t *hr = &f->hrs[h];
        const cbm_gbuf_node_t *src = cbm_gbuf_find_by_id(ctx->gbuf, hr->node);
        bool repo_path = hr->source && (strcmp(hr->source, "GitRepository") == 0 ||
                                        strcmp(hr->source, "Bucket") == 0);
        char esc[CBM_SZ_1K];
        char props[CBM_SZ_2K];
        cbm_json_escape(esc, (int)sizeof(esc), hr->chart);
        snprintf(props, sizeof(props), "{\"via\":\"helmrelease\",\"chart\":\"%s\"%s}", esc,
                 repo_path ? "" : ",\"external\":true");
        char dir[CBM_SZ_1K];
        if (repo_path) {
            if (k8s_norm_dir(hr->chart, dir, sizeof(dir)) && cbm_ht_get(b->charts, dir)) {
                k8s_deploys_edge(&l, hr->node, k8s_chart_node(&l, dir), props);
            }
        } else if (src && src->file_path && !strchr(hr->chart, '/') && !strchr(hr->chart, '$')) {
            /* the source's URL is not read (a HelmRepository's spec.url):
             * the per-project external Chart */
            k8s_deploys_edge(&l, hr->node, k8s_external_chart(&l, hr->chart, NULL, src->file_path),
                             props);
        }
    }
    for (int s = 0; l.files && l.charts && s < f->src_n; s++) {
        k8s_link_chart_refs(&l, &f->srcs[s]);
    }
    if (l.files && l.kusts) {
        k8s_link_kcharts(&l, b, f);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)members.items);
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)units.items);
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)manifests.items);
    cbm_free(CBM_MEM_CLASS_OTHER, seen);
    if (by_path) {
        cbm_ht_free(by_path);
    }
    k8s_free_index(l.files);
    k8s_free_index(l.charts);
    k8s_free_index(l.kusts);
    if (l.edges > 0) {
        cbm_log_info("pass.k8s.deploys_edges", "edges", itoa_k8s(l.edges));
    }
}

int cbm_pipeline_pass_k8s(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count) {
    cbm_log_info("pass.start", "pass", "k8s", "files", itoa_k8s(file_count));

    cbm_init();

    int manifest_count = 0;
    int helm_count = 0;

    /* Collect per-manifest selector/label records for cross-manifest matching. */
    k8s_record_array_t recs = {0};
    k8s_local_deps_t local_deps = {0};
    k8s_kusts_t kusts = {0};
    k8s_bundles_t bundles = {.charts = cbm_ht_create(0)};
    k8s_facts_t facts = {0};

    k8s_prep_t *prep = cbm_calloc(CBM_MEM_CLASS_OTHER, K8S_CHUNK * sizeof(*prep));
    if (!prep) {
        k8s_bundles_free(&bundles);
        return CBM_NOT_FOUND;
    }
    int workers = cbm_default_worker_count(false);

    for (int begin = 0; begin < file_count; begin += K8S_CHUNK) {
        int end = begin + K8S_CHUNK < file_count ? begin + K8S_CHUNK : file_count;
        if (cbm_pipeline_check_cancel(ctx)) {
            cbm_free(CBM_MEM_CLASS_OTHER, prep);
            k8s_records_free(&recs);
            k8s_local_deps_free(&local_deps);
            k8s_kusts_free(&kusts);
            k8s_bundles_free(&bundles);
            k8s_facts_free(&facts);
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
                switch (p->kind) {
                case K8S_DEP:
                    handle_dep_manifest(ctx, rel, p->source,
                                        is_requirements_file(base) ? "pypi" : "gomod");
                    break;
                case K8S_HELM:
                    handle_helm_chart(ctx, rel, p->source, &local_deps);
                    k8s_chart_dir_add(&bundles, rel);
                    helm_count++;
                    break;
                case K8S_WERF: {
                    char chart_dir[CBM_SZ_1K];
                    if (handle_werf_chart(ctx, rel, p->source, chart_dir, sizeof(chart_dir))) {
                        k8s_chart_dir_register(&bundles, chart_dir);
                        helm_count++;
                    }
                    break;
                }
                case K8S_KUSTOMIZE:
                    k8s_kusts_push(&kusts, rel, p->source);
                    k8s_kustomize_keep(&facts, rel, p->res);
                    break;
                case K8S_MANIFEST:
                    handle_k8s_manifest(ctx, rel, p->source, p->res, &recs, &facts);
                    manifest_count++;
                    break;
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
            k8s_records_free(&recs);
            k8s_local_deps_free(&local_deps);
            k8s_kusts_free(&kusts);
            k8s_bundles_free(&bundles);
            k8s_facts_free(&facts);
            return CBM_NOT_FOUND;
        }
    }
    cbm_free(CBM_MEM_CLASS_OTHER, prep);

    /* Kubernetes objects Terraform declares (pass_terraform.c), before the
     * bundles and the joins: references meet them like manifest objects. */
    cbm_pipeline_tf_k8s_objects(ctx, files, file_count);

    /* Connect Services to the workloads their selectors target (INFRA_MAPS),
     * inside the bundles that deploy them together. */
    k8s_bundles_build(ctx, &kusts, &bundles);
    k8s_deploys_build(ctx, &kusts, &bundles, &facts);
    k8s_apply_effective_namespaces(ctx, &bundles);
    k8s_link_selectors(ctx, &recs, &bundles);
    k8s_resolve_refs(ctx, &bundles, &facts);
    k8s_link_crds(ctx, &bundles, &facts);
    k8s_link_deploys(ctx, &kusts, &bundles, &facts);
    k8s_link_images(ctx, &facts);
    k8s_helm_t helm;
    k8s_helm_init(&helm);
    k8s_helm_collect(ctx, &facts, &helm);
    k8s_link_envs(ctx, &facts);
    k8s_link_patches(ctx, &kusts, &facts);
    k8s_link_renames(ctx, &kusts);
    k8s_facts_free(&facts);
    k8s_records_free(&recs);
    /* Link `file://` chart dependencies now that every Chart.yaml is read,
     * then the templates' includes, which see those dependencies' defines. */
    k8s_link_local_deps(ctx, &local_deps);
    k8s_link_includes(ctx, &bundles, files, file_count);
    k8s_link_chart_contents(ctx, &bundles);
    /* The images chart templates render with their values (helm_template.c). */
    k8s_helm_link_defaults(ctx, &bundles, &helm);
    k8s_kusts_free(&kusts);
    /* Shell and Make deploy commands, against the nodes linked above. */
    cbm_pipeline_pass_shell(ctx, files, file_count);
    cbm_pipeline_pass_terraform(ctx, files, file_count);
    /* The images releases render (their values over the charts' defaults),
     * before the image hubs move references. */
    k8s_helm_link_releases(ctx, &bundles, &helm);
    k8s_helm_free(&helm);
    k8s_bundles_free(&bundles);
    cbm_pipeline_pass_ansible(ctx, files, file_count);
    cbm_pipeline_pass_docker(ctx, files, file_count);
    /* The Images a chart's objects use, once the image hubs moved them. */
    k8s_link_chart_images(ctx);
    /* The variables a container sets, to the readers in its own image. */
    (void)cbm_pipeline_env_join(ctx);

    cbm_log_info("pass.done", "pass", "k8s", "manifests", itoa_k8s(manifest_count));
    (void)helm_count;
    return 0;
}
