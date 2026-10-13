// extract_k8s.c — K8s manifest and Kustomize file extractor.
//
// For CBM_LANG_KUSTOMIZE: walks top-level block_mapping_pair nodes whose key
// matches "resources", "bases", "patches", "components", or
// "patchesStrategicMerge", then emits one CBMImport per block_sequence item.
//
// For CBM_LANG_K8S: reads apiVersion, kind, metadata.name and
// metadata.namespace of EVERY document and emits one CBMDefinition per object:
// label "Resource", name "Kind/metadata-name", QN
// "<file-base>::<group>/<Kind>/<declared namespace or ->/<name>" (identity
// spec: the file base keeps the extension, so it is unique per file).

#include "cbm.h"
#include "arena.h"
#include "helpers.h"
#include "tree_sitter/api.h"
#include "foundation/constants.h"
#include <stdint.h>
#include <stdio.h>
#include <string.h>

/* Local constants. */
enum {
    K8S_BUF_SIZE = 256,
};

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// The children of `node` (named only when `named`), in order, through a
// tree cursor: ts_node_child(n, i) / ts_node_named_child(n, i) walk from the
// first child on every call, so an index loop over a wide node (a list of
// 100k items, a file of thousands of documents) is quadratic. The cursor is
// released when the children are exhausted, or by k8s_kids_done.
typedef struct {
    TSTreeCursor cur;
    bool live;
} k8s_kids_t;

static k8s_kids_t k8s_kids(TSNode node) {
    k8s_kids_t k = {.live = false};
    if (ts_node_is_null(node)) {
        return k;
    }
    k.cur = ts_tree_cursor_new(node);
    k.live = ts_tree_cursor_goto_first_child(&k.cur);
    if (!k.live) {
        ts_tree_cursor_delete(&k.cur);
    }
    return k;
}

static bool k8s_kid(k8s_kids_t *k, bool named, TSNode *out) {
    while (k->live) {
        TSNode n = ts_tree_cursor_current_node(&k->cur);
        if (!ts_tree_cursor_goto_next_sibling(&k->cur)) {
            ts_tree_cursor_delete(&k->cur);
            k->live = false;
        }
        if (!named || ts_node_is_named(n)) {
            *out = n;
            return true;
        }
    }
    return false;
}

static void k8s_kids_done(k8s_kids_t *k) {
    if (k->live) {
        ts_tree_cursor_delete(&k->cur);
        k->live = false;
    }
}

// The value inside a flow_node: its first named child that is not an anchor
// (`&name`), a tag (`!!str`) or a comment, all of which precede the value.
static TSNode flow_node_value(TSNode node) {
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode child = ts_node_named_child(node, i);
        const char *type = ts_node_type(child);
        if (strcmp(type, "anchor") != 0 && strcmp(type, "tag") != 0 &&
            strcmp(type, "comment") != 0) {
            return child;
        }
    }
    TSNode null_node = {0};
    return null_node;
}

// Return the raw source text for a scalar node (plain, single-quoted, or
// double-quoted). Surrounding quote characters are stripped for quoted forms.
// Handles flow_node wrappers transparently by descending into the value they
// wrap (the tree-sitter YAML grammar often wraps scalars in flow_node).
// Returns NULL for non-scalar node types.
static const char *get_scalar_text(CBMArena *a, TSNode node, const char *source) {
    enum { MAX_UNWRAP = 4 };
    for (int depth = 0; depth < MAX_UNWRAP; depth++) {
        const char *type = ts_node_type(node);
        if (strcmp(type, "flow_node") == 0) {
            TSNode inner = flow_node_value(node);
            if (ts_node_is_null(inner)) {
                return NULL;
            }
            node = inner;
            continue;
        }
        if (strcmp(type, "plain_scalar") == 0) {
            return cbm_node_text(a, node, source);
        }
        if (strcmp(type, "double_quote_scalar") == 0 || strcmp(type, "single_quote_scalar") == 0) {
            const char *raw = cbm_node_text(a, node, source);
            if (!raw) {
                return NULL;
            }
            size_t len = strlen(raw);
            if (len >= PAIR_LEN) {
                return cbm_arena_strndup(a, raw + SKIP_ONE, len - PAIR_LEN);
            }
            return raw;
        }
        break;
    }
    return NULL;
}

// Return true if the key text of a block_mapping_pair matches one of the
// Kustomize resource-list field names.
static int is_kustomize_list_key(const char *key) {
    return (strcmp(key, "resources") == 0 || strcmp(key, "bases") == 0 ||
            strcmp(key, "patches") == 0 || strcmp(key, "components") == 0 ||
            strcmp(key, "patchesStrategicMerge") == 0 || strcmp(key, "crds") == 0);
}

// ---------------------------------------------------------------------------
// Kustomize extraction
// ---------------------------------------------------------------------------

// Walk a block_sequence node and emit one CBMImport per block_sequence_item
// scalar child, using key_name as the local_name.
static void emit_kustomize_sequence(CBMExtractCtx *ctx, TSNode seq_node, const char *key_name) {
    CBMArena *a = ctx->arena;
    k8s_kids_t items = k8s_kids(seq_node);
    TSNode item;
    while (k8s_kid(&items, false, &item)) {
        if (strcmp(ts_node_type(item), "block_sequence_item") != 0) {
            continue;
        }
        // block_sequence_item has one named child: the value
        k8s_kids_t vals = k8s_kids(item);
        TSNode val;
        while (k8s_kid(&vals, false, &val)) {
            const char *scalar = get_scalar_text(a, val, ctx->source);
            if (!scalar) {
                continue;
            }
            CBMImport imp = {
                .local_name = cbm_arena_strdup(a, key_name),
                .module_path = cbm_arena_strdup(a, scalar),
            };
            cbm_imports_push(&ctx->result->imports, a, imp);
        }
    }
}

// First named child that is not a comment. A comment between `---` and the
// mapping (the `# yaml-language-server: $schema=...` line editors add) is the
// document's first named child; the content follows it.
static TSNode first_content_child(TSNode node) {
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (strcmp(ts_node_type(child), "comment") != 0) {
            return child;
        }
    }
    TSNode null_node = {0};
    return null_node;
}

// Unwrap a YAML node through optional block_node wrapper to get block_mapping.
// Returns null node if not a block_mapping.
static TSNode unwrap_block_mapping(TSNode doc_child) {
    TSNode mapping = first_content_child(doc_child);
    if (ts_node_is_null(mapping)) {
        return mapping;
    }
    if (strcmp(ts_node_type(mapping), "block_node") == 0) {
        mapping = first_content_child(mapping);
    }
    if (ts_node_is_null(mapping) || strcmp(ts_node_type(mapping), "block_mapping") != 0) {
        TSNode null_node = {0};
        return null_node;
    }
    return mapping;
}

// Unwrap a block_mapping_pair value through optional block_node. Comments are
// skipped on both levels: one trailing the key line (`metadata: # note`) is a
// named child of the pair ahead of its value, and a commented-out first entry
// (`resources:` / `  # - a.yaml` / `  - b.yaml`) is the block_node's first
// named child, ahead of the sequence.
static TSNode unwrap_pair_value(TSNode pair) {
    TSNode val_node = {0};
    uint32_t n = ts_node_named_child_count(pair);
    for (uint32_t i = SKIP_ONE; i < n; i++) {
        TSNode child = ts_node_named_child(pair, i);
        if (strcmp(ts_node_type(child), "comment") != 0) {
            val_node = child;
            break;
        }
    }
    if (!ts_node_is_null(val_node) && strcmp(ts_node_type(val_node), "block_node") == 0) {
        val_node = first_content_child(val_node);
    }
    return val_node;
}

// Process a single block_mapping_pair for kustomize list keys.
static void process_kustomize_pair(CBMExtractCtx *ctx, TSNode pair) {
    if (strcmp(ts_node_type(pair), "block_mapping_pair") != 0) {
        return;
    }
    TSNode key_node = ts_node_named_child(pair, 0);
    if (ts_node_is_null(key_node)) {
        return;
    }
    const char *key_text = get_scalar_text(ctx->arena, key_node, ctx->source);
    if (!key_text || !is_kustomize_list_key(key_text)) {
        return;
    }

    TSNode val_node = unwrap_pair_value(pair);
    if (ts_node_is_null(val_node) || strcmp(ts_node_type(val_node), "block_sequence") != 0) {
        return;
    }
    emit_kustomize_sequence(ctx, val_node, key_text);
}

// Emit a "Class" def named after the document's `kind` scalar. A kustomization
// file has no metadata.name, so the def name is the bare kind ("Kustomization").
// Mirrors the K8s manifest kind-def so Kustomize resources are also discoverable.
static void emit_kustomize_kind_def(CBMExtractCtx *ctx, TSNode mapping) {
    CBMArena *a = ctx->arena;
    k8s_kids_t pairs = k8s_kids(mapping);
    TSNode pair;
    while (k8s_kid(&pairs, false, &pair)) {
        if (strcmp(ts_node_type(pair), "block_mapping_pair") != 0) {
            continue;
        }
        TSNode key_node = ts_node_named_child(pair, 0);
        if (ts_node_is_null(key_node)) {
            continue;
        }
        const char *key = get_scalar_text(a, key_node, ctx->source);
        if (!key || strcmp(key, "kind") != 0) {
            continue;
        }
        TSNode val_node = unwrap_pair_value(pair);
        if (ts_node_is_null(val_node)) {
            continue;
        }
        const char *kind = get_scalar_text(a, val_node, ctx->source);
        if (!kind || !kind[0]) {
            continue;
        }
        CBMDefinition def = {0};
        def.name = cbm_arena_strdup(a, kind);
        def.qualified_name = cbm_arena_sprintf(a, "%s.%s", ctx->module_qn, kind);
        def.label = cbm_arena_strdup(a, "Resource");
        def.file_path = ctx->rel_path;
        def.start_line = ts_node_start_point(mapping).row + TS_LINE_OFFSET;
        def.end_line = ts_node_end_point(mapping).row + TS_LINE_OFFSET;
        cbm_defs_push(&ctx->result->defs, a, def);
        k8s_kids_done(&pairs);
        return;
    }
}

static void extract_kustomize(CBMExtractCtx *ctx) {
    k8s_kids_t docs = k8s_kids(ctx->root);
    TSNode stream_child;
    while (k8s_kid(&docs, false, &stream_child)) {
        if (strcmp(ts_node_type(stream_child), "document") != 0) {
            continue;
        }
        TSNode mapping = unwrap_block_mapping(stream_child);
        if (ts_node_is_null(mapping)) {
            continue;
        }

        emit_kustomize_kind_def(ctx, mapping);

        k8s_kids_t pairs = k8s_kids(mapping);
        TSNode pair;
        while (k8s_kid(&pairs, false, &pair)) {
            process_kustomize_pair(ctx, pair);
        }
    }
}

// ---------------------------------------------------------------------------
// K8s manifest extraction
// ---------------------------------------------------------------------------

// The identity scalars of one manifest document.
typedef struct {
    char api_version[K8S_BUF_SIZE];
    char kind[K8S_BUF_SIZE];
    char name[K8S_BUF_SIZE];
    char ns[K8S_BUF_SIZE];
} k8s_doc_id_t;

// Read metadata.name and metadata.namespace from a metadata block_mapping.
static void extract_metadata(CBMArena *a, TSNode meta_mapping, const char *source,
                             k8s_doc_id_t *id) {
    if (ts_node_is_null(meta_mapping) || strcmp(ts_node_type(meta_mapping), "block_mapping") != 0) {
        return;
    }
    k8s_kids_t pairs = k8s_kids(meta_mapping);
    TSNode mpair;
    while (k8s_kid(&pairs, false, &mpair)) {
        if (strcmp(ts_node_type(mpair), "block_mapping_pair") != 0) {
            continue;
        }
        TSNode mkey = ts_node_named_child(mpair, 0);
        if (ts_node_is_null(mkey)) {
            continue;
        }
        const char *mkey_text = get_scalar_text(a, mkey, source);
        bool is_name = mkey_text && strcmp(mkey_text, "name") == 0;
        bool is_ns = mkey_text && strcmp(mkey_text, "namespace") == 0;
        if (!is_name && !is_ns) {
            continue;
        }
        TSNode mval = unwrap_pair_value(mpair);
        const char *text = ts_node_is_null(mval) ? NULL : get_scalar_text(a, mval, source);
        if (text) {
            snprintf(is_name ? id->name : id->ns, K8S_BUF_SIZE, "%s", text);
        }
    }
}

// Read apiVersion, kind and metadata from a document's top-level mapping.
static void extract_k8s_scalars(CBMExtractCtx *ctx, TSNode mapping, k8s_doc_id_t *id) {
    CBMArena *a = ctx->arena;
    k8s_kids_t pairs = k8s_kids(mapping);
    TSNode pair;
    while (k8s_kid(&pairs, false, &pair)) {
        if (strcmp(ts_node_type(pair), "block_mapping_pair") != 0) {
            continue;
        }
        TSNode key_node = ts_node_named_child(pair, 0);
        if (ts_node_is_null(key_node)) {
            continue;
        }
        const char *key = get_scalar_text(a, key_node, ctx->source);
        if (!key) {
            continue;
        }
        TSNode val_node = unwrap_pair_value(pair);
        if (ts_node_is_null(val_node)) {
            continue;
        }
        if (strcmp(key, "metadata") == 0) {
            extract_metadata(a, val_node, ctx->source, id);
            continue;
        }
        char *dst = strcmp(key, "kind") == 0         ? id->kind
                    : strcmp(key, "apiVersion") == 0 ? id->api_version
                                                     : NULL;
        const char *v = dst ? get_scalar_text(a, val_node, ctx->source) : NULL;
        if (v) {
            snprintf(dst, K8S_BUF_SIZE, "%s", v);
        }
    }
}

// ---------------------------------------------------------------------------
// Object references (PHASE1.md): facts the K8s pass resolves per bundle
// ---------------------------------------------------------------------------

// A reference held by a scalar at a path, relative to the pod spec, to a
// container, or (kinds) to the whole object.
typedef struct {
    const char *path;
    const char *kind;
    const char *via;
} k8s_leaf_ref_t;

typedef struct {
    const char *kinds; // "|A|B|" of the source kinds
    const char *path;
    const char *kind;
    const char *via;
    const char *edge;
} k8s_object_ref_t;

// A reference held by a mapping's kind/name/namespace fields.
typedef struct {
    const char *kinds;   // "|A|B|" of the source kinds
    const char *path;    // the mapping's path
    const char *kind;    // the target kind; NULL = the mapping's own `kind` field
    const char *allowed; // with kind NULL: "|A|" of the kinds that are objects
    const char *dflt;    // with kind NULL: the kind when the field is absent
    const char *via;
    const char *edge;
} k8s_mapping_ref_t;

static const k8s_leaf_ref_t k8s_pod_refs[] = {
    {"serviceAccountName", "ServiceAccount", "serviceAccountName"},
    {"serviceAccount", "ServiceAccount", "serviceAccountName"},
    {"priorityClassName", "PriorityClass", "priorityClassName"},
    {"imagePullSecrets[].name", "Secret", "imagePullSecrets"},
    {"volumes[].configMap.name", "ConfigMap", "volume"},
    {"volumes[].secret.secretName", "Secret", "volume"},
    {"volumes[].persistentVolumeClaim.claimName", "PersistentVolumeClaim", "volume"},
    {"volumes[].projected.sources[].configMap.name", "ConfigMap", "volume"},
    {"volumes[].projected.sources[].secret.name", "Secret", "volume"},
};

static const k8s_leaf_ref_t k8s_container_refs[] = {
    {"env[].valueFrom.configMapKeyRef.name", "ConfigMap", "env"},
    {"env[].valueFrom.secretKeyRef.name", "Secret", "env"},
    {"envFrom[].configMapRef.name", "ConfigMap", "envFrom"},
    {"envFrom[].secretRef.name", "Secret", "envFrom"},
};

static const k8s_object_ref_t k8s_object_refs[] = {
    {"|Ingress|", "spec.rules[].http.paths[].backend.service.name", "Service", "ingress",
     "INFRA_MAPS"},
    {"|Ingress|", "spec.defaultBackend.service.name", "Service", "ingress", "INFRA_MAPS"},
    {"|Ingress|", "spec.tls[].secretName", "Secret", "tls", "USAGE"},
    {"|Ingress|", "spec.ingressClassName", "IngressClass", "ingressClassName", "USAGE"},
    {"|StatefulSet|", "spec.serviceName", "Service", "serviceName", "USAGE"},
    {"|PersistentVolumeClaim|", "spec.storageClassName", "StorageClass", "storageClassName",
     "USAGE"},
};

static const k8s_mapping_ref_t k8s_mapping_refs[] = {
    {"|RoleBinding|ClusterRoleBinding|", "roleRef", NULL, "|Role|ClusterRole|", NULL, "roleRef",
     "USAGE"},
    {"|RoleBinding|ClusterRoleBinding|", "subjects[]", NULL, "|ServiceAccount|", NULL, "subject",
     "USAGE"},
    {"|HorizontalPodAutoscaler|", "spec.scaleTargetRef", NULL, NULL, NULL, "scaleTarget", "USAGE"},
    {"|ValidatingWebhookConfiguration|MutatingWebhookConfiguration|",
     "webhooks[].clientConfig.service", "Service", NULL, NULL, "webhook", "INFRA_MAPS"},
    {"|APIService|", "spec.service", "Service", NULL, NULL, "apiservice", "INFRA_MAPS"},
    {"|HTTPRoute|GRPCRoute|TCPRoute|TLSRoute|UDPRoute|", "spec.rules[].backendRefs[]", NULL, NULL,
     "Service", "route", "INFRA_MAPS"},
    {"|HTTPRoute|GRPCRoute|TCPRoute|TLSRoute|UDPRoute|", "spec.parentRefs[]", NULL, NULL, "Gateway",
     "parentRef", "USAGE"},
};

// The curated second table: the CRDs of Flux, Argo, Cluster API,
// cert-manager and Kubermatic, written from their API types. Source kinds
// are qualified by API group ("|g|" list), and a path may use `*`, which
// matches within one segment (`spec.cloud.*.credentialsReference`).
typedef struct {
    const char *groups;
    const char *kinds;
    const char *path;
    const char *kind;
    const char *via;
    const char *edge;
} k8s_crd_leaf_ref_t;

typedef struct {
    const char *groups;
    const char *kinds;
    const char *path;
    const char *kind;    // the target kind; NULL = the mapping's own `kind` field
    const char *allowed; // with kind NULL: "|A|" of the kinds that are objects (NULL: any)
    const char *dflt;    // with kind NULL: the kind when the field is absent
    const char *via;
    const char *edge;
} k8s_crd_mapping_ref_t;

#define K8S_FLUX_KS "|kustomize.toolkit.fluxcd.io|"
#define K8S_FLUX_HELM "|helm.toolkit.fluxcd.io|"
#define K8S_FLUX_SRC "|source.toolkit.fluxcd.io|"
#define K8S_FLUX_NOTE "|notification.toolkit.fluxcd.io|"
#define K8S_FLUX_IMAGE "|image.toolkit.fluxcd.io|"
#define K8S_ARGO "|argoproj.io|"
#define K8S_CAPI "|cluster.x-k8s.io|"
#define K8S_CAPI_BOOT "|bootstrap.cluster.x-k8s.io|"
#define K8S_CAPI_CP "|controlplane.cluster.x-k8s.io|"
#define K8S_CAPI_ADDONS "|addons.cluster.x-k8s.io|"
#define K8S_CM "|cert-manager.io|"
#define K8S_KKP "|kubermatic.k8c.io|"
#define K8S_KKP_APPS "|apps.kubermatic.k8c.io|"
#define K8S_FLUX_SOURCES "|GitRepository|OCIRepository|HelmRepository|Bucket|"
#define K8S_ISSUERS "|Issuer|ClusterIssuer|"

static const k8s_crd_leaf_ref_t k8s_crd_leaf_refs[] = {
    {K8S_FLUX_KS, "|Kustomization|", "spec.decryption.secretRef.name", "Secret", "secretRef",
     "USAGE"},
    {K8S_FLUX_KS "helm.toolkit.fluxcd.io|", "|Kustomization|HelmRelease|",
     "spec.kubeConfig.secretRef.name", "Secret", "secretRef", "USAGE"},
    {K8S_FLUX_KS "helm.toolkit.fluxcd.io|", "|Kustomization|HelmRelease|",
     "spec.serviceAccountName", "ServiceAccount", "serviceAccountName", "USAGE"},
    {K8S_FLUX_SRC, K8S_FLUX_SOURCES, "spec.secretRef.name", "Secret", "secretRef", "USAGE"},
    {K8S_FLUX_SRC, K8S_FLUX_SOURCES, "spec.certSecretRef.name", "Secret", "secretRef", "USAGE"},
    {K8S_FLUX_SRC, K8S_FLUX_SOURCES, "spec.proxySecretRef.name", "Secret", "secretRef", "USAGE"},
    {K8S_FLUX_SRC, "|GitRepository|OCIRepository|", "spec.verify.secretRef.name", "Secret",
     "secretRef", "USAGE"},
    {K8S_FLUX_SRC, "|OCIRepository|HelmRepository|", "spec.serviceAccountName", "ServiceAccount",
     "serviceAccountName", "USAGE"},
    {K8S_FLUX_NOTE, "|Alert|", "spec.providerRef.name", "Provider", "providerRef", "USAGE"},
    {K8S_FLUX_NOTE, "|Provider|Receiver|", "spec.secretRef.name", "Secret", "secretRef", "USAGE"},
    {K8S_FLUX_NOTE, "|Provider|", "spec.certSecretRef.name", "Secret", "secretRef", "USAGE"},
    {K8S_FLUX_IMAGE, "|ImagePolicy|", "spec.imageRepositoryRef.name", "ImageRepository",
     "imageRepositoryRef", "USAGE"},
    {K8S_FLUX_IMAGE, "|ImageRepository|", "spec.secretRef.name", "Secret", "secretRef", "USAGE"},
    {K8S_FLUX_IMAGE, "|ImageRepository|", "spec.certSecretRef.name", "Secret", "secretRef",
     "USAGE"},
    {K8S_FLUX_IMAGE, "|ImageRepository|", "spec.serviceAccountName", "ServiceAccount",
     "serviceAccountName", "USAGE"},
    {K8S_ARGO, "|Application|", "spec.project", "AppProject", "project", "USAGE"},
    {K8S_ARGO, "|ApplicationSet|", "spec.template.spec.project", "AppProject", "project", "USAGE"},
    {K8S_ARGO, "|Rollout|", "spec.strategy.canary.canaryService", "Service", "rolloutService",
     "USAGE"},
    {K8S_ARGO, "|Rollout|", "spec.strategy.canary.stableService", "Service", "rolloutService",
     "USAGE"},
    {K8S_ARGO, "|Rollout|", "spec.strategy.blueGreen.activeService", "Service", "rolloutService",
     "USAGE"},
    {K8S_ARGO, "|Rollout|", "spec.strategy.blueGreen.previewService", "Service", "rolloutService",
     "USAGE"},
    {K8S_ARGO, "|Rollout|", "spec.strategy.canary.analysis.templates[].templateName",
     "AnalysisTemplate", "analysis", "USAGE"},
    {K8S_ARGO, "|Rollout|", "spec.strategy.canary.steps[].analysis.templates[].templateName",
     "AnalysisTemplate", "analysis", "USAGE"},
    {K8S_ARGO, "|Workflow|", "spec.workflowTemplateRef.name", "WorkflowTemplate",
     "workflowTemplateRef", "USAGE"},
    {K8S_ARGO, "|CronWorkflow|", "spec.workflowSpec.workflowTemplateRef.name", "WorkflowTemplate",
     "workflowTemplateRef", "USAGE"},
    {K8S_ARGO, "|Workflow|WorkflowTemplate|", "spec.templates[].steps[][].templateRef.name",
     "WorkflowTemplate", "templateRef", "USAGE"},
    {K8S_ARGO, "|Workflow|WorkflowTemplate|", "spec.templates[].dag.tasks[].templateRef.name",
     "WorkflowTemplate", "templateRef", "USAGE"},
    {K8S_CAPI, "|Cluster|", "spec.topology.class", "ClusterClass", "topologyClass", "USAGE"},
    {K8S_CAPI, "|Machine|MachineDeployment|MachineSet|MachinePool|MachineHealthCheck|",
     "spec.clusterName", "Cluster", "clusterName", "USAGE"},
    {K8S_CAPI, "|Machine|", "spec.bootstrap.dataSecretName", "Secret", "dataSecretName", "USAGE"},
    {K8S_CAPI, "|MachineDeployment|MachineSet|MachinePool|",
     "spec.template.spec.bootstrap.dataSecretName", "Secret", "dataSecretName", "USAGE"},
    {K8S_CAPI_BOOT, "|KubeadmConfig|", "spec.files[].contentFrom.secret.name", "Secret", "files",
     "USAGE"},
    {K8S_CAPI_BOOT, "|KubeadmConfigTemplate|", "spec.template.spec.files[].contentFrom.secret.name",
     "Secret", "files", "USAGE"},
    {K8S_CAPI_CP, "|KubeadmControlPlane|", "spec.kubeadmConfigSpec.files[].contentFrom.secret.name",
     "Secret", "files", "USAGE"},
    {K8S_CAPI_CP, "|KubeadmControlPlaneTemplate|",
     "spec.template.spec.kubeadmConfigSpec.files[].contentFrom.secret.name", "Secret", "files",
     "USAGE"},
    {K8S_CM, "|Certificate|", "spec.secretName", "Secret", "secretName", "USAGE"},
    {K8S_CM, K8S_ISSUERS, "spec.ca.secretName", "Secret", "secretRef", "USAGE"},
    {K8S_CM, K8S_ISSUERS, "spec.acme.privateKeySecretRef.name", "Secret", "secretRef", "USAGE"},
    {K8S_CM, K8S_ISSUERS, "spec.acme.externalAccountBinding.keySecretRef.name", "Secret",
     "secretRef", "USAGE"},
    {K8S_CM, K8S_ISSUERS, "spec.acme.solvers[].dns01.*.*SecretRef.name", "Secret", "secretRef",
     "USAGE"},
    {K8S_CM, K8S_ISSUERS, "spec.vault.auth.tokenSecretRef.name", "Secret", "secretRef", "USAGE"},
    {K8S_CM, K8S_ISSUERS, "spec.vault.auth.*.secretRef.name", "Secret", "secretRef", "USAGE"},
    {K8S_CM, K8S_ISSUERS, "spec.vault.caBundleSecretRef.name", "Secret", "secretRef", "USAGE"},
    {K8S_CM, K8S_ISSUERS, "spec.venafi.*.*Ref.name", "Secret", "secretRef", "USAGE"},
    {"|networking.k8s.io|extensions|gateway.networking.k8s.io|", "|Ingress|Gateway|",
     "metadata.annotations.cert-manager.io/cluster-issuer", "ClusterIssuer", "issuer", "USAGE"},
    {"|networking.k8s.io|extensions|gateway.networking.k8s.io|", "|Ingress|Gateway|",
     "metadata.annotations.cert-manager.io/issuer", "Issuer", "issuer", "USAGE"},
    {K8S_KKP_APPS, "|ApplicationInstallation|", "spec.applicationRef.name", "ApplicationDefinition",
     "applicationRef", "USAGE"},
    {K8S_KKP_APPS, "|ApplicationDefinition|",
     "spec.versions[].template.source.*.credentials.*.name", "Secret", "credentials", "USAGE"},
};

static const k8s_crd_mapping_ref_t k8s_crd_mapping_refs[] = {
    {K8S_FLUX_KS, "|Kustomization|", "spec.sourceRef", NULL, NULL, NULL, "sourceRef", "USAGE"},
    {K8S_FLUX_KS, "|Kustomization|", "spec.dependsOn[]", "Kustomization", NULL, NULL, "dependsOn",
     "DEPENDS_ON"},
    {K8S_FLUX_KS, "|Kustomization|", "spec.postBuild.substituteFrom[]", NULL, "|ConfigMap|Secret|",
     NULL, "substituteFrom", "USAGE"},
    {K8S_FLUX_KS, "|Kustomization|", "spec.healthChecks[]", NULL, NULL, NULL, "healthCheck",
     "USAGE"},
    {K8S_FLUX_HELM, "|HelmRelease|", "spec.chart.spec.sourceRef", NULL, NULL, NULL, "sourceRef",
     "USAGE"},
    {K8S_FLUX_HELM, "|HelmRelease|", "spec.chartRef", NULL, NULL, NULL, "chartRef", "USAGE"},
    {K8S_FLUX_HELM, "|HelmRelease|", "spec.dependsOn[]", "HelmRelease", NULL, NULL, "dependsOn",
     "DEPENDS_ON"},
    {K8S_FLUX_HELM, "|HelmRelease|", "spec.valuesFrom[]", NULL, "|ConfigMap|Secret|", NULL,
     "valuesFrom", "USAGE"},
    {K8S_FLUX_SRC, "|HelmChart|", "spec.sourceRef", NULL, NULL, NULL, "sourceRef", "USAGE"},
    {K8S_FLUX_SRC, "|GitRepository|", "spec.include[].repository", "GitRepository", NULL, NULL,
     "include", "USAGE"},
    {K8S_FLUX_NOTE, "|Alert|", "spec.eventSources[]", NULL, NULL, NULL, "eventSource", "USAGE"},
    {K8S_FLUX_NOTE, "|Receiver|", "spec.resources[]", NULL, NULL, NULL, "resource", "USAGE"},
    {K8S_FLUX_IMAGE, "|ImageUpdateAutomation|", "spec.sourceRef", NULL, NULL, "GitRepository",
     "sourceRef", "USAGE"},
    {K8S_ARGO, "|Rollout|", "spec.workloadRef", NULL, NULL, NULL, "workloadRef", "USAGE"},
    {K8S_CAPI, "|Cluster|", "spec.infrastructureRef", NULL, NULL, NULL, "infrastructureRef",
     "USAGE"},
    {K8S_CAPI, "|Cluster|", "spec.controlPlaneRef", NULL, NULL, NULL, "controlPlaneRef", "USAGE"},
    {K8S_CAPI, "|Cluster|", "spec.topology.classRef", "ClusterClass", NULL, NULL, "topologyClass",
     "USAGE"},
    {K8S_CAPI, "|Machine|", "spec.bootstrap.configRef", NULL, NULL, NULL, "bootstrap", "USAGE"},
    {K8S_CAPI, "|Machine|", "spec.infrastructureRef", NULL, NULL, NULL, "infrastructureRef",
     "USAGE"},
    {K8S_CAPI, "|MachineDeployment|MachineSet|MachinePool|",
     "spec.template.spec.bootstrap.configRef", NULL, NULL, NULL, "bootstrap", "USAGE"},
    {K8S_CAPI, "|MachineDeployment|MachineSet|MachinePool|", "spec.template.spec.infrastructureRef",
     NULL, NULL, NULL, "infrastructureRef", "USAGE"},
    {K8S_CAPI_CP, "|KubeadmControlPlane|", "spec.machineTemplate.infrastructureRef", NULL, NULL,
     NULL, "infrastructureRef", "USAGE"},
    {K8S_CAPI_CP, "|KubeadmControlPlane|", "spec.machineTemplate.spec.infrastructureRef", NULL,
     NULL, NULL, "infrastructureRef", "USAGE"},
    {K8S_CAPI, "|ClusterClass|", "spec.infrastructure.ref", NULL, NULL, NULL, "template", "USAGE"},
    {K8S_CAPI, "|ClusterClass|", "spec.infrastructure.templateRef", NULL, NULL, NULL, "template",
     "USAGE"},
    {K8S_CAPI, "|ClusterClass|", "spec.controlPlane.ref", NULL, NULL, NULL, "template", "USAGE"},
    {K8S_CAPI, "|ClusterClass|", "spec.controlPlane.templateRef", NULL, NULL, NULL, "template",
     "USAGE"},
    {K8S_CAPI, "|ClusterClass|", "spec.controlPlane.machineInfrastructure.ref", NULL, NULL, NULL,
     "template", "USAGE"},
    {K8S_CAPI, "|ClusterClass|", "spec.controlPlane.machineInfrastructure.templateRef", NULL, NULL,
     NULL, "template", "USAGE"},
    {K8S_CAPI, "|ClusterClass|", "spec.workers.*[].template.*.ref", NULL, NULL, NULL, "template",
     "USAGE"},
    {K8S_CAPI, "|ClusterClass|", "spec.workers.*[].*.templateRef", NULL, NULL, NULL, "template",
     "USAGE"},
    {K8S_CAPI_ADDONS, "|ClusterResourceSet|", "spec.resources[]", NULL, "|ConfigMap|Secret|", NULL,
     "resources", "USAGE"},
    {K8S_CM, "|Certificate|CertificateRequest|", "spec.issuerRef", NULL, NULL, "Issuer",
     "issuerRef", "USAGE"},
    {K8S_KKP, "|KubermaticConfiguration|", "spec.ingress.certificateIssuer", NULL, NULL, NULL,
     "certificateIssuer", "USAGE"},
    {K8S_KKP, "|Seed|", "spec.kubeconfig", "Secret", NULL, NULL, "kubeconfig", "USAGE"},
    {K8S_KKP, "|Seed|", "spec.etcdBackupRestore.destinations.*.credentials", "Secret", NULL, NULL,
     "credentials", "USAGE"},
    {K8S_KKP, "|Cluster|", "spec.cloud.*.credentialsReference", "Secret", NULL, NULL, "credentials",
     "USAGE"},
    {K8S_KKP, "|Addon|EtcdBackupConfig|EtcdRestore|", "spec.cluster", "Cluster", NULL, NULL,
     "cluster", "USAGE"},
};

// Does `path` match `pat`, where `*` matches any run within one segment?
static bool k8s_path_match(const char *pat, const char *path) {
    const char *star = NULL;
    const char *mark = NULL;
    while (*path) {
        if (*pat == '*') {
            star = pat++;
            mark = path;
        } else if (*pat && *pat == *path) {
            pat++;
            path++;
        } else if (star && *mark != '.') {
            pat = star + SKIP_ONE;
            path = ++mark;
        } else {
            return false;
        }
    }
    while (*pat == '*') {
        pat++;
    }
    return *pat == '\0';
}

// Is `kind` one of the "|A|B|" list?
static bool k8s_kind_in(const char *list, const char *kind) {
    if (!list || !kind[0]) {
        return false;
    }
    size_t n = strlen(kind);
    for (const char *p = strstr(list, kind); p; p = strstr(p + SKIP_ONE, kind)) {
        if (p[-SKIP_ONE] == '|' && p[n] == '|') {
            return true;
        }
    }
    return false;
}

// Where a kind keeps its pod spec, "" for none.
static const char *k8s_pod_prefix(const char *kind) {
    if (strcmp(kind, "Pod") == 0) {
        return "spec";
    }
    if (strcmp(kind, "CronJob") == 0) {
        return "spec.jobTemplate.spec.template.spec";
    }
    if (k8s_kind_in(
            "|Deployment|StatefulSet|DaemonSet|ReplicaSet|ReplicationController|Job|Rollout|",
            kind)) {
        return "spec.template.spec";
    }
    return "";
}

typedef struct {
    CBMExtractCtx *ctx;
    const char *group; // the document's API group
    const char *kind;  // the document's kind
    int def_index;
    char crd_group[K8S_BUF_SIZE];
    char crd_kind[K8S_BUF_SIZE];
    const char *deploy_ns; // a deploying object's target namespace
    const char *hr_chart;  // a HelmRelease's spec.chart.spec.chart
    const char *hr_source; // ... and its sourceRef kind
} k8s_walk_t;

// Is the document of a source kind in group list `groups`?
static bool k8s_source_is(const k8s_walk_t *w, const char *groups, const char *kinds) {
    return k8s_kind_in(groups, w->group) && k8s_kind_in(kinds, w->kind);
}

// A Flux Kustomization / Argo Application deploys a repository directory.
static void k8s_deploy_leaf(k8s_walk_t *w, const char *path, const char *value) {
    bool flux = k8s_source_is(w, K8S_FLUX_KS, "|Kustomization|");
    bool argo = k8s_source_is(w, K8S_ARGO, "|Application|");
    if ((flux && strcmp(path, "spec.targetNamespace") == 0) ||
        (argo && strcmp(path, "spec.destination.namespace") == 0)) {
        w->deploy_ns = strstr(value, "{{") ? NULL : value;
        return;
    }
    if (k8s_source_is(w, K8S_FLUX_HELM, "|HelmRelease|")) {
        if (strcmp(path, "spec.chart.spec.chart") == 0 && !strstr(value, "{{")) {
            w->hr_chart = value;
        } else if (strcmp(path, "spec.chart.spec.sourceRef.kind") == 0) {
            w->hr_source = value;
        }
        return;
    }
    /* the chart an OCIRepository fetches (a HelmRelease's chartRef) */
    if (k8s_source_is(w, K8S_FLUX_SRC, "|OCIRepository|") && strcmp(path, "spec.url") == 0) {
        if (!strstr(value, "{{") && !strstr(value, "${")) {
            CBMK8sFact f = {.fact = CBM_K8S_FACT_SOURCE,
                            .def_index = w->def_index,
                            .name = cbm_arena_strdup(w->ctx->arena, value),
                            .via = "ocirepository"};
            cbm_k8sfact_push(&w->ctx->result->k8s_facts, w->ctx->arena, f);
        }
        return;
    }
    if ((flux && strcmp(path, "spec.path") == 0) ||
        (argo &&
         (strcmp(path, "spec.source.path") == 0 || strcmp(path, "spec.sources[].path") == 0))) {
        if (strstr(value, "{{")) {
            return;
        }
        CBMK8sFact f = {.fact = CBM_K8S_FACT_DEPLOY,
                        .def_index = w->def_index,
                        .name = cbm_arena_strdup(w->ctx->arena, value),
                        .via = flux ? "flux" : "argo"};
        cbm_k8sfact_push(&w->ctx->result->k8s_facts, w->ctx->arena, f);
    }
}

static void k8s_emit_ref(k8s_walk_t *w, const char *kind, const char *ns, const char *name,
                         const char *via, const char *edge) {
    if (!kind || !kind[0] || !name || !name[0] || strstr(name, "{{") || strstr(kind, "{{")) {
        return;
    }
    CBMArena *a = w->ctx->arena;
    CBMK8sFact f = {.fact = CBM_K8S_FACT_REF,
                    .def_index = w->def_index,
                    .kind = cbm_arena_strdup(a, kind),
                    .ns = ns && ns[0] && ns[0] != '<' && !strstr(ns, "{{") ? cbm_arena_strdup(a, ns)
                                                                           : NULL,
                    .name = cbm_arena_strdup(a, name),
                    .via = via,
                    .edge = edge};
    cbm_k8sfact_push(&w->ctx->result->k8s_facts, a, f);
}

// A container's literal image: `path` ends in containers[].image,
// initContainers[].image or ephemeralContainers[].image at any depth (pod
// templates, CronJobs, the pod templates of custom resources).
static bool k8s_image_leaf(const char *path, const char *value) {
    static const char *const LEAVES[] = {"containers[].image", "initContainers[].image",
                                         "ephemeralContainers[].image"};
    if (!value[0] || strchr(value, '$') || strchr(value, ' ') || strstr(value, "{{")) {
        return false;
    }
    size_t pl = strlen(path);
    for (size_t i = 0; i < sizeof(LEAVES) / sizeof(LEAVES[0]); i++) {
        size_t ll = strlen(LEAVES[i]);
        if (pl >= ll && strcmp(path + pl - ll, LEAVES[i]) == 0 &&
            (pl == ll || path[pl - ll - SKIP_ONE] == '.')) {
            return true;
        }
    }
    return false;
}

// A scalar at `path`: a leaf reference, or a CRD's group / kind.
static void k8s_leaf(k8s_walk_t *w, const char *path, const char *value) {
    if (k8s_image_leaf(path, value)) {
        CBMK8sFact f = {.fact = CBM_K8S_FACT_IMAGE,
                        .def_index = w->def_index,
                        .name = cbm_arena_strdup(w->ctx->arena, value),
                        .via = "container"};
        cbm_k8sfact_push(&w->ctx->result->k8s_facts, w->ctx->arena, f);
    }
    const char *pod = k8s_pod_prefix(w->kind);
    size_t pl = strlen(pod);
    if (pl && strncmp(path, pod, pl) == 0 && path[pl] == '.') {
        const char *rel = path + pl + SKIP_ONE;
        for (size_t i = 0; i < sizeof(k8s_pod_refs) / sizeof(k8s_pod_refs[0]); i++) {
            if (strcmp(rel, k8s_pod_refs[i].path) == 0) {
                k8s_emit_ref(w, k8s_pod_refs[i].kind, NULL, value, k8s_pod_refs[i].via, "USAGE");
            }
        }
        static const char *const holders[] = {"containers[].", "initContainers[].",
                                              "ephemeralContainers[]."};
        for (size_t h = 0; h < sizeof(holders) / sizeof(holders[0]); h++) {
            size_t hl = strlen(holders[h]);
            if (strncmp(rel, holders[h], hl) != 0) {
                continue;
            }
            for (size_t i = 0; i < sizeof(k8s_container_refs) / sizeof(k8s_container_refs[0]);
                 i++) {
                if (strcmp(rel + hl, k8s_container_refs[i].path) == 0) {
                    k8s_emit_ref(w, k8s_container_refs[i].kind, NULL, value,
                                 k8s_container_refs[i].via, "USAGE");
                }
            }
        }
    }
    for (size_t i = 0; i < sizeof(k8s_object_refs) / sizeof(k8s_object_refs[0]); i++) {
        const k8s_object_ref_t *r = &k8s_object_refs[i];
        if (k8s_kind_in(r->kinds, w->kind) && strcmp(path, r->path) == 0) {
            k8s_emit_ref(w, r->kind, NULL, value, r->via, r->edge);
        }
    }
    for (size_t i = 0; i < sizeof(k8s_crd_leaf_refs) / sizeof(k8s_crd_leaf_refs[0]); i++) {
        const k8s_crd_leaf_ref_t *r = &k8s_crd_leaf_refs[i];
        if (k8s_source_is(w, r->groups, r->kinds) && k8s_path_match(r->path, path)) {
            k8s_emit_ref(w, r->kind, NULL, value, r->via, r->edge);
        }
    }
    k8s_deploy_leaf(w, path, value);
    if (strcmp(w->kind, "CustomResourceDefinition") == 0) {
        if (strcmp(path, "spec.group") == 0) {
            snprintf(w->crd_group, sizeof(w->crd_group), "%s", value);
        } else if (strcmp(path, "spec.names.kind") == 0) {
            snprintf(w->crd_kind, sizeof(w->crd_kind), "%s", value);
        }
    }
}

// Through block_node / flow_node wrappers to the content node.
static TSNode k8s_unwrap(TSNode node) {
    for (int i = 0; i < PAIR_LEN && !ts_node_is_null(node); i++) {
        const char *t = ts_node_type(node);
        if (strcmp(t, "block_node") == 0) {
            node = first_content_child(node);
        } else if (strcmp(t, "flow_node") == 0) {
            node = flow_node_value(node);
        } else {
            break;
        }
    }
    return node;
}

static bool k8s_is_mapping(TSNode n) {
    const char *t = ts_node_type(n);
    return strcmp(t, "block_mapping") == 0 || strcmp(t, "flow_mapping") == 0;
}

static bool k8s_is_pair(TSNode n) {
    const char *t = ts_node_type(n);
    return strcmp(t, "block_mapping_pair") == 0 || strcmp(t, "flow_pair") == 0;
}

// The scalar value of `field` in a mapping, or NULL.
static const char *k8s_field(k8s_walk_t *w, TSNode mapping, const char *field) {
    k8s_kids_t pairs = k8s_kids(mapping);
    TSNode pair;
    while (k8s_kid(&pairs, true, &pair)) {
        if (!k8s_is_pair(pair)) {
            continue;
        }
        TSNode key = ts_node_named_child(pair, 0);
        const char *k =
            ts_node_is_null(key) ? NULL : get_scalar_text(w->ctx->arena, key, w->ctx->source);
        if (k && strcmp(k, field) == 0) {
            k8s_kids_done(&pairs);
            TSNode v = unwrap_pair_value(pair);
            return ts_node_is_null(v) ? NULL : get_scalar_text(w->ctx->arena, v, w->ctx->source);
        }
    }
    return NULL;
}

// The content node of `field` in a mapping (null when absent or not a mapping).
static TSNode k8s_child(k8s_walk_t *w, TSNode mapping, const char *field) {
    TSNode none = {0};
    if (ts_node_is_null(mapping) || !k8s_is_mapping(mapping)) {
        return none;
    }
    k8s_kids_t pairs = k8s_kids(mapping);
    TSNode pair;
    while (k8s_kid(&pairs, true, &pair)) {
        if (!k8s_is_pair(pair)) {
            continue;
        }
        TSNode key = ts_node_named_child(pair, 0);
        const char *k =
            ts_node_is_null(key) ? NULL : get_scalar_text(w->ctx->arena, key, w->ctx->source);
        if (k && strcmp(k, field) == 0) {
            k8s_kids_done(&pairs);
            TSNode v = unwrap_pair_value(pair);
            return ts_node_is_null(v) ? none : k8s_unwrap(v);
        }
    }
    return none;
}

// The items of a sequence node, unwrapped, comments skipped (iterate to the
// end: the cursor is released when the items are exhausted).
typedef k8s_kids_t k8s_iter_t;

static k8s_iter_t k8s_iter(TSNode seq) {
    TSNode none = {0};
    bool is_seq = !ts_node_is_null(seq) && (strcmp(ts_node_type(seq), "block_sequence") == 0 ||
                                            strcmp(ts_node_type(seq), "flow_sequence") == 0);
    return k8s_kids(is_seq ? seq : none);
}

static bool k8s_next(k8s_iter_t *it, TSNode *out) {
    TSNode item;
    while (k8s_kid(it, true, &item)) {
        if (strcmp(ts_node_type(item), "block_sequence_item") == 0) {
            item = first_content_child(item);
        }
        if (!ts_node_is_null(item) && strcmp(ts_node_type(item), "comment") != 0) {
            *out = k8s_unwrap(item);
            return true;
        }
    }
    return false;
}

// A scalar field of a node (NULL unless a mapping holding a scalar there).
static const char *k8s_text(k8s_walk_t *w, TSNode mapping, const char *field) {
    TSNode v = k8s_child(w, mapping, field);
    return ts_node_is_null(v) ? NULL : get_scalar_text(w->ctx->arena, v, w->ctx->source);
}

// An exposed route "<host or *><path>" to backend `name` (NULL: none).
static void k8s_emit_route(k8s_walk_t *w, const char *host, const char *path, const char *ns,
                           const char *name, const char *via) {
    host = host && host[0] ? host : "*";
    path = path && path[0] ? path : "/";
    if (strstr(host, "{{") || strstr(path, "{{") || strchr(host, '/')) {
        return;
    }
    CBMArena *a = w->ctx->arena;
    CBMK8sFact f = {.fact = CBM_K8S_FACT_ROUTE,
                    .def_index = w->def_index,
                    .kind = name ? "Service" : NULL,
                    .ns = ns && ns[0] && ns[0] != '<' && !strstr(ns, "{{") ? cbm_arena_strdup(a, ns)
                                                                           : NULL,
                    .name = name && !strstr(name, "{{") ? cbm_arena_strdup(a, name) : NULL,
                    .via = via,
                    .edge = "INFRA_MAPS",
                    .route = cbm_arena_sprintf(a, "%s%s%s", host, path[0] == '/' ? "" : "/", path)};
    cbm_k8sfact_push(&w->ctx->result->k8s_facts, a, f);
}

// The routes an Ingress rule (spec.rules[] item) exposes: its host and each
// http path, sent to the path's backend Service.
static void k8s_ingress_rule(k8s_walk_t *w, TSNode rule) {
    const char *host = k8s_text(w, rule, "host");
    k8s_iter_t paths = k8s_iter(k8s_child(w, k8s_child(w, rule, "http"), "paths"));
    TSNode p;
    while (k8s_next(&paths, &p)) {
        TSNode backend = k8s_child(w, p, "backend");
        const char *svc = k8s_text(w, k8s_child(w, backend, "service"), "name");
        svc = svc ? svc : k8s_text(w, backend, "serviceName");
        k8s_emit_route(w, host, k8s_text(w, p, "path"), NULL, svc, "ingress");
    }
}

// One HTTPRoute host x path: to each Service backend of the rule, else none.
static void k8s_httproute_target(k8s_walk_t *w, TSNode rule, const char *host, const char *path) {
    k8s_iter_t backs = k8s_iter(k8s_child(w, rule, "backendRefs"));
    TSNode b;
    bool any = false;
    while (k8s_next(&backs, &b)) {
        const char *kind = k8s_text(w, b, "kind");
        if (kind && strcmp(kind, "Service") != 0) {
            continue;
        }
        k8s_emit_route(w, host, path, k8s_text(w, b, "namespace"), k8s_text(w, b, "name"),
                       "httproute");
        any = true;
    }
    if (!any) {
        k8s_emit_route(w, host, path, NULL, NULL, "httproute");
    }
}

// The routes an HTTPRoute spec exposes: every hostname (else any) with
// every path match of a rule (else `/`), sent to the rule's Service backends.
static void k8s_httproute_spec(k8s_walk_t *w, TSNode spec) {
    TSNode hostnames = k8s_child(w, spec, "hostnames");
    k8s_iter_t rules = k8s_iter(k8s_child(w, spec, "rules"));
    TSNode rule;
    while (k8s_next(&rules, &rule)) {
        k8s_iter_t hosts = k8s_iter(hostnames);
        TSNode h;
        bool any_host = k8s_next(&hosts, &h);
        do {
            const char *host = any_host ? get_scalar_text(w->ctx->arena, h, w->ctx->source) : NULL;
            k8s_iter_t matches = k8s_iter(k8s_child(w, rule, "matches"));
            TSNode m;
            bool any_match = false;
            while (k8s_next(&matches, &m)) {
                k8s_httproute_target(w, rule, host, k8s_text(w, k8s_child(w, m, "path"), "value"));
                any_match = true;
            }
            if (!any_match) {
                k8s_httproute_target(w, rule, host, NULL);
            }
        } while (any_host && k8s_next(&hosts, &h));
    }
}

// The reference a mapping holds in its kind/name/namespace: to `kind`, or
// (NULL) to its own `kind` field, one of `allowed` (NULL: any), `dflt`
// when absent.
static void k8s_mapping_ref(k8s_walk_t *w, TSNode mapping, const char *kind, const char *allowed,
                            const char *dflt, const char *via, const char *edge) {
    if (!kind) {
        kind = k8s_field(w, mapping, "kind");
        kind = kind ? kind : dflt;
        if (!kind || (allowed && !k8s_kind_in(allowed, kind))) {
            return;
        }
    }
    k8s_emit_ref(w, kind, k8s_field(w, mapping, "namespace"), k8s_field(w, mapping, "name"), via,
                 edge);
}

// Where an env[] item's value comes from: "value", or its valueFrom kind
// (configMapKeyRef, secretKeyRef, fieldRef, resourceFieldRef, fileKeyRef;
// "valueFrom" for any other). The value itself is never read.
static const char *k8s_env_source(k8s_walk_t *w, TSNode item) {
    static const char *const KINDS[] = {"configMapKeyRef", "secretKeyRef", "fieldRef",
                                        "resourceFieldRef", "fileKeyRef"};
    k8s_kids_t pairs = k8s_kids(item);
    TSNode pair;
    while (k8s_kid(&pairs, true, &pair)) {
        if (!k8s_is_pair(pair)) {
            continue;
        }
        TSNode key = ts_node_named_child(pair, 0);
        const char *k =
            ts_node_is_null(key) ? NULL : get_scalar_text(w->ctx->arena, key, w->ctx->source);
        if (!k || strcmp(k, "valueFrom") != 0) {
            continue;
        }
        k8s_kids_done(&pairs);
        TSNode from = k8s_unwrap(unwrap_pair_value(pair));
        k8s_kids_t kinds = k8s_kids(from);
        TSNode kp;
        const char *kind = NULL;
        while (k8s_kid(&kinds, true, &kp)) {
            TSNode kk = k8s_is_pair(kp) ? ts_node_named_child(kp, 0) : (TSNode){0};
            const char *s =
                ts_node_is_null(kk) ? NULL : get_scalar_text(w->ctx->arena, kk, w->ctx->source);
            if (s && !kind) {
                kind = s;
            }
        }
        for (size_t i = 0; kind && i < sizeof(KINDS) / sizeof(KINDS[0]); i++) {
            if (strcmp(kind, KINDS[i]) == 0) {
                return KINDS[i];
            }
        }
        return "valueFrom";
    }
    return "value";
}

// The environment variables a container (`path` ends in containers[],
// initContainers[] or ephemeralContainers[], any depth) sets: each env[]
// name, with where its value comes from -- never the value.
static void k8s_container_env(k8s_walk_t *w, const char *path, TSNode container) {
    static const char *const ITEMS[] = {"containers[]", "initContainers[]",
                                        "ephemeralContainers[]"};
    size_t pl = strlen(path);
    bool at = false;
    for (size_t i = 0; i < sizeof(ITEMS) / sizeof(ITEMS[0]) && !at; i++) {
        size_t il = strlen(ITEMS[i]);
        at = pl >= il && strcmp(path + pl - il, ITEMS[i]) == 0 &&
             (pl == il || path[pl - il - SKIP_ONE] == '.');
    }
    if (!at) {
        return;
    }
    const char *cname = k8s_text(w, container, "name");
    cname = cname && cname[0] && !strstr(cname, "{{") ? cname : NULL;
    k8s_iter_t env = k8s_iter(k8s_child(w, container, "env"));
    TSNode item;
    while (k8s_next(&env, &item)) {
        const char *key = k8s_is_mapping(item) ? k8s_text(w, item, "name") : NULL;
        if (!key || !key[0] || strstr(key, "{{")) {
            continue;
        }
        CBMK8sFact f = {.fact = CBM_K8S_FACT_ENV,
                        .def_index = w->def_index,
                        .name = key,
                        .container = cname,
                        .via = k8s_env_source(w, item),
                        .line = (int)ts_node_start_point(item).row + SKIP_ONE};
        cbm_k8sfact_push(&w->ctx->result->k8s_facts, w->ctx->arena, f);
    }
}

// A mapping at `path` that holds a reference in its kind/name/namespace.
static void k8s_mapping(k8s_walk_t *w, const char *path, TSNode mapping) {
    k8s_container_env(w, path, mapping);
    for (size_t i = 0; i < sizeof(k8s_mapping_refs) / sizeof(k8s_mapping_refs[0]); i++) {
        const k8s_mapping_ref_t *r = &k8s_mapping_refs[i];
        if (k8s_kind_in(r->kinds, w->kind) && strcmp(path, r->path) == 0) {
            k8s_mapping_ref(w, mapping, r->kind, r->allowed, r->dflt, r->via, r->edge);
        }
    }
    for (size_t i = 0; i < sizeof(k8s_crd_mapping_refs) / sizeof(k8s_crd_mapping_refs[0]); i++) {
        const k8s_crd_mapping_ref_t *r = &k8s_crd_mapping_refs[i];
        if (k8s_source_is(w, r->groups, r->kinds) && k8s_path_match(r->path, path)) {
            k8s_mapping_ref(w, mapping, r->kind, r->allowed, r->dflt, r->via, r->edge);
        }
    }
    if (k8s_source_is(w, "|networking.k8s.io|extensions|", "|Ingress|") &&
        strcmp(path, "spec.rules[]") == 0) {
        k8s_ingress_rule(w, mapping);
    }
    if (k8s_source_is(w, "|gateway.networking.k8s.io|", "|HTTPRoute|") &&
        strcmp(path, "spec") == 0) {
        k8s_httproute_spec(w, mapping);
    }
}

// Walk `node` (reached at `path`) for references.
static void k8s_walk(k8s_walk_t *w, TSNode node, char *path, size_t len, size_t cap) {
    node = k8s_unwrap(node);
    if (ts_node_is_null(node)) {
        return;
    }
    const char *type = ts_node_type(node);
    if (k8s_is_mapping(node)) {
        k8s_mapping(w, path, node);
        k8s_kids_t pairs = k8s_kids(node);
        TSNode pair;
        while (k8s_kid(&pairs, true, &pair)) {
            if (!k8s_is_pair(pair)) {
                continue;
            }
            TSNode key = ts_node_named_child(pair, 0);
            const char *k =
                ts_node_is_null(key) ? NULL : get_scalar_text(w->ctx->arena, key, w->ctx->source);
            int add = k ? snprintf(path + len, cap - len, "%s%s", len ? "." : "", k) : -1;
            if (add <= 0 || len + (size_t)add >= cap) {
                path[len] = '\0'; // a path too long for any reference path: not one
                continue;
            }
            TSNode value = unwrap_pair_value(pair);
            TSNode inner = k8s_unwrap(value);
            const char *scalar = ts_node_is_null(inner)
                                     ? NULL
                                     : get_scalar_text(w->ctx->arena, inner, w->ctx->source);
            if (scalar) {
                k8s_leaf(w, path, scalar);
            } else if (!ts_node_is_null(value)) {
                k8s_walk(w, value, path, len + (size_t)add, cap);
            }
            path[len] = '\0';
        }
        return;
    }
    if (strcmp(type, "block_sequence") == 0 || strcmp(type, "flow_sequence") == 0) {
        int add = snprintf(path + len, cap - len, "[]");
        if (add <= 0 || len + (size_t)add >= cap) {
            path[len] = '\0';
            return;
        }
        k8s_kids_t items = k8s_kids(node);
        TSNode item;
        while (k8s_kid(&items, true, &item)) {
            if (strcmp(ts_node_type(item), "block_sequence_item") == 0) {
                item = first_content_child(item);
            }
            if (!ts_node_is_null(item) && strcmp(ts_node_type(item), "comment") != 0) {
                k8s_walk(w, item, path, len + (size_t)add, cap);
            }
        }
        path[len] = '\0';
    }
}

// The references, deploys and CRD declaration of document def `def_index`.
static void k8s_emit_facts(CBMExtractCtx *ctx, TSNode mapping, const char *group, const char *kind,
                           int def_index) {
    k8s_walk_t w = {.ctx = ctx, .group = group, .kind = kind, .def_index = def_index};
    char path[CBM_SZ_512] = "";
    int first = ctx->result->k8s_facts.count;
    k8s_walk(&w, mapping, path, 0, sizeof(path));
    /* a HelmRelease deploys its chart: a path of a Git/Bucket source, else
     * a chart name of a Helm repository (kind: the source kind) */
    if (w.hr_chart) {
        CBMK8sFact f = {.fact = CBM_K8S_FACT_DEPLOY,
                        .def_index = def_index,
                        .kind = w.hr_source ? cbm_arena_strdup(ctx->arena, w.hr_source) : NULL,
                        .name = cbm_arena_strdup(ctx->arena, w.hr_chart),
                        .via = "helmrelease"};
        cbm_k8sfact_push(&ctx->result->k8s_facts, ctx->arena, f);
    }
    /* the target namespace may follow the path in the document */
    for (int i = first; w.deploy_ns && i < ctx->result->k8s_facts.count; i++) {
        CBMK8sFact *f = &ctx->result->k8s_facts.items[i];
        if (f->fact == CBM_K8S_FACT_DEPLOY) {
            f->ns = cbm_arena_strdup(ctx->arena, w.deploy_ns);
        }
    }
    if (w.crd_group[0] && w.crd_kind[0]) {
        CBMK8sFact f = {.fact = CBM_K8S_FACT_CRD,
                        .def_index = def_index,
                        .kind = cbm_arena_strdup(ctx->arena, w.crd_kind),
                        .name = cbm_arena_strdup(ctx->arena, w.crd_group)};
        cbm_k8sfact_push(&ctx->result->k8s_facts, ctx->arena, f);
    }
}

// "<project>.<rel_path segments>" with the extension kept: the File node's QN
// without ".__file__" (IDENTITY.md). Unique per file, unlike the module QN,
// which drops the extension (`a.yaml` / `a.yml`, `x/` / `x.yaml`).
static const char *k8s_file_base(CBMArena *a, const char *project, const char *rel_path) {
    project = project ? project : "";
    rel_path = rel_path ? rel_path : "";
    size_t plen = strlen(project);
    char *buf = cbm_arena_alloc(a, plen + strlen(rel_path) + PAIR_LEN);
    if (!buf) {
        return NULL;
    }
    char *o = buf;
    memcpy(o, project, plen);
    o += plen;
    bool seg_start = true;
    for (const char *p = rel_path; *p; p++) {
        char c = *p == '\\' ? '/' : *p;
        if (c == '/') {
            seg_start = true;
            continue;
        }
        if (seg_start) {
            *o++ = '.';
            seg_start = false;
        }
        *o++ = c;
    }
    *o = '\0';
    return buf;
}

// The API group of an apiVersion: the part before '/', "core" for the core
// group ("v1"), "?" when the document names none. The version is not part of
// an object's identity.
static void k8s_api_group(const char *api_version, char *out, size_t out_sz) {
    if (api_version[0] == '<') { // templated (a skeleton's symbolic `<...>`)
        snprintf(out, out_sz, "?");
        return;
    }
    const char *slash = strchr(api_version, '/');
    if (slash) {
        snprintf(out, out_sz, "%.*s", (int)(slash - api_version), api_version);
    } else {
        snprintf(out, out_sz, "%s", api_version[0] ? "core" : "?");
    }
}

// One Resource def per document: `Kind/name`, QN
// `<file-base>::<group>/<Kind>/<declared namespace or ->/<name>` (IDENTITY.md).
static void extract_k8s_manifest(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    const char *base = k8s_file_base(a, ctx->project, ctx->rel_path);
    if (!base) {
        return;
    }

    k8s_kids_t docs = k8s_kids(ctx->root);
    TSNode stream_child;
    while (k8s_kid(&docs, false, &stream_child)) {
        if (strcmp(ts_node_type(stream_child), "document") != 0) {
            continue;
        }

        TSNode mapping = unwrap_block_mapping(stream_child);
        if (ts_node_is_null(mapping)) {
            continue;
        }

        k8s_doc_id_t id;
        memset(&id, 0, sizeof(id));
        extract_k8s_scalars(ctx, mapping, &id);

        // Skip malformed manifests (no kind or no metadata.name), and a
        // templated kind (a skeleton's symbolic `<...>`): unidentifiable.
        if (id.kind[0] == '\0' || id.name[0] == '\0' || id.kind[0] == '<') {
            continue;
        }
        // A templated namespace (`<.Release.Namespace>`) is the deploy's.
        if (id.ns[0] == '<') {
            id.ns[0] = '\0';
        }

        char group[K8S_BUF_SIZE];
        k8s_api_group(id.api_version, group, sizeof(group));

        CBMDefinition def = {0};
        def.name = cbm_arena_sprintf(a, "%s/%s", id.kind, id.name);
        def.qualified_name = cbm_arena_sprintf(a, "%s::%s/%s/%s/%s", base, group, id.kind,
                                               id.ns[0] ? id.ns : "-", id.name);
        // "Resource" is the canonical def label for a K8s resource kind. It is a
        // valid graph label and is what the K8s pipeline pass (pass_k8s.c) filters
        // on to upsert Resource nodes and emit INFRA_MAPS edges.
        def.label = cbm_arena_strdup(a, "Resource");
        def.file_path = ctx->rel_path;
        def.start_line = ts_node_start_point(mapping).row + TS_LINE_OFFSET;
        def.end_line = ts_node_end_point(mapping).row + TS_LINE_OFFSET;
        cbm_defs_push(&ctx->result->defs, a, def);
        k8s_emit_facts(ctx, mapping, group, id.kind, ctx->result->defs.count - SKIP_ONE);
    }
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

void cbm_extract_k8s(CBMExtractCtx *ctx) {
    if (ctx->language == CBM_LANG_KUSTOMIZE) {
        extract_kustomize(ctx);
    } else if (ctx->language == CBM_LANG_K8S) {
        extract_k8s_manifest(ctx);
    }
}
