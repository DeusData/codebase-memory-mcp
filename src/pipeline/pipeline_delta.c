#include "pipeline/pipeline_internal.h"

#include "foundation/compat.h"
#include "foundation/constants.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <yyjson/yyjson.h>

static const char cbm_delta_edge_imports[] = "IMPORTS";
static const char cbm_delta_prop_is_exported[] = "is_exported";
static const char cbm_delta_reason_candidate[] = "candidate";
static const char cbm_delta_reason_delete_requires_full[] = "delete_requires_full";
static const char cbm_delta_reason_frontier_error[] = "frontier_error";
static const char cbm_delta_reason_frontier_too_large[] = "frontier_too_large";
static const char cbm_delta_reason_invalid_input[] = "invalid_input";
static const char cbm_delta_reason_missing_file_metadata[] = "missing_file_metadata";
static const char cbm_delta_reason_preflight_error[] = "preflight_error";
static const char cbm_delta_reason_rename_requires_full[] = "rename_requires_full";
static const char cbm_delta_reason_unresolved_edge_endpoint[] = "unresolved_edge_endpoint";
static const char cbm_delta_reason_unsupported_derived_view[] = "unsupported_derived_view";
static const char cbm_delta_reason_unsupported_edges[] = "unsupported_edges";

enum { CBM_DELTA_GROWTH = 2 };

typedef struct {
    cbm_pipeline_file_delta_t *out;
    const cbm_gbuf_t *gbuf;
    const char *project;
    const char *rel_path;
    int node_cap;
    int edge_cap;
    int export_cap;
    int import_cap;
    int rc;
} cbm_delta_build_ctx_t;

static char *delta_strdup(const char *s) {
    return cbm_strdup(s ? s : "");
}

static bool delta_same_path(const char *a, const char *b) {
    return a && b && strcmp(a, b) == 0;
}

static bool delta_node_is_exported(const cbm_gbuf_node_t *node) {
    if (!node || !node->properties_json) {
        return false;
    }
    yyjson_doc *doc = yyjson_read(node->properties_json, strlen(node->properties_json), 0);
    if (!doc) {
        return false;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *value = root ? yyjson_obj_get(root, cbm_delta_prop_is_exported) : NULL;
    bool exported = value && yyjson_is_bool(value) && yyjson_get_bool(value);
    yyjson_doc_free(doc);
    return exported;
}

static int delta_grow(void **items, int *cap, size_t item_sz) {
    if (!items || !cap || item_sz == 0) {
        return CBM_STORE_ERR;
    }
    int new_cap = (*cap > 0) ? *cap * CBM_DELTA_GROWTH : CBM_SZ_8;
    void *tmp = realloc(*items, (size_t)new_cap * item_sz);
    if (!tmp) {
        return CBM_STORE_ERR;
    }
    *items = tmp;
    *cap = new_cap;
    return CBM_STORE_OK;
}

static int delta_append_node(cbm_delta_build_ctx_t *ctx, const cbm_gbuf_node_t *node) {
    if (ctx->out->delta.node_count >= ctx->node_cap &&
        delta_grow((void **)&ctx->out->nodes, &ctx->node_cap, sizeof(*ctx->out->nodes)) !=
            CBM_STORE_OK) {
        return CBM_STORE_ERR;
    }
    cbm_node_t *dst = &ctx->out->nodes[ctx->out->delta.node_count++];
    *dst = (cbm_node_t){.id = CBM_STORE_NO_NODE_ID,
                        .project = delta_strdup(ctx->project),
                        .label = delta_strdup(node->label),
                        .name = delta_strdup(node->name),
                        .qualified_name = delta_strdup(node->qualified_name),
                        .file_path = delta_strdup(node->file_path),
                        .start_line = node->start_line,
                        .end_line = node->end_line,
                        .properties_json = delta_strdup(node->properties_json ? node->properties_json : "{}")};
    if (!dst->project || !dst->label || !dst->name || !dst->qualified_name || !dst->file_path ||
        !dst->properties_json) {
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

static int delta_append_export(cbm_delta_build_ctx_t *ctx, const cbm_gbuf_node_t *node) {
    if (ctx->out->delta.export_count >= ctx->export_cap &&
        delta_grow((void **)&ctx->out->exports, &ctx->export_cap, sizeof(*ctx->out->exports)) !=
            CBM_STORE_OK) {
        return CBM_STORE_ERR;
    }
    cbm_store_symbol_export_t *dst = &ctx->out->exports[ctx->out->delta.export_count++];
    *dst = (cbm_store_symbol_export_t){.qualified_name = delta_strdup(node->qualified_name),
                                       .node_id = CBM_STORE_NO_NODE_ID};
    return dst->qualified_name ? CBM_STORE_OK : CBM_STORE_ERR;
}

static int delta_append_edge(cbm_delta_build_ctx_t *ctx, const cbm_gbuf_node_t *src,
                             const cbm_gbuf_node_t *tgt, const cbm_gbuf_edge_t *edge) {
    if (ctx->out->delta.edge_count >= ctx->edge_cap &&
        delta_grow((void **)&ctx->out->edges, &ctx->edge_cap, sizeof(*ctx->out->edges)) !=
            CBM_STORE_OK) {
        return CBM_STORE_ERR;
    }
    cbm_store_delta_edge_t *dst = &ctx->out->edges[ctx->out->delta.edge_count++];
    *dst = (cbm_store_delta_edge_t){
        .source_qn = delta_strdup(src->qualified_name),
        .target_qn = delta_strdup(tgt->qualified_name),
        .type = delta_strdup(edge->type),
        .properties_json = delta_strdup(edge->properties_json ? edge->properties_json : "{}"),
        .derived_kind = CBM_STORE_DERIVED_KIND_DIRECT,
    };
    if (!dst->source_qn || !dst->target_qn || !dst->type || !dst->properties_json) {
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

static int delta_append_import(cbm_delta_build_ctx_t *ctx, const cbm_gbuf_node_t *tgt,
                               const cbm_gbuf_edge_t *edge) {
    if (ctx->out->delta.import_count >= ctx->import_cap &&
        delta_grow((void **)&ctx->out->imports, &ctx->import_cap, sizeof(*ctx->out->imports)) !=
            CBM_STORE_OK) {
        return CBM_STORE_ERR;
    }
    char *local = cbm_pipeline_import_edge_local_name_dup(edge);
    cbm_store_import_ref_t *dst = &ctx->out->imports[ctx->out->delta.import_count++];
    /* The graph edge preserves local_name and target_qn, not the original import
     * specifier. target_qn is the stable key needed for reverse closure. */
    *dst = (cbm_store_import_ref_t){
        .import_text = delta_strdup(tgt->qualified_name),
        .local_name = local ? local : delta_strdup(""),
        .target_qn = delta_strdup(tgt->qualified_name),
    };
    if (!dst->import_text || !dst->local_name || !dst->target_qn) {
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

static void delta_visit_node(const cbm_gbuf_node_t *node, void *userdata) {
    cbm_delta_build_ctx_t *ctx = (cbm_delta_build_ctx_t *)userdata;
    if (ctx->rc != CBM_STORE_OK || !delta_same_path(node->file_path, ctx->rel_path)) {
        return;
    }
    ctx->rc = delta_append_node(ctx, node);
    if (ctx->rc == CBM_STORE_OK && delta_node_is_exported(node)) {
        ctx->rc = delta_append_export(ctx, node);
    }
}

static void delta_visit_edge(const cbm_gbuf_edge_t *edge, void *userdata) {
    cbm_delta_build_ctx_t *ctx = (cbm_delta_build_ctx_t *)userdata;
    if (ctx->rc != CBM_STORE_OK || !edge) {
        return;
    }
    const cbm_gbuf_node_t *src = cbm_gbuf_find_by_id(ctx->gbuf, edge->source_id);
    const cbm_gbuf_node_t *tgt = cbm_gbuf_find_by_id(ctx->gbuf, edge->target_id);
    if (!src || !tgt || !src->qualified_name || !tgt->qualified_name || !edge->type) {
        ctx->out->unsupported_edge_count++;
        return;
    }
    if (!delta_same_path(src->file_path, ctx->rel_path)) {
        return;
    }
    ctx->rc = delta_append_edge(ctx, src, tgt, edge);
    if (ctx->rc == CBM_STORE_OK && strcmp(edge->type, cbm_delta_edge_imports) == 0) {
        ctx->rc = delta_append_import(ctx, tgt, edge);
    }
}

int cbm_pipeline_build_file_delta_from_gbuf(const cbm_gbuf_t *gbuf, const char *project,
                                            const char *rel_path, int64_t generation,
                                            cbm_pipeline_file_delta_t *out) {
    if (!gbuf || !project || !rel_path || generation < 0 || !out) {
        return CBM_STORE_ERR;
    }
    memset(out, 0, sizeof(*out));
    out->delta = (cbm_store_file_delta_t){
        .project = project,
        .rel_path = rel_path,
        .generation = generation,
        .derived_view_name = CBM_STORE_DERIVED_VIEW_NODES_FTS,
        .derived_status = CBM_STORE_DERIVED_STATUS_STALE,
    };
    cbm_delta_build_ctx_t ctx = {
        .out = out,
        .gbuf = gbuf,
        .project = project,
        .rel_path = rel_path,
        .rc = CBM_STORE_OK,
    };
    cbm_gbuf_foreach_node(gbuf, delta_visit_node, &ctx);
    if (ctx.rc == CBM_STORE_OK) {
        cbm_gbuf_foreach_edge(gbuf, delta_visit_edge, &ctx);
    }
    out->delta.nodes = out->nodes;
    out->delta.edges = out->edges;
    out->delta.exports = out->exports;
    out->delta.imports = out->imports;
    if (ctx.rc != CBM_STORE_OK) {
        cbm_pipeline_file_delta_free(out);
    }
    return ctx.rc;
}

void cbm_pipeline_file_delta_free(cbm_pipeline_file_delta_t *delta) {
    if (!delta) {
        return;
    }
    for (int i = 0; i < delta->delta.node_count; i++) {
        free((void *)delta->nodes[i].project);
        free((void *)delta->nodes[i].label);
        free((void *)delta->nodes[i].name);
        free((void *)delta->nodes[i].qualified_name);
        free((void *)delta->nodes[i].file_path);
        free((void *)delta->nodes[i].properties_json);
    }
    for (int i = 0; i < delta->delta.edge_count; i++) {
        free((void *)delta->edges[i].source_qn);
        free((void *)delta->edges[i].target_qn);
        free((void *)delta->edges[i].type);
        free((void *)delta->edges[i].properties_json);
    }
    for (int i = 0; i < delta->delta.export_count; i++) {
        free((void *)delta->exports[i].qualified_name);
    }
    for (int i = 0; i < delta->delta.import_count; i++) {
        free((void *)delta->imports[i].import_text);
        free((void *)delta->imports[i].local_name);
        free((void *)delta->imports[i].target_qn);
    }
    free(delta->nodes);
    free(delta->edges);
    free(delta->exports);
    free(delta->imports);
    memset(delta, 0, sizeof(*delta));
}

static void delta_plan_set_fallback(cbm_pipeline_file_delta_plan_t *plan, const char *reason) {
    plan->route = CBM_PIPELINE_DELTA_ROUTE_FALLBACK;
    plan->reason = reason;
}

static bool delta_field_matches(const char *actual, const char *expected) {
    return actual && expected && strcmp(actual, expected) == 0;
}

static bool delta_file_metadata_complete(const cbm_store_file_delta_t *delta) {
    return delta && delta->file_hash && delta->file_state &&
           delta_field_matches(delta->file_hash->project, delta->project) &&
           delta_field_matches(delta->file_hash->rel_path, delta->rel_path) &&
           delta->file_hash->sha256 &&
           delta_field_matches(delta->file_state->project, delta->project) &&
           delta_field_matches(delta->file_state->rel_path, delta->rel_path) &&
           delta->file_state->content_hash && delta->file_state->indexed_at;
}

static bool delta_derived_view_supported(const cbm_store_file_delta_t *delta) {
    return delta && (!delta->derived_view_name ||
                     strcmp(delta->derived_view_name, CBM_STORE_DERIVED_VIEW_NODES_FTS) == 0);
}

static bool delta_node_qn_present(const cbm_store_file_delta_t *delta, const char *qn) {
    if (!delta || !qn) {
        return false;
    }
    for (int i = 0; i < delta->node_count; i++) {
        if (delta->nodes[i].qualified_name && strcmp(delta->nodes[i].qualified_name, qn) == 0) {
            return true;
        }
    }
    return false;
}

static bool delta_qn_list_contains(const char **qns, int count, const char *qn) {
    for (int i = 0; i < count; i++) {
        if (strcmp(qns[i], qn) == 0) {
            return true;
        }
    }
    return false;
}

static int delta_edge_endpoints_resolve(cbm_store_t *store, const cbm_store_file_delta_t *delta) {
    if (!delta || delta->edge_count <= 0) {
        return CBM_STORE_OK;
    }
    if (delta->edge_count > INT_MAX / PAIR_LEN) {
        return CBM_STORE_ERR;
    }
    int qn_cap = delta->edge_count * PAIR_LEN;
    const char **qns = malloc((size_t)qn_cap * sizeof(*qns));
    if (!qns) {
        return CBM_STORE_ERR;
    }
    int qn_count = 0;
    for (int i = 0; i < delta->edge_count; i++) {
        const char *edge_qns[PAIR_LEN] = {delta->edges[i].source_qn, delta->edges[i].target_qn};
        for (int j = 0; j < PAIR_LEN; j++) {
            const char *qn = edge_qns[j];
            if (!qn) {
                free(qns);
                return CBM_STORE_NOT_FOUND;
            }
            if (!delta_node_qn_present(delta, qn) && !delta_qn_list_contains(qns, qn_count, qn)) {
                qns[qn_count++] = qn;
            }
        }
    }
    if (qn_count == 0) {
        free(qns);
        return CBM_STORE_OK;
    }
    int64_t *ids = calloc((size_t)qn_count, sizeof(*ids));
    if (!ids) {
        free(qns);
        return CBM_STORE_ERR;
    }
    int found = cbm_store_find_node_ids_by_qns(store, delta->project, qns, qn_count, ids);
    free(ids);
    free(qns);
    if (found < 0) {
        return CBM_STORE_ERR;
    }
    return found == qn_count ? CBM_STORE_OK : CBM_STORE_NOT_FOUND;
}

int cbm_pipeline_plan_file_delta(cbm_store_t *store, const cbm_pipeline_file_delta_t *delta,
                                 int max_affected_paths, cbm_pipeline_file_delta_plan_t *out) {
    if (!out) {
        return CBM_STORE_ERR;
    }
    memset(out, 0, sizeof(*out));
    delta_plan_set_fallback(out, cbm_delta_reason_invalid_input);
    if (!store || !delta || !delta->delta.project || !delta->delta.rel_path ||
        max_affected_paths <= 0) {
        return CBM_STORE_OK;
    }
    if (delta->change_kind == CBM_PIPELINE_DELTA_CHANGE_DELETE) {
        delta_plan_set_fallback(out, cbm_delta_reason_delete_requires_full);
        return CBM_STORE_OK;
    }
    if (delta->change_kind == CBM_PIPELINE_DELTA_CHANGE_RENAME) {
        delta_plan_set_fallback(out, cbm_delta_reason_rename_requires_full);
        return CBM_STORE_OK;
    }
    if (delta->unsupported_edge_count > 0) {
        delta_plan_set_fallback(out, cbm_delta_reason_unsupported_edges);
        return CBM_STORE_OK;
    }
    if (!delta_file_metadata_complete(&delta->delta)) {
        delta_plan_set_fallback(out, cbm_delta_reason_missing_file_metadata);
        return CBM_STORE_OK;
    }
    if (!delta_derived_view_supported(&delta->delta)) {
        delta_plan_set_fallback(out, cbm_delta_reason_unsupported_derived_view);
        return CBM_STORE_OK;
    }
    int endpoint_rc = delta_edge_endpoints_resolve(store, &delta->delta);
    if (endpoint_rc == CBM_STORE_NOT_FOUND) {
        delta_plan_set_fallback(out, cbm_delta_reason_unresolved_edge_endpoint);
        return CBM_STORE_OK;
    }
    if (endpoint_rc != CBM_STORE_OK) {
        delta_plan_set_fallback(out, cbm_delta_reason_preflight_error);
        return CBM_STORE_OK;
    }

    const char **new_export_qns = NULL;
    if (delta->delta.export_count > 0) {
        new_export_qns = malloc((size_t)delta->delta.export_count * sizeof(*new_export_qns));
        if (!new_export_qns) {
            delta_plan_set_fallback(out, cbm_delta_reason_frontier_error);
            return CBM_STORE_OK;
        }
        for (int i = 0; i < delta->delta.export_count; i++) {
            new_export_qns[i] = delta->delta.exports[i].qualified_name;
        }
    }

    int rc = cbm_store_list_file_delta_affected_paths(
        store, delta->delta.project, delta->delta.rel_path, new_export_qns,
        delta->delta.export_count, &out->affected_paths, &out->affected_count);
    free(new_export_qns);
    if (rc != CBM_STORE_OK) {
        delta_plan_set_fallback(out, cbm_delta_reason_frontier_error);
        return CBM_STORE_OK;
    }
    if (out->affected_count > max_affected_paths) {
        delta_plan_set_fallback(out, cbm_delta_reason_frontier_too_large);
        return CBM_STORE_OK;
    }

    out->route = CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE;
    out->reason = cbm_delta_reason_candidate;
    return CBM_STORE_OK;
}

void cbm_pipeline_file_delta_plan_free(cbm_pipeline_file_delta_plan_t *plan) {
    if (!plan) {
        return;
    }
    for (int i = 0; i < plan->affected_count; i++) {
        free(plan->affected_paths[i]);
    }
    free(plan->affected_paths);
    memset(plan, 0, sizeof(*plan));
}
