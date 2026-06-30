#include "pipeline/pipeline_internal.h"

#include "foundation/compat.h"
#include "foundation/constants.h"

#include <stdlib.h>
#include <string.h>
#include <yyjson/yyjson.h>

static const char cbm_delta_edge_imports[] = "IMPORTS";
static const char cbm_delta_prop_is_exported[] = "is_exported";

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
