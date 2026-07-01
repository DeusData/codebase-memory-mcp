#include "pipeline/pipeline_internal.h"

#include "foundation/compat.h"
#include "foundation/constants.h"

#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <yyjson/yyjson.h>

#define XXH_INLINE_ALL
#include "xxhash/xxhash.h"

static const char cbm_delta_edge_imports[] = "IMPORTS";
static const char cbm_delta_file_hash_legacy_empty[] = "";
static const char cbm_delta_prop_is_exported[] = "is_exported";
static const char cbm_delta_pass_fingerprint_v1[] = "pipeline-file-delta-v1";
static const char cbm_delta_reason_candidate[] = "candidate";
static const char cbm_delta_reason_delete_requires_full[] = "delete_requires_full";
static const char cbm_delta_reason_frontier_error[] = "frontier_error";
static const char cbm_delta_reason_frontier_too_large[] = "frontier_too_large";
static const char cbm_delta_reason_inbound_edges_require_full[] = "inbound_edges_require_full";
static const char cbm_delta_reason_invalid_input[] = "invalid_input";
static const char cbm_delta_reason_missing_existing_ownership[] = "missing_existing_ownership";
static const char cbm_delta_reason_missing_file_metadata[] = "missing_file_metadata";
static const char cbm_delta_reason_preflight_error[] = "preflight_error";
static const char cbm_delta_reason_publish_error[] = "publish_error";
static const char cbm_delta_reason_rename_requires_full[] = "rename_requires_full";
static const char cbm_delta_reason_unresolved_edge_endpoint[] = "unresolved_edge_endpoint";
static const char cbm_delta_reason_unsupported_derived_view[] = "unsupported_derived_view";
static const char cbm_delta_reason_unsupported_edges[] = "unsupported_edges";

static const char *const cbm_delta_scratch_seed_labels[] = {
    "File",      "Module", "Struct", "Enum",  "Trait",    "Type",
    "Function",  "Method", "Class",  "Interface", "Variable", "Field",
    NULL,
};

enum {
    CBM_DELTA_GROWTH = 2,
    CBM_DELTA_XXH64_HEX_LEN = (int)(sizeof(uint64_t) * PAIR_LEN),
    CBM_DELTA_ISO8601_UTC_LEN = 20,
};

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

const char *cbm_pipeline_file_delta_pass_fingerprint(void) {
    return cbm_delta_pass_fingerprint_v1;
}

static bool delta_same_path(const char *a, const char *b) {
    return a && b && strcmp(a, b) == 0;
}

static bool delta_path_in_list(const char *path, const char *const *paths, int count) {
    if (!path || !paths || count <= 0) {
        return false;
    }
    for (int i = 0; i < count; i++) {
        if (delta_same_path(path, paths[i])) {
            return true;
        }
    }
    return false;
}

static int delta_seed_store_node(cbm_gbuf_t *gbuf, cbm_registry_t *registry,
                                 const cbm_node_t *node) {
    if (!gbuf || !node || !node->label || !node->qualified_name) {
        return CBM_STORE_ERR;
    }
    int64_t id = cbm_gbuf_upsert_node(gbuf, node->label, node->name ? node->name : "",
                                      node->qualified_name, node->file_path ? node->file_path : "",
                                      node->start_line, node->end_line,
                                      node->properties_json ? node->properties_json : "{}");
    if (id <= 0) {
        return CBM_STORE_ERR;
    }
    if (registry && cbm_pipeline_label_is_registry_symbol(node->label) && node->name) {
        cbm_registry_add(registry, node->name, node->qualified_name, node->label);
    }
    return CBM_STORE_OK;
}

int cbm_pipeline_seed_file_delta_scratch_from_store(cbm_store_t *store, cbm_gbuf_t *gbuf,
                                                    cbm_registry_t *registry,
                                                    const char *project,
                                                    const char *const *changed_paths,
                                                    int changed_path_count) {
    if (!store || !gbuf || !project || changed_path_count < 0 ||
        (changed_path_count > 0 && !changed_paths)) {
        return CBM_STORE_ERR;
    }
    for (const char *const *label = cbm_delta_scratch_seed_labels; *label; label++) {
        cbm_node_t *nodes = NULL;
        int node_count = 0;
        int rc = cbm_store_find_nodes_by_label(store, project, *label, &nodes, &node_count);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
        for (int i = 0; i < node_count; i++) {
            if (delta_path_in_list(nodes[i].file_path, changed_paths, changed_path_count)) {
                continue;
            }
            if (delta_seed_store_node(gbuf, registry, &nodes[i]) != CBM_STORE_OK) {
                cbm_store_free_nodes(nodes, node_count);
                return CBM_STORE_ERR;
            }
        }
        cbm_store_free_nodes(nodes, node_count);
    }
    return CBM_STORE_OK;
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

int64_t cbm_pipeline_stat_mtime_ns(const struct stat *st) {
#ifdef __APPLE__
    return ((int64_t)st->st_mtimespec.tv_sec * (int64_t)CBM_NSEC_PER_SEC) +
           (int64_t)st->st_mtimespec.tv_nsec;
#elif defined(_WIN32)
    return (int64_t)st->st_mtime * (int64_t)CBM_NSEC_PER_SEC;
#else
    return ((int64_t)st->st_mtim.tv_sec * (int64_t)CBM_NSEC_PER_SEC) +
           (int64_t)st->st_mtim.tv_nsec;
#endif
}

static int delta_iso_now(char *buf, size_t sz) {
    if (!buf || sz <= CBM_DELTA_ISO8601_UTC_LEN) {
        return CBM_STORE_ERR;
    }
    time_t t = time(NULL);
    struct tm tm;
    cbm_gmtime_r(&t, &tm);
    return strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &tm) == CBM_DELTA_ISO8601_UTC_LEN
               ? CBM_STORE_OK
               : CBM_STORE_ERR;
}

int cbm_pipeline_content_hash_file(const char *path, char *out, size_t out_sz) {
    if (!path || !out || out_sz <= CBM_DELTA_XXH64_HEX_LEN) {
        return CBM_STORE_ERR;
    }
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        return CBM_STORE_ERR;
    }

    XXH3_state_t *state = XXH3_createState();
    if (!state) {
        fclose(fp);
        return CBM_STORE_ERR;
    }
    if (XXH3_64bits_reset(state) == XXH_ERROR) {
        XXH3_freeState(state);
        fclose(fp);
        return CBM_STORE_ERR;
    }

    unsigned char buf[CBM_SZ_64K];
    int rc = CBM_STORE_OK;
    for (;;) {
        size_t n = fread(buf, CBM_ALLOC_ONE, sizeof(buf), fp);
        if (n > 0 && XXH3_64bits_update(state, buf, n) == XXH_ERROR) {
            rc = CBM_STORE_ERR;
            break;
        }
        if (n < sizeof(buf)) {
            if (ferror(fp)) {
                rc = CBM_STORE_ERR;
            }
            break;
        }
    }
    uint64_t hash = XXH3_64bits_digest(state);
    XXH3_freeState(state);
    if (fclose(fp) != 0) {
        rc = CBM_STORE_ERR;
    }
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    int n = snprintf(out, out_sz, "%0*" PRIx64, CBM_DELTA_XXH64_HEX_LEN, hash);
    return n == CBM_DELTA_XXH64_HEX_LEN ? CBM_STORE_OK : CBM_STORE_ERR;
}

bool cbm_pipeline_file_state_is_current_or_legacy(cbm_store_t *store, const char *project,
                                                  const cbm_file_info_t *file,
                                                  const char *pass_fingerprint) {
    if (!store || !project || !project[0] || !file || !file->path || !file->rel_path) {
        return true;
    }

    cbm_file_state_t state = {0};
    int rc = cbm_store_get_file_state(store, project, file->rel_path, &state);
    if (rc == CBM_STORE_NOT_FOUND) {
        return true;
    }
    const char *current_pass =
        pass_fingerprint ? pass_fingerprint : cbm_pipeline_file_delta_pass_fingerprint();
    bool matches = false;
    if (rc == CBM_STORE_OK && state.content_hash && state.content_hash[0] &&
        state.pass_fingerprint && strcmp(state.pass_fingerprint, current_pass) == 0) {
        char current_hash[CBM_SZ_32];
        matches =
            (cbm_pipeline_content_hash_file(file->path, current_hash, sizeof(current_hash)) ==
                 CBM_STORE_OK &&
             strcmp(current_hash, state.content_hash) == 0);
    }
    cbm_store_file_state_free_fields(&state);
    return matches;
}

int cbm_pipeline_persist_file_states(cbm_store_t *store, const char *project,
                                     const cbm_file_info_t *files, int file_count,
                                     int64_t generation, const char *pass_fingerprint) {
    if (!store || !project || !project[0] || file_count < 0 || (file_count > 0 && !files) ||
        generation < 0) {
        return CBM_STORE_ERR;
    }
    int rc = cbm_store_begin(store);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    for (int i = 0; i < file_count; i++) {
        if (!files[i].path || !files[i].rel_path || !files[i].rel_path[0]) {
            (void)cbm_store_rollback(store);
            return CBM_STORE_ERR;
        }
        struct stat st;
        if (stat(files[i].path, &st) != 0) {
            (void)cbm_store_rollback(store);
            return CBM_STORE_ERR;
        }
        char content_hash[CBM_SZ_32];
        if (cbm_pipeline_content_hash_file(files[i].path, content_hash, sizeof(content_hash)) !=
            CBM_STORE_OK) {
            (void)cbm_store_rollback(store);
            return CBM_STORE_ERR;
        }
        char indexed_at[CBM_SZ_32];
        if (delta_iso_now(indexed_at, sizeof(indexed_at)) != CBM_STORE_OK) {
            (void)cbm_store_rollback(store);
            return CBM_STORE_ERR;
        }
        cbm_file_state_t state = {.project = project,
                                  .rel_path = files[i].rel_path,
                                  .content_hash = content_hash,
                                  .git_oid = NULL,
                                  .mtime_ns = cbm_pipeline_stat_mtime_ns(&st),
                                  .size = st.st_size,
                                  .language = cbm_language_name(files[i].language),
                                  .pass_fingerprint = pass_fingerprint ? pass_fingerprint
                                                                       : cbm_pipeline_file_delta_pass_fingerprint(),
                                  .generation = generation,
                                  .indexed_at = indexed_at};
        rc = cbm_store_upsert_file_state(store, &state);
        if (rc != CBM_STORE_OK) {
            (void)cbm_store_rollback(store);
            return rc;
        }
    }
    rc = cbm_store_commit(store);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(store);
        return rc;
    }
    return CBM_STORE_OK;
}

int cbm_pipeline_attach_file_delta_metadata(cbm_pipeline_file_delta_t *delta,
                                            const cbm_file_info_t *file) {
    if (!delta || !file || !file->path || !delta->delta.project || !delta->delta.rel_path ||
        delta->delta.generation < 0) {
        return CBM_STORE_ERR;
    }
    struct stat st;
    if (stat(file->path, &st) != 0) {
        return CBM_STORE_ERR;
    }
    if (cbm_pipeline_content_hash_file(file->path, delta->file_content_hash,
                                       sizeof(delta->file_content_hash)) != CBM_STORE_OK) {
        return CBM_STORE_ERR;
    }
    if (delta_iso_now(delta->file_indexed_at, sizeof(delta->file_indexed_at)) != CBM_STORE_OK) {
        return CBM_STORE_ERR;
    }

    int64_t mtime_ns = cbm_pipeline_stat_mtime_ns(&st);
    delta->file_hash = (cbm_file_hash_t){.project = delta->delta.project,
                                         .rel_path = delta->delta.rel_path,
                                         .sha256 = cbm_delta_file_hash_legacy_empty,
                                         .mtime_ns = mtime_ns,
                                         .size = st.st_size};
    delta->file_state = (cbm_file_state_t){.project = delta->delta.project,
                                           .rel_path = delta->delta.rel_path,
                                           .content_hash = delta->file_content_hash,
                                           .git_oid = NULL,
                                           .mtime_ns = mtime_ns,
                                           .size = st.st_size,
                                           .language = cbm_language_name(file->language),
                                           .pass_fingerprint =
                                               cbm_pipeline_file_delta_pass_fingerprint(),
                                           .generation = delta->delta.generation,
                                           .indexed_at = delta->file_indexed_at};
    delta->delta.file_hash = &delta->file_hash;
    delta->delta.file_state = &delta->file_state;
    return CBM_STORE_OK;
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

static bool delta_existing_ownership_available(cbm_store_t *store,
                                               const cbm_store_file_delta_t *delta,
                                               cbm_pipeline_file_delta_plan_t *plan) {
    cbm_file_state_t state = {0};
    int rc = cbm_store_get_file_state(store, delta->project, delta->rel_path, &state);
    cbm_store_file_state_free_fields(&state);
    if (rc == CBM_STORE_NOT_FOUND) {
        delta_plan_set_fallback(plan, cbm_delta_reason_missing_existing_ownership);
        return false;
    }
    if (rc != CBM_STORE_OK) {
        delta_plan_set_fallback(plan, cbm_delta_reason_preflight_error);
        return false;
    }

    int node_owners = 0;
    int edge_owners = 0;
    rc = cbm_store_count_file_delta_owners(store, delta->project, delta->rel_path, &node_owners,
                                           &edge_owners);
    if (rc != CBM_STORE_OK) {
        delta_plan_set_fallback(plan, cbm_delta_reason_preflight_error);
        return false;
    }
    if (node_owners <= 0) {
        delta_plan_set_fallback(plan, cbm_delta_reason_missing_existing_ownership);
        return false;
    }
    return true;
}

static bool delta_path_in_batch(const char *path, const cbm_pipeline_file_delta_t *const *deltas,
                                int delta_count) {
    if (!path || !*path || !deltas) {
        return false;
    }
    for (int i = 0; i < delta_count; i++) {
        if (deltas[i] && deltas[i]->delta.rel_path &&
            strcmp(path, deltas[i]->delta.rel_path) == 0) {
            return true;
        }
    }
    return false;
}

static bool delta_inbound_edges_supported(cbm_store_t *store,
                                          const cbm_pipeline_file_delta_t *delta,
                                          const cbm_pipeline_file_delta_t *const *deltas,
                                          int delta_count,
                                          cbm_pipeline_file_delta_plan_t *plan) {
    char **source_paths = NULL;
    int source_count = 0;
    int rc = cbm_store_list_file_delta_inbound_source_paths(
        store, delta->delta.project, delta->delta.rel_path, &source_paths, &source_count);
    if (rc != CBM_STORE_OK) {
        delta_plan_set_fallback(plan, cbm_delta_reason_preflight_error);
        return false;
    }
    bool ok = true;
    for (int i = 0; i < source_count; i++) {
        if (!delta_path_in_batch(source_paths[i], deltas, delta_count)) {
            ok = false;
            break;
        }
    }
    for (int i = 0; i < source_count; i++) {
        free(source_paths[i]);
    }
    free(source_paths);
    if (!ok) {
        delta_plan_set_fallback(plan, cbm_delta_reason_inbound_edges_require_full);
    }
    return ok;
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

static bool delta_plan_precheck_common(const cbm_pipeline_file_delta_t *delta,
                                       cbm_pipeline_file_delta_plan_t *plan) {
    if (delta->change_kind == CBM_PIPELINE_DELTA_CHANGE_DELETE) {
        delta_plan_set_fallback(plan, cbm_delta_reason_delete_requires_full);
        return false;
    }
    if (delta->change_kind == CBM_PIPELINE_DELTA_CHANGE_RENAME) {
        delta_plan_set_fallback(plan, cbm_delta_reason_rename_requires_full);
        return false;
    }
    if (delta->unsupported_edge_count > 0) {
        delta_plan_set_fallback(plan, cbm_delta_reason_unsupported_edges);
        return false;
    }
    if (!delta_file_metadata_complete(&delta->delta)) {
        delta_plan_set_fallback(plan, cbm_delta_reason_missing_file_metadata);
        return false;
    }
    if (!delta_derived_view_supported(&delta->delta)) {
        delta_plan_set_fallback(plan, cbm_delta_reason_unsupported_derived_view);
        return false;
    }
    return true;
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

static bool delta_batch_node_qn_present(const cbm_pipeline_file_delta_t *const *deltas,
                                        int delta_count, const char *qn) {
    if (!deltas || !qn) {
        return false;
    }
    for (int i = 0; i < delta_count; i++) {
        if (deltas[i] && delta_node_qn_present(&deltas[i]->delta, qn)) {
            return true;
        }
    }
    return false;
}

static int delta_batch_edge_endpoints_resolve(cbm_store_t *store,
                                              const cbm_store_file_delta_t *delta,
                                              const cbm_pipeline_file_delta_t *const *deltas,
                                              int delta_count) {
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
            if (!delta_batch_node_qn_present(deltas, delta_count, qn) &&
                !delta_qn_list_contains(qns, qn_count, qn)) {
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

static int delta_plan_append_affected_path(cbm_pipeline_file_delta_plan_t *plan,
                                           const char *path) {
    if (!plan || !path) {
        return CBM_STORE_ERR;
    }
    for (int i = 0; i < plan->affected_count; i++) {
        if (strcmp(plan->affected_paths[i], path) == 0) {
            return CBM_STORE_OK;
        }
    }
    char *dup = delta_strdup(path);
    if (!dup) {
        return CBM_STORE_ERR;
    }
    char **next =
        realloc(plan->affected_paths, (size_t)(plan->affected_count + 1) * sizeof(*next));
    if (!next) {
        free(dup);
        return CBM_STORE_ERR;
    }
    plan->affected_paths = next;
    plan->affected_paths[plan->affected_count++] = dup;
    return CBM_STORE_OK;
}

static int delta_plan_append_frontier(cbm_pipeline_file_delta_plan_t *plan, char **paths,
                                      int count) {
    for (int i = 0; i < count; i++) {
        if (delta_plan_append_affected_path(plan, paths[i]) != CBM_STORE_OK) {
            return CBM_STORE_ERR;
        }
    }
    return CBM_STORE_OK;
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
    if (!delta_plan_precheck_common(delta, out)) {
        return CBM_STORE_OK;
    }
    if (!delta_existing_ownership_available(store, &delta->delta, out)) {
        return CBM_STORE_OK;
    }
    enum { CBM_DELTA_SINGLE_COUNT = 1 };
    const cbm_pipeline_file_delta_t *single_delta[] = {delta};
    if (!delta_inbound_edges_supported(store, delta, single_delta, CBM_DELTA_SINGLE_COUNT, out)) {
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

int cbm_pipeline_plan_file_delta_batch(cbm_store_t *store,
                                       const cbm_pipeline_file_delta_t *const *deltas,
                                       int delta_count, int max_affected_paths,
                                       cbm_pipeline_file_delta_plan_t *out) {
    if (!out) {
        return CBM_STORE_ERR;
    }
    memset(out, 0, sizeof(*out));
    delta_plan_set_fallback(out, cbm_delta_reason_invalid_input);
    if (!store || !deltas || delta_count <= 0 || max_affected_paths <= 0) {
        return CBM_STORE_OK;
    }

    const char *project = NULL;
    for (int i = 0; i < delta_count; i++) {
        const cbm_pipeline_file_delta_t *delta = deltas[i];
        if (!delta || !delta->delta.project || !delta->delta.rel_path) {
            return CBM_STORE_OK;
        }
        if (!project) {
            project = delta->delta.project;
        } else if (strcmp(project, delta->delta.project) != 0) {
            return CBM_STORE_OK;
        }
        if (!delta_plan_precheck_common(delta, out)) {
            return CBM_STORE_OK;
        }
        if (!delta_existing_ownership_available(store, &delta->delta, out)) {
            return CBM_STORE_OK;
        }
        if (!delta_inbound_edges_supported(store, delta, deltas, delta_count, out)) {
            return CBM_STORE_OK;
        }
        int endpoint_rc =
            delta_batch_edge_endpoints_resolve(store, &delta->delta, deltas, delta_count);
        if (endpoint_rc == CBM_STORE_NOT_FOUND) {
            delta_plan_set_fallback(out, cbm_delta_reason_unresolved_edge_endpoint);
            return CBM_STORE_OK;
        }
        if (endpoint_rc != CBM_STORE_OK) {
            delta_plan_set_fallback(out, cbm_delta_reason_preflight_error);
            return CBM_STORE_OK;
        }
    }

    for (int i = 0; i < delta_count; i++) {
        const cbm_store_file_delta_t *delta = &deltas[i]->delta;
        const char **new_export_qns = NULL;
        if (delta->export_count > 0) {
            new_export_qns = malloc((size_t)delta->export_count * sizeof(*new_export_qns));
            if (!new_export_qns) {
                delta_plan_set_fallback(out, cbm_delta_reason_frontier_error);
                return CBM_STORE_OK;
            }
            for (int j = 0; j < delta->export_count; j++) {
                new_export_qns[j] = delta->exports[j].qualified_name;
            }
        }

        char **paths = NULL;
        int path_count = 0;
        int rc = cbm_store_list_file_delta_affected_paths(
            store, delta->project, delta->rel_path, new_export_qns, delta->export_count, &paths,
            &path_count);
        free(new_export_qns);
        if (rc != CBM_STORE_OK ||
            delta_plan_append_frontier(out, paths, path_count) != CBM_STORE_OK) {
            for (int j = 0; j < path_count; j++) {
                free(paths[j]);
            }
            free(paths);
            delta_plan_set_fallback(out, cbm_delta_reason_frontier_error);
            return CBM_STORE_OK;
        }
        for (int j = 0; j < path_count; j++) {
            free(paths[j]);
        }
        free(paths);
        if (out->affected_count > max_affected_paths) {
            delta_plan_set_fallback(out, cbm_delta_reason_frontier_too_large);
            return CBM_STORE_OK;
        }
    }

    out->route = CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE;
    out->reason = cbm_delta_reason_candidate;
    return CBM_STORE_OK;
}

int cbm_pipeline_apply_file_delta_batch(cbm_store_t *store,
                                        const cbm_pipeline_file_delta_t *const *deltas,
                                        int delta_count, int max_affected_paths,
                                        cbm_pipeline_file_delta_plan_t *out) {
    int rc =
        cbm_pipeline_plan_file_delta_batch(store, deltas, delta_count, max_affected_paths, out);
    if (rc != CBM_STORE_OK || !out || out->route != CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE) {
        return rc;
    }

    const cbm_store_file_delta_t **publish_deltas =
        malloc((size_t)delta_count * sizeof(*publish_deltas));
    if (!publish_deltas) {
        delta_plan_set_fallback(out, cbm_delta_reason_preflight_error);
        return CBM_STORE_OK;
    }
    for (int i = 0; i < delta_count; i++) {
        publish_deltas[i] = &deltas[i]->delta;
    }
    rc = cbm_store_publish_file_delta_batch_complete(store, publish_deltas, delta_count);
    free(publish_deltas);
    if (rc != CBM_STORE_OK) {
        delta_plan_set_fallback(out, cbm_delta_reason_publish_error);
        return CBM_STORE_OK;
    }
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
