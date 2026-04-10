// extract_k8s.c — K8s manifest and Kustomize file extractor.
//
// For CBM_LANG_KUSTOMIZE: walks top-level block_mapping_pair nodes whose key
// matches "resources", "bases", "patches", "components", or
// "patchesStrategicMerge", then emits one CBMImport per block_sequence item.
//
// For CBM_LANG_K8S: finds apiVersion, kind, and metadata.name scalars in the
// first document's block_mapping and emits one CBMDefinition with label
// "Resource" and name "Kind/metadata-name".

#include "cbm.h"
#include "arena.h"
#include "helpers.h"
#include "tree_sitter/api.h"
#include <stdint.h>
#include <stdio.h>
#include <string.h>

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// Return the raw source text for a scalar node (plain, single-quoted, or
// double-quoted). Surrounding quote characters are stripped for quoted forms.
// Handles flow_node wrappers transparently by descending into the first named
// child (the tree-sitter YAML grammar often wraps scalars in flow_node).
// Returns NULL for non-scalar node types.
static const char *get_scalar_text(CBMArena *a, TSNode node, const char *source) {
    const char *type = ts_node_type(node);
    // Unwrap flow_node: the actual scalar is the first named child
    if (strcmp(type, "flow_node") == 0) {
        TSNode inner = ts_node_named_child(node, 0);
        if (ts_node_is_null(inner)) {
            return NULL;
        }
        return get_scalar_text(a, inner, source);
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
        if (len >= 2) {
            return cbm_arena_strndup(a, raw + 1, len - 2);
        }
        return raw;
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
    uint32_t n = ts_node_child_count(seq_node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode item = ts_node_child(seq_node, i);
        if (strcmp(ts_node_type(item), "block_sequence_item") != 0) {
            continue;
        }
        // block_sequence_item has one named child: the value
        uint32_t ic = ts_node_child_count(item);
        for (uint32_t j = 0; j < ic; j++) {
            TSNode val = ts_node_child(item, j);
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

static void extract_kustomize(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;

    // Traverse: stream -> document -> block_node -> block_mapping -> block_mapping_pair
    TSNode root = ctx->root;
    uint32_t root_n = ts_node_child_count(root);
    for (uint32_t si = 0; si < root_n; si++) {
        TSNode stream_child = ts_node_child(root, si);
        if (strcmp(ts_node_type(stream_child), "document") != 0) {
            continue;
        }
        // Find block_mapping inside the document (may be wrapped in block_node)
        TSNode mapping = ts_node_named_child(stream_child, 0);
        if (ts_node_is_null(mapping)) {
            continue;
        }
        // Some grammars wrap in block_node
        if (strcmp(ts_node_type(mapping), "block_node") == 0) {
            mapping = ts_node_named_child(mapping, 0);
        }
        if (ts_node_is_null(mapping) || strcmp(ts_node_type(mapping), "block_mapping") != 0) {
            continue;
        }

        uint32_t pair_n = ts_node_child_count(mapping);
        for (uint32_t pi = 0; pi < pair_n; pi++) {
            TSNode pair = ts_node_child(mapping, pi);
            if (strcmp(ts_node_type(pair), "block_mapping_pair") != 0) {
                continue;
            }

            // First named child = key
            TSNode key_node = ts_node_named_child(pair, 0);
            if (ts_node_is_null(key_node)) {
                continue;
            }
            const char *key_text = get_scalar_text(a, key_node, ctx->source);
            if (!key_text || !is_kustomize_list_key(key_text)) {
                continue;
            }

            // Second named child = value (should be a block_sequence or block_node wrapping one)
            TSNode val_node = ts_node_named_child(pair, 1);
            if (ts_node_is_null(val_node)) {
                continue;
            }
            if (strcmp(ts_node_type(val_node), "block_node") == 0) {
                val_node = ts_node_named_child(val_node, 0);
            }
            if (ts_node_is_null(val_node) ||
                strcmp(ts_node_type(val_node), "block_sequence") != 0) {
                continue;
            }

            emit_kustomize_sequence(ctx, val_node, key_text);
        }
    }
}

// ---------------------------------------------------------------------------
// K8s manifest extraction
// ---------------------------------------------------------------------------

// Descend into the first block_mapping of a document and extract apiVersion,
// kind, and metadata.name. Returns void; fills kind_buf and meta_name_buf.
static void extract_k8s_scalars(CBMExtractCtx *ctx, TSNode mapping, char *kind_buf, size_t kind_sz,
                                char *meta_name_buf, size_t meta_sz) {
    CBMArena *a = ctx->arena;
    kind_buf[0] = '\0';
    meta_name_buf[0] = '\0';

    uint32_t n = ts_node_child_count(mapping);
    for (uint32_t i = 0; i < n; i++) {
        TSNode pair = ts_node_child(mapping, i);
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

        TSNode val_node = ts_node_named_child(pair, 1);
        if (ts_node_is_null(val_node)) {
            continue;
        }
        // Unwrap block_node if present
        if (strcmp(ts_node_type(val_node), "block_node") == 0) {
            val_node = ts_node_named_child(val_node, 0);
        }
        if (ts_node_is_null(val_node)) {
            continue;
        }

        if (strcmp(key, "kind") == 0) {
            const char *v = get_scalar_text(a, val_node, ctx->source);
            if (v) {
                snprintf(kind_buf, kind_sz, "%s", v);
            }
        } else if (strcmp(key, "metadata") == 0) {
            // Descend into metadata block_mapping to find "name"
            // val_node is already unwrapped from block_node above.
            TSNode meta_mapping = val_node;
            if (ts_node_is_null(meta_mapping) ||
                strcmp(ts_node_type(meta_mapping), "block_mapping") != 0) {
                continue;
            }
            uint32_t mn = ts_node_child_count(meta_mapping);
            for (uint32_t mi = 0; mi < mn; mi++) {
                TSNode mpair = ts_node_child(meta_mapping, mi);
                if (strcmp(ts_node_type(mpair), "block_mapping_pair") != 0) {
                    continue;
                }
                TSNode mkey = ts_node_named_child(mpair, 0);
                if (ts_node_is_null(mkey)) {
                    continue;
                }
                const char *mkey_text = get_scalar_text(a, mkey, ctx->source);
                if (!mkey_text || strcmp(mkey_text, "name") != 0) {
                    continue;
                }
                TSNode mval = ts_node_named_child(mpair, 1);
                if (ts_node_is_null(mval)) {
                    continue;
                }
                const char *meta_name = get_scalar_text(a, mval, ctx->source);
                if (meta_name) {
                    snprintf(meta_name_buf, meta_sz, "%s", meta_name);
                }
            }
        }
    }
}

static void extract_k8s_manifest(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;

    TSNode root = ctx->root;
    uint32_t root_n = ts_node_child_count(root);
    for (uint32_t si = 0; si < root_n; si++) {
        TSNode stream_child = ts_node_child(root, si);
        if (strcmp(ts_node_type(stream_child), "document") != 0) {
            continue;
        }

        TSNode mapping = ts_node_named_child(stream_child, 0);
        if (ts_node_is_null(mapping)) {
            continue;
        }
        if (strcmp(ts_node_type(mapping), "block_node") == 0) {
            mapping = ts_node_named_child(mapping, 0);
        }
        if (ts_node_is_null(mapping) || strcmp(ts_node_type(mapping), "block_mapping") != 0) {
            continue;
        }

        char kind_buf[256] = {0};
        char meta_name_buf[256] = {0};
        extract_k8s_scalars(ctx, mapping, kind_buf, sizeof(kind_buf), meta_name_buf,
                            sizeof(meta_name_buf));

        // Skip malformed manifests (no kind or no metadata.name)
        if (kind_buf[0] == '\0' || meta_name_buf[0] == '\0') {
            continue;
        }

        char def_name[512];
        snprintf(def_name, sizeof(def_name), "%s/%s", kind_buf, meta_name_buf);

        CBMDefinition def = {0};
        def.name = cbm_arena_strdup(a, def_name);
        def.qualified_name = cbm_arena_sprintf(a, "%s.%s", ctx->module_qn, def_name);
        def.label = cbm_arena_strdup(a, "Resource");
        def.file_path = ctx->rel_path;
        def.start_line = ts_node_start_point(mapping).row + 1;
        def.end_line = ts_node_end_point(mapping).row + 1;
        cbm_defs_push(&ctx->result->defs, a, def);

        break; // Only the first document per file
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
