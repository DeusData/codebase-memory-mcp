/*
 * pass_similarity.c — Generate SIMILAR_TO edges from MinHash fingerprints.
 *
 * Reads "fp" hex strings from Function/Method node properties,
 * builds an LSH index, and emits SIMILAR_TO edges for pairs with
 * Jaccard similarity ≥ threshold.
 *
 * Runs as a post-pass after enrichment (both full and incremental).
 */
#include "foundation/constants.h"
#include "pipeline/pipeline.h"
#include <stdint.h>
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "simhash/minhash.h"
#include "foundation/log.h"
#include "foundation/compat.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { FP_KEY_PREFIX_LEN = 6, MIN_FP_ENTRIES = 2 }; /* strlen("\"fp\":\"") */

/* ── Helpers ─────────────────────────────────────────────────────── */

/* Extract file extension from a path (including the dot). */
static const char *file_ext(const char *path) {
    if (!path) {
        return "";
    }
    const char *dot = strrchr(path, '.');
    return dot ? dot : "";
}

/* Parse "fp" hex string from a node's properties_json.
 * Returns true if found and decoded successfully. */
static bool parse_fp_from_props(const char *props_json, cbm_minhash_t *out) {
    if (!props_json) {
        return false;
    }
    const char *fp_key = strstr(props_json, "\"fp\":\"");
    if (!fp_key) {
        return false;
    }
    const char *hex_start = fp_key + FP_KEY_PREFIX_LEN;
    /* Find closing quote */
    const char *hex_end = strchr(hex_start, '"');
    if (!hex_end) {
        return false;
    }
    int hex_len = (int)(hex_end - hex_start);
    if (hex_len != CBM_MINHASH_HEX_LEN) {
        return false;
    }
    char hex_buf[CBM_MINHASH_HEX_BUF];
    memcpy(hex_buf, hex_start, (size_t)hex_len);
    hex_buf[hex_len] = '\0';
    return cbm_minhash_from_hex(hex_buf, out);
}

/* Log helper for integer-to-string in log calls. */
static const char *itoa_log(int val) {
    enum { RING_BUF_COUNT = 4, RING_BUF_MASK = 3 };
    static CBM_TLS char bufs[RING_BUF_COUNT][CBM_SZ_32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + SKIP_ONE) & RING_BUF_MASK;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* ── Internal types ──────────────────────────────────────────────── */

enum { FP_ENTRY_INIT_CAP = 256, FP_ENTRY_GROW = 2, PROPS_BUF_LEN = 256 };

typedef struct {
    int64_t node_id;
    cbm_minhash_t fp;
    const char *file_path;
    const char *ext;
} fp_entry_t;

/* Collect all Function/Method nodes with fingerprints from graph buffer. */
static int collect_fp_entries(cbm_gbuf_t *gbuf, fp_entry_t **out_entries) {
    fp_entry_t *entries = NULL;
    int count = 0;
    int cap = 0;

    const char *labels[] = {"Function", "Method", NULL};
    for (int li = 0; labels[li]; li++) {
        const cbm_gbuf_node_t **nodes = NULL;
        int node_count = 0;
        if (cbm_gbuf_find_by_label(gbuf, labels[li], &nodes, &node_count) != 0) {
            continue;
        }
        for (int i = 0; i < node_count; i++) {
            const cbm_gbuf_node_t *n = nodes[i];
            cbm_minhash_t fp;
            if (!parse_fp_from_props(n->properties_json, &fp)) {
                continue;
            }
            if (count >= cap) {
                int new_cap = cap < FP_ENTRY_INIT_CAP ? FP_ENTRY_INIT_CAP : cap * FP_ENTRY_GROW;
                fp_entry_t *grown = realloc(entries, (size_t)new_cap * sizeof(fp_entry_t));
                if (!grown) {
                    break;
                }
                entries = grown;
                cap = new_cap;
            }
            entries[count++] = (fp_entry_t){
                .node_id = n->id,
                .fp = fp,
                .file_path = n->file_path,
                .ext = file_ext(n->file_path),
            };
        }
    }
    *out_entries = entries;
    return count;
}

/* Emit SIMILAR_TO edges for one source entry against LSH candidates. */
static int emit_edges_for_entry(cbm_gbuf_t *gbuf, const fp_entry_t *src,
                                const cbm_lsh_entry_t **candidates, int cand_count,
                                int edge_count) {
    int emitted = 0;
    for (int c = 0; c < cand_count; c++) {
        const cbm_lsh_entry_t *cand = candidates[c];
        if (cand->node_id == src->node_id) {
            continue;
        }
        if (strcmp(src->ext, cand->file_ext) != 0) {
            continue;
        }
        if (src->node_id >= cand->node_id) {
            continue;
        }
        if (edge_count + emitted >= CBM_MINHASH_MAX_EDGES_PER_NODE) {
            break;
        }
        double jaccard = cbm_minhash_jaccard(&src->fp, cand->fingerprint);
        if (jaccard < CBM_MINHASH_JACCARD_THRESHOLD) {
            continue;
        }
        bool same_file =
            src->file_path && cand->file_path && strcmp(src->file_path, cand->file_path) == 0;
        char props[PROPS_BUF_LEN];
        snprintf(props, sizeof(props), "{\"jaccard\":%.3f,\"same_file\":%s}", jaccard,
                 same_file ? "true" : "false");
        cbm_gbuf_insert_edge(gbuf, src->node_id, cand->node_id, "SIMILAR_TO", props);
        emitted++;
    }
    return emitted;
}

/* ── Pass entry point ────────────────────────────────────────────── */

int cbm_pipeline_pass_similarity(cbm_pipeline_ctx_t *ctx) {
    cbm_log_info("pass.start", "pass", "similarity");

    cbm_gbuf_t *gbuf = ctx->gbuf;

    fp_entry_t *entries = NULL;
    int entry_count = collect_fp_entries(gbuf, &entries);

    cbm_log_info("pass.similarity.collected", "nodes_with_fp", itoa_log(entry_count));

    if (entry_count < MIN_FP_ENTRIES) {
        free(entries);
        cbm_log_info("pass.done", "pass", "similarity", "edges", "0");
        return 0;
    }

    /* Build LSH index */
    cbm_lsh_index_t *lsh = cbm_lsh_new();
    cbm_lsh_entry_t *lsh_entries = malloc((size_t)entry_count * sizeof(cbm_lsh_entry_t));
    if (!lsh_entries) {
        free(entries);
        cbm_lsh_free(lsh);
        return CBM_NOT_FOUND;
    }

    for (int i = 0; i < entry_count; i++) {
        lsh_entries[i] = (cbm_lsh_entry_t){
            .node_id = entries[i].node_id,
            .fingerprint = &entries[i].fp,
            .file_path = entries[i].file_path,
            .file_ext = entries[i].ext,
        };
        cbm_lsh_insert(lsh, &lsh_entries[i]);
    }

    /* Query and emit edges */
    int *edge_counts = calloc((size_t)entry_count, sizeof(int));
    int total_edges = 0;

    for (int i = 0; i < entry_count; i++) {
        if (edge_counts[i] >= CBM_MINHASH_MAX_EDGES_PER_NODE) {
            continue;
        }
        const cbm_lsh_entry_t **candidates = NULL;
        int cand_count = 0;
        cbm_lsh_query(lsh, &entries[i].fp, &candidates, &cand_count);

        int emitted =
            emit_edges_for_entry(gbuf, &entries[i], candidates, cand_count, edge_counts[i]);
        edge_counts[i] += emitted;
        total_edges += emitted;
    }

    cbm_log_info("pass.done", "pass", "similarity", "edges", itoa_log(total_edges));

    free(edge_counts);
    free(lsh_entries);
    free(entries);
    cbm_lsh_free(lsh);
    return 0;
}
