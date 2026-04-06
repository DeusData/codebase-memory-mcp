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

#include "foundation/profile.h"
#include "foundation/platform.h"

enum {
    SIM_EDGE_INIT_CAP = 256,
    SIM_EDGE_GROW = 2,
};
#include "pipeline/worker_pool.h"

#include <stdatomic.h>
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

/* ── Parallel query + emit ────────────────────────────────────────── */

/* Deferred edge record; collected per-worker, merged into gbuf sequentially. */
typedef struct {
    int64_t source_id;
    int64_t target_id;
    double jaccard;
    bool same_file;
} sim_deferred_edge_t;

typedef struct {
    sim_deferred_edge_t *edges;
    int count;
    int cap;
} sim_edge_buf_t;

static void sim_edge_buf_push(sim_edge_buf_t *buf, int64_t src, int64_t tgt, double jaccard,
                              bool same_file) {
    if (buf->count >= buf->cap) {
        int nc = buf->cap < SIM_EDGE_INIT_CAP ? SIM_EDGE_INIT_CAP : buf->cap * SIM_EDGE_GROW;
        sim_deferred_edge_t *grown = realloc(buf->edges, (size_t)nc * sizeof(sim_deferred_edge_t));
        if (!grown) {
            return;
        }
        buf->edges = grown;
        buf->cap = nc;
    }
    buf->edges[buf->count++] = (sim_deferred_edge_t){
        .source_id = src, .target_id = tgt, .jaccard = jaccard, .same_file = same_file};
}

typedef struct {
    const fp_entry_t *entries;
    int entry_count;
    const cbm_lsh_index_t *lsh;
    sim_edge_buf_t *worker_bufs;
    _Atomic int next_idx;
    _Atomic int *edge_counts; /* shared atomic array, one per entry */
} sim_query_ctx_t;

enum { SIM_CAND_CAP = 4096 };

static void sim_query_worker(int worker_id, void *ctx_ptr) {
    sim_query_ctx_t *sc = ctx_ptr;
    sim_edge_buf_t *my_buf = &sc->worker_bufs[worker_id];

    /* Thread-local candidate buffer (stack-allocated) */
    const cbm_lsh_entry_t *cands[SIM_CAND_CAP];

    while (true) {
        int i = atomic_fetch_add_explicit(&sc->next_idx, SKIP_ONE, memory_order_relaxed);
        if (i >= sc->entry_count) {
            break;
        }

        int ec = atomic_load_explicit(&sc->edge_counts[i], memory_order_relaxed);
        if (ec >= CBM_MINHASH_MAX_EDGES_PER_NODE) {
            continue;
        }

        const fp_entry_t *src = &sc->entries[i];
        int cand_count = cbm_lsh_query_into(sc->lsh, &src->fp, cands, SIM_CAND_CAP);

        int emitted = 0;
        for (int c = 0; c < cand_count; c++) {
            const cbm_lsh_entry_t *cand = cands[c];
            if (cand->node_id == src->node_id) {
                continue;
            }
            if (strcmp(src->ext, cand->file_ext) != 0) {
                continue;
            }
            if (src->node_id >= cand->node_id) {
                continue;
            }

            int cur = atomic_load_explicit(&sc->edge_counts[i], memory_order_relaxed);
            if (cur + emitted >= CBM_MINHASH_MAX_EDGES_PER_NODE) {
                break;
            }

            double jaccard = cbm_minhash_jaccard(&src->fp, cand->fingerprint);
            if (jaccard < CBM_MINHASH_JACCARD_THRESHOLD) {
                continue;
            }

            bool same_file =
                src->file_path && cand->file_path && strcmp(src->file_path, cand->file_path) == 0;
            sim_edge_buf_push(my_buf, src->node_id, cand->node_id, jaccard, same_file);
            emitted++;
        }
        if (emitted > 0) {
            atomic_fetch_add_explicit(&sc->edge_counts[i], emitted, memory_order_relaxed);
        }
    }
}

/* Merge worker edge buffers into gbuf. Returns total edge count. Frees worker buffers. */
static int merge_sim_edges(cbm_gbuf_t *gbuf, sim_edge_buf_t *worker_bufs, int worker_count) {
    int total = 0;
    for (int w = 0; w < worker_count; w++) {
        for (int e = 0; e < worker_bufs[w].count; e++) {
            sim_deferred_edge_t *de = &worker_bufs[w].edges[e];
            char props[PROPS_BUF_LEN];
            snprintf(props, sizeof(props), "{\"jaccard\":%.3f,\"same_file\":%s}", de->jaccard,
                     de->same_file ? "true" : "false");
            cbm_gbuf_insert_edge(gbuf, de->source_id, de->target_id, "SIMILAR_TO", props);
            total++;
        }
        free(worker_bufs[w].edges);
    }
    free(worker_bufs);
    return total;
}

/* ── Pass entry point ────────────────────────────────────────────── */

int cbm_pipeline_pass_similarity(cbm_pipeline_ctx_t *ctx) {
    cbm_log_info("pass.start", "pass", "similarity");

    cbm_gbuf_t *gbuf = ctx->gbuf;

    /* Phase 1: Collect fingerprints from Function/Method nodes */
    CBM_PROF_START(t_collect);
    fp_entry_t *entries = NULL;
    int entry_count = collect_fp_entries(gbuf, &entries);
    CBM_PROF_END_N("similarity", "1_collect_fp", t_collect, entry_count);

    cbm_log_info("pass.similarity.collected", "nodes_with_fp", itoa_log(entry_count));

    if (entry_count < MIN_FP_ENTRIES) {
        free(entries);
        cbm_log_info("pass.done", "pass", "similarity", "edges", "0");
        return 0;
    }

    /* Phase 2: Build LSH index (sequential — cbm_lsh_insert mutates shared state) */
    CBM_PROF_START(t_lsh_build);
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
    CBM_PROF_END_N("similarity", "2_lsh_build_seq", t_lsh_build, entry_count);

    /* Phase 3: Query LSH + emit edges (PARALLEL via cbm_lsh_query_into).
     * Each worker claims entries, queries, scores candidates, stashes edges
     * in its own deferred buffer. Shared edge_counts is atomic.
     * Final merge into gbuf is sequential (gbuf not thread-safe). */
    CBM_PROF_START(t_query_emit);
    _Atomic int *edge_counts = calloc((size_t)entry_count, sizeof(_Atomic int));
    int worker_count = cbm_default_worker_count(false);
    sim_edge_buf_t *worker_bufs = calloc((size_t)worker_count, sizeof(sim_edge_buf_t));

    {
        sim_query_ctx_t sc = {
            .entries = entries,
            .entry_count = entry_count,
            .lsh = lsh,
            .worker_bufs = worker_bufs,
            .edge_counts = edge_counts,
        };
        atomic_init(&sc.next_idx, 0);
        cbm_parallel_for_opts_t opts = {.max_workers = worker_count, .force_pthreads = false};
        cbm_parallel_for(worker_count, sim_query_worker, &sc, opts);
    }
    CBM_PROF_END_N("similarity", "3_query_parallel", t_query_emit, entry_count);

    CBM_PROF_START(t_merge);
    int total_edges = merge_sim_edges(gbuf, worker_bufs, worker_count);
    CBM_PROF_END_N("similarity", "4_edge_merge_seq", t_merge, total_edges);

    cbm_log_info("pass.done", "pass", "similarity", "edges", itoa_log(total_edges));

    free(edge_counts);
    free(lsh_entries);
    free(entries);
    cbm_lsh_free(lsh);
    return 0;
}
