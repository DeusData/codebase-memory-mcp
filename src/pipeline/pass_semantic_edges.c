/*
 * pass_semantic_edges.c — Emit SEMANTICALLY_RELATED edges from combined
 * algorithmic embeddings (11 signals, zero external dependencies).
 *
 * Runs as a post-pass after pass_similarity. Reads all Function/Method
 * nodes from the graph buffer, computes TF-IDF + Random Indexing + API
 * signatures + type/decorator vectors + AST profile, builds LSH index,
 * scores candidate pairs, applies graph diffusion, emits edges.
 *
 * Runs in moderate and full modes (not fast). Controlled by pipeline mode.
 */
#include "foundation/constants.h"
#include "foundation/mem.h"
#include "foundation/mem_core.h"
#include "pipeline/pipeline.h"
#include <stdint.h>
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "semantic/semantic.h"
#include "semantic/ast_profile.h"
#include "simhash/minhash.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h" /* cbm_fopen for the pair dump */
#define XXH_INLINE_ALL
#include "xxhash/xxhash.h"

#include "pipeline/worker_pool.h"
#include "foundation/platform.h"
#include "foundation/profile.h"

#include <ctype.h>
#include <math.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Helpers ─────────────────────────────────────────────────────── */

enum {
    PROPS_BUF = 512,
    MAX_FUNCS_INIT = 4096,
    GROW = 2,
    MAX_CALLEES = 64,
    MAX_DEFERRED_EDGES = 8192,
    /* Bit-mask for the low order parity bit used by sparse random indexing. */
    PSE_PARITY_BIT = 1,
    /* Bit shift count for signature bit positions (1 << k). */
    PSE_BIT_1 = 1,
    PSE_BIT_64 = 64,
    PSE_MOD_64 = 64,
    PSE_MINHASH_K = 64,
    PSE_LSH_BAND_COUNT = 2,
    PSE_FP_PREFIX_LEN = 6, /* strlen("\"fp\":\"") */
    PSE_LSH_ROWS_PER_BAND = 2,
    PSE_MIN_FUNCS_FOR_PAIR = 2,
};

/* Scalar weight constants used in score_worker and related helpers. */
#define PSE_UNIT_POS 1.0F
#define PSE_INT8_MAX 127.0F
#define PSE_ROUND_BIAS 0.5F
#define PSE_FLOW_WEIGHT 0.01F
#define PSE_ONE_ULL ((uint64_t)1)

/* ── Deferred edge buffer (thread-local, merged after parallel pass) ── */

typedef struct {
    int64_t source_id;
    int64_t target_id;
    float score;
    bool same_file;
    bool below; /* under the threshold: recorded for CBM_SEM_PAIR_SIGNALS only, never admitted */
    /* Canonical admission keys (determinism): func index of the discovering
     * side, its candidate rank, and the partner's func index. The sequential
     * admission pass replays pairs in (i, c) order so which pairs win the
     * per-node max_edges budget no longer depends on worker scheduling. */
    int i;
    int j;
    int c;
} deferred_edge_t;

typedef struct {
    deferred_edge_t *edges;
    int count;
    int cap;
} deferred_edge_buf_t;

static void deferred_buf_init(deferred_edge_buf_t *buf) {
    buf->edges = NULL;
    buf->count = 0;
    buf->cap = 0;
}

static void deferred_buf_push(deferred_edge_buf_t *buf, int64_t src, int64_t tgt, float score,
                              bool same_file, bool below, int i, int j, int c) {
    if (buf->count >= buf->cap) {
        int nc = buf->cap < CBM_SZ_256 ? CBM_SZ_256 : buf->cap * GROW;
        deferred_edge_t *grown =
            cbm_realloc(CBM_MEM_CLASS_SEMANTIC, buf->edges, (size_t)nc * sizeof(deferred_edge_t));
        if (!grown) {
            return;
        }
        buf->edges = grown;
        buf->cap = nc;
    }
    buf->edges[buf->count++] = (deferred_edge_t){.source_id = src,
                                                 .target_id = tgt,
                                                 .score = score,
                                                 .same_file = same_file,
                                                 .below = below,
                                                 .i = i,
                                                 .j = j,
                                                 .c = c};
}

static void deferred_buf_free(deferred_edge_buf_t *buf) {
    cbm_free(CBM_MEM_CLASS_SEMANTIC, buf->edges);
    buf->edges = NULL;
    buf->count = buf->cap = 0;
}

/* Forward declare helpers used by pattern injection. */
static const char *json_str_value(const char *json, const char *key, char *buf, int bufsize);

/* ── Technique 2: Code pattern vocabulary injection ──────────────── */
/* Inject semantic tokens based on detected code patterns.
 * This bridges the vocabulary gap for abstract concepts like "error handling". */

/* Append a single token to `tokens` if there's capacity.  Consolidates the
 * `if (count < max_tokens) { tokens[count++] = strdup(...); }` pattern. */
static int push_pattern_token(char **tokens, int count, int max_tokens, const char *text) {
    if (count >= max_tokens) {
        return count;
    }
    tokens[count] = cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, text);
    return tokens[count] ? count + SKIP_ONE : count;
}

/* True if `s` contains any of the space-separated substrings in `needles`. */
static bool has_any(const char *s, const char *const *needles) {
    if (!s) {
        return false;
    }
    for (const char *const *n = needles; *n; n++) {
        if (strstr(s, *n)) {
            return true;
        }
    }
    return false;
}

/* Inject tokens derived from body-text patterns (try/catch, raise, log). */
static int inject_body_pattern_tokens(const char *bt, char **tokens, int count, int max_tokens) {
    if (!bt) {
        return count;
    }
    static const char *const ERR_HANDLING[] = {"except", "catch", "rescue", NULL};
    if (has_any(bt, ERR_HANDLING)) {
        count = push_pattern_token(tokens, count, max_tokens, "error");
        count = push_pattern_token(tokens, count, max_tokens, "handling");
        count = push_pattern_token(tokens, count, max_tokens, "exception");
    }
    static const char *const ERR_THROW[] = {"raise", "throw", NULL};
    if (has_any(bt, ERR_THROW)) {
        count = push_pattern_token(tokens, count, max_tokens, "error");
        count = push_pattern_token(tokens, count, max_tokens, "exception");
        count = push_pattern_token(tokens, count, max_tokens, "throw");
    }
    static const char *const LOGGING[] = {"logger", "logging", "log_", NULL};
    if (has_any(bt, LOGGING)) {
        count = push_pattern_token(tokens, count, max_tokens, "logging");
        count = push_pattern_token(tokens, count, max_tokens, "log");
    }
    return count;
}

/* Inject tokens for one CALLS-target name based on keyword groups. */
static int inject_callee_tokens(const char *name, char **tokens, int count, int max_tokens) {
    static const char *const LOG_FNS[] = {"log", "Log", "warn", "debug", "info", NULL};
    static const char *const ERR_FNS[] = {"Error", "error", "Errorf", "panic", NULL};
    static const char *const IO_FNS[] = {"open", "read", "write", "close", "Open", "Read", NULL};
    if (has_any(name, LOG_FNS)) {
        count = push_pattern_token(tokens, count, max_tokens, "logging");
        count = push_pattern_token(tokens, count, max_tokens, "log");
    }
    if (has_any(name, ERR_FNS)) {
        count = push_pattern_token(tokens, count, max_tokens, "error");
        count = push_pattern_token(tokens, count, max_tokens, "handling");
    }
    if (has_any(name, IO_FNS)) {
        count = push_pattern_token(tokens, count, max_tokens, "io");
        count = push_pattern_token(tokens, count, max_tokens, "file");
    }
    return count;
}

/* Walk the outbound CALLS edges of n and inject tokens for each target. */
static const char **collect_sorted_call_neighbors(const cbm_gbuf_t *gbuf, int64_t node_id,
                                                  bool outbound, int *out_n);

static int inject_calls_pattern_tokens(const cbm_gbuf_node_t *n, const cbm_gbuf_t *gbuf,
                                       char **tokens, int count, int max_tokens) {
    if (!gbuf) {
        return count;
    }
    /* SORTED, like every other consumer of CALLS neighbours in this file. The
     * raw edge array is in graph-buffer insertion order — parallel worker merge
     * order — and this loop both ORDERS the injected tokens and truncates at
     * max_tokens. Co-occurrence enrichment reads a window around each token
     * POSITION, so a different order yields different token vectors while
     * leaving document frequencies untouched: measured on the go corpus
     * 2026-09-19, two runs of one binary with identical idf but different token
     * sequences and different enriched vectors, which flickered near-threshold
     * SEMANTICALLY_RELATED edges. collect_sorted_call_neighbors() was written
     * for exactly this class and this call site was missed. */
    int nn = 0;
    const char **names = collect_sorted_call_neighbors(gbuf, n->id, /*outbound=*/true, &nn);
    if (!names) {
        return count;
    }
    for (int e = 0; e < nn && count < max_tokens; e++) {
        count = inject_callee_tokens(names[e], tokens, count, max_tokens);
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, (void *)names);
    return count;
}

/* Inject tokens from decorator annotations (@route, @middleware, @pytest.*). */
static int inject_decorator_tokens(const char *decs, char **tokens, int count, int max_tokens) {
    if (!decs) {
        return count;
    }
    static const char *const ROUTING[] = {"route", "Route", "app.", NULL};
    if (has_any(decs, ROUTING)) {
        count = push_pattern_token(tokens, count, max_tokens, "routing");
        count = push_pattern_token(tokens, count, max_tokens, "endpoint");
        count = push_pattern_token(tokens, count, max_tokens, "handler");
    }
    static const char *const MIDDLEWARE[] = {"middleware", "Middleware", NULL};
    if (has_any(decs, MIDDLEWARE)) {
        count = push_pattern_token(tokens, count, max_tokens, "middleware");
    }
    static const char *const TEST[] = {"test", "Test", "pytest", NULL};
    if (has_any(decs, TEST)) {
        count = push_pattern_token(tokens, count, max_tokens, "test");
        count = push_pattern_token(tokens, count, max_tokens, "testing");
    }
    return count;
}

/* Inject tokens keyed off the node's own name (test_*, *Handler, validator). */
static int inject_name_pattern_tokens(const char *name, char **tokens, int count, int max_tokens) {
    if (!name) {
        return count;
    }
    static const char *const TEST[] = {"test_", "Test", NULL};
    if (has_any(name, TEST)) {
        count = push_pattern_token(tokens, count, max_tokens, "test");
        count = push_pattern_token(tokens, count, max_tokens, "testing");
    }
    static const char *const MIDDLEWARE[] = {"middleware", "Middleware", NULL};
    if (has_any(name, MIDDLEWARE)) {
        count = push_pattern_token(tokens, count, max_tokens, "middleware");
    }
    static const char *const HANDLER[] = {"handler", "Handler", NULL};
    if (has_any(name, HANDLER)) {
        count = push_pattern_token(tokens, count, max_tokens, "handler");
    }
    static const char *const VALIDATOR[] = {"validator", "Validator", "validate", "Validate", NULL};
    if (has_any(name, VALIDATOR)) {
        count = push_pattern_token(tokens, count, max_tokens, "validation");
    }
    return count;
}

static int inject_pattern_tokens(const cbm_gbuf_node_t *n, const cbm_gbuf_t *gbuf, char **tokens,
                                 int count, int max_tokens) {
    if (!n || count >= max_tokens) {
        return count;
    }

    char bt_buf[CBM_SZ_512];
    const char *bt = n->properties_json
                         ? json_str_value(n->properties_json, "bt", bt_buf, sizeof(bt_buf))
                         : NULL;
    char dec_buf[CBM_SZ_256];
    const char *decs = n->properties_json ? json_str_value(n->properties_json, "decorators",
                                                           dec_buf, sizeof(dec_buf))
                                          : NULL;

    count = inject_body_pattern_tokens(bt, tokens, count, max_tokens);
    count = inject_calls_pattern_tokens(n, gbuf, tokens, count, max_tokens);
    count = inject_decorator_tokens(decs, tokens, count, max_tokens);
    count = inject_name_pattern_tokens(n->name, tokens, count, max_tokens);
    return count;
}

/* ── Technique 3: Field weights for token sources ────────────────── */
enum {
    FW_NAME = 30,      /* ×3.0 */
    FW_CALLEE = 20,    /* ×2.0 */
    FW_BODY = 15,      /* ×1.5 */
    FW_SIGNATURE = 10, /* ×1.0 */
    FW_PARAM = 10,     /* ×1.0 */
    FW_PATTERN = 25,   /* ×2.5 — injected semantic tokens are high value */
    FW_PATH = 5,       /* ×0.5 */
    FW_SCALE = 10,     /* divisor */
};

static const char *itoa_log(int val) {
    enum { RING = 4, MASK = 3 };
    static CBM_TLS char bufs[RING][CBM_SZ_32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + SKIP_ONE) & MASK;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

static const char *file_ext(const char *path) {
    if (!path) {
        return "";
    }
    const char *dot = strrchr(path, '.');
    return dot ? dot : "";
}

/* Extract a JSON string value by key, escapes honoured (cbm_sem_json_str). */
static const char *json_str_value(const char *json, const char *key, char *buf, int bufsize) {
    return cbm_sem_json_str(json, key, buf, bufsize);
}

/* Extract a JSON array of strings by key. Returns count. */
static int json_str_array(const char *json, const char *key, char **out, int max_out) {
    if (!json || !key) {
        return 0;
    }
    char search[CBM_SZ_64];
    snprintf(search, sizeof(search), "\"%s\":[", key);
    const char *start = strstr(json, search);
    if (!start) {
        return 0;
    }
    start += strlen(search);
    int count = 0;
    while (*start && *start != ']' && count < max_out) {
        if (*start == '"') {
            start++;
            const char *end = strchr(start, '"');
            if (!end) {
                break;
            }
            int len = (int)(end - start);
            out[count] = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)len + SKIP_ONE);
            memcpy(out[count], start, (size_t)len);
            out[count][len] = '\0';
            count++;
            start = end + SKIP_ONE;
        } else {
            start++;
        }
    }
    return count;
}

/* ── Tokenize node metadata ──────────────────────────────────────── */

/* Tokenize a single string field keyed out of node->properties_json, if
 * present.  Returns the new count after appending any tokens. */
static int tokenize_json_string_field(const char *json, const char *key, char **tokens, int count,
                                      int max_tokens) {
    if (count >= max_tokens) {
        return count;
    }
    char buf[CBM_SZ_512];
    if (!json_str_value(json, key, buf, sizeof(buf))) {
        return count;
    }
    count += cbm_sem_tokenize(buf, tokens + count, max_tokens - count);
    return count;
}

/* Tokenize a JSON array field (e.g. "param_names", "decorators"). */
static int tokenize_json_array_field(const char *json, const char *key, char **tokens, int count,
                                     int max_tokens) {
    if (count >= max_tokens) {
        return count;
    }
    char *arr[CBM_SZ_16];
    int n = json_str_array(json, key, arr, CBM_SZ_16);
    for (int p = 0; p < n; p++) {
        if (count < max_tokens) {
            count += cbm_sem_tokenize(arr[p], tokens + count, max_tokens - count);
        }
        cbm_free(CBM_MEM_CLASS_SEMANTIC, arr[p]);
    }
    return count;
}

/* Walk the CALLS edges rooted at n (either outbound or inbound depending on
 * `outbound`) and tokenize the names of the target/source nodes.  Caller-side
 * caps via max_tokens and MAX_CALLEES. */
static int cmp_name_ptr(const void *pa, const void *pb) {
    const char *a = *(const char *const *)pa;
    const char *b = *(const char *const *)pb;
    return strcmp(a, b);
}

/* Collect the CALLS-neighbor names of `node_id`, SORTED by name. The edge
 * arrays are in insertion order — which under parallel extraction varies run
 * to run — and both consumers truncate (MAX_CALLEES / max_tokens), so an
 * unstable order changed WHICH neighbors contribute to the semantic vectors
 * and flickered near-threshold SEMANTICALLY_RELATED edges (determinism).
 * Returns a malloc'd array of borrowed name pointers; caller frees the array. */
static const char **collect_sorted_call_neighbors(const cbm_gbuf_t *gbuf, int64_t node_id,
                                                  bool outbound, int *out_n) {
    *out_n = 0;
    const cbm_gbuf_edge_t **edges = NULL;
    int ec = 0;
    int rc = outbound ? cbm_gbuf_find_edges_by_source_type(gbuf, node_id, "CALLS", &edges, &ec)
                      : cbm_gbuf_find_edges_by_target_type(gbuf, node_id, "CALLS", &edges, &ec);
    if (rc != 0 || ec <= 0) {
        return NULL;
    }
    const char **names = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)ec * sizeof(char *));
    if (!names) {
        return NULL;
    }
    int n = 0;
    for (int e = 0; e < ec; e++) {
        int64_t id = outbound ? edges[e]->target_id : edges[e]->source_id;
        const cbm_gbuf_node_t *neighbor = cbm_gbuf_find_by_id(gbuf, id);
        if (neighbor && neighbor->name) {
            names[n++] = neighbor->name;
        }
    }
    qsort(names, (size_t)n, sizeof(char *), cmp_name_ptr);
    *out_n = n;
    return names;
}

static int tokenize_call_neighbors(const cbm_gbuf_node_t *n, const cbm_gbuf_t *gbuf, bool outbound,
                                   char **tokens, int count, int max_tokens) {
    if (!gbuf || count >= max_tokens) {
        return count;
    }
    int nn = 0;
    const char **names = collect_sorted_call_neighbors(gbuf, n->id, outbound, &nn);
    if (!names) {
        return count;
    }
    for (int e = 0; e < nn && e < MAX_CALLEES && count < max_tokens; e++) {
        count += cbm_sem_tokenize(names[e], tokens + count, max_tokens - count);
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, (void *)names);
    return count;
}

static int tokenize_node(const cbm_gbuf_node_t *n, const cbm_gbuf_t *gbuf, char **tokens,
                         int max_tokens) {
    int count = 0;
    count += cbm_sem_tokenize(n->name, tokens + count, max_tokens - count);
    if (n->qualified_name && count < max_tokens) {
        count += cbm_sem_tokenize(n->qualified_name, tokens + count, max_tokens - count);
    }
    if (n->file_path && count < max_tokens) {
        count += cbm_sem_tokenize(n->file_path, tokens + count, max_tokens - count);
    }
    if (n->properties_json) {
        count =
            tokenize_json_string_field(n->properties_json, "signature", tokens, count, max_tokens);
        count = tokenize_json_string_field(n->properties_json, "return_type", tokens, count,
                                           max_tokens);
        count =
            tokenize_json_string_field(n->properties_json, "docstring", tokens, count, max_tokens);
        count =
            tokenize_json_array_field(n->properties_json, "param_names", tokens, count, max_tokens);
        count =
            tokenize_json_array_field(n->properties_json, "param_types", tokens, count, max_tokens);
        count =
            tokenize_json_array_field(n->properties_json, "decorators", tokens, count, max_tokens);
        count = tokenize_json_string_field(n->properties_json, "bt", tokens, count, max_tokens);
    }
    count = tokenize_call_neighbors(n, gbuf, /*outbound=*/true, tokens, count, max_tokens);

    /* Caller names: what CALLS this function (contextual vocabulary).
     * Functions called by error handlers inherit "error" context. */
    count = tokenize_call_neighbors(n, gbuf, /*outbound=*/false, tokens, count, max_tokens);
    return count;
}

/* ── Build per-function semantic data ────────────────────────────── */

static void build_api_vec(const cbm_gbuf_t *gbuf, int64_t node_id, cbm_sem_vec_t *out) {
    memset(out, 0, sizeof(*out));
    /* Sorted neighbors: stable MAX_CALLEES subset + stable float-accumulation
     * order (see collect_sorted_call_neighbors). */
    int n = 0;
    const char **names = collect_sorted_call_neighbors(gbuf, node_id, /*outbound=*/true, &n);
    if (!names) {
        return;
    }
    for (int i = 0; i < n && i < MAX_CALLEES; i++) {
        cbm_sem_vec_t callee_ri;
        cbm_sem_random_index(names[i], &callee_ri);
        cbm_sem_vec_add_scaled(out, &callee_ri, PSE_UNIT_POS);
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, (void *)names);
    cbm_sem_normalize(out);
}

static void build_type_vec(const char *props_json, cbm_sem_vec_t *out) {
    memset(out, 0, sizeof(*out));
    if (!props_json) {
        return;
    }
    /* Extract param_types and return_type */
    char rt_buf[CBM_SZ_128];
    if (json_str_value(props_json, "return_type", rt_buf, sizeof(rt_buf))) {
        cbm_sem_vec_t ri;
        cbm_sem_random_index(rt_buf, &ri);
        cbm_sem_vec_add_scaled(out, &ri, PSE_UNIT_POS);
    }
    char *ptypes[CBM_SZ_16];
    int pt_count = json_str_array(props_json, "param_types", ptypes, CBM_SZ_16);
    for (int i = 0; i < pt_count; i++) {
        cbm_sem_vec_t ri;
        cbm_sem_random_index(ptypes[i], &ri);
        cbm_sem_vec_add_scaled(out, &ri, PSE_UNIT_POS);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, ptypes[i]);
    }
    cbm_sem_normalize(out);
}

static void build_deco_vec(const char *props_json, cbm_sem_vec_t *out) {
    memset(out, 0, sizeof(*out));
    if (!props_json) {
        return;
    }
    char *decos[CBM_SZ_16];
    int dc = json_str_array(props_json, "decorators", decos, CBM_SZ_16);
    for (int i = 0; i < dc; i++) {
        cbm_sem_vec_t ri;
        cbm_sem_random_index(decos[i], &ri);
        cbm_sem_vec_add_scaled(out, &ri, PSE_UNIT_POS);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, decos[i]);
    }
    cbm_sem_normalize(out);
}

static void decode_struct_profile(const char *props_json, float *out) {
    memset(out, 0, sizeof(float) * CBM_AST_PROFILE_DIMS);
    if (!props_json) {
        return;
    }
    char sp_buf[CBM_AST_PROFILE_BUF];
    if (json_str_value(props_json, "sp", sp_buf, sizeof(sp_buf))) {
        cbm_ast_profile_t profile;
        if (cbm_ast_profile_from_str(sp_buf, &profile)) {
            cbm_ast_profile_to_vector(&profile, out);
        }
    }
}

static void decode_minhash(const char *props_json, cbm_sem_func_t *func) {
    func->has_minhash = false;
    if (!props_json) {
        return;
    }
    const char *fp_key = strstr(props_json, "\"fp\":\"");
    if (!fp_key) {
        return;
    }
    const char *hex = fp_key + PSE_FP_PREFIX_LEN; /* strlen("\"fp\":\"") */
    const char *end = strchr(hex, '"');
    if (!end || (int)(end - hex) != CBM_MINHASH_HEX_LEN) {
        return;
    }
    char hex_buf[CBM_MINHASH_HEX_BUF];
    memcpy(hex_buf, hex, CBM_MINHASH_HEX_LEN);
    hex_buf[CBM_MINHASH_HEX_LEN] = '\0';
    cbm_minhash_t mh;
    if (cbm_minhash_from_hex(hex_buf, &mh)) {
        memcpy(func->minhash, mh.values, sizeof(func->minhash));
        func->has_minhash = true;
    }
}

/* ── Parallel Phase 2: Tokenize nodes ────────────────────────────── */

/* One growable token-pointer buffer per worker. A function's tokens are
 * appended contiguously; where they landed is recorded per function so a
 * single pass after the parallel phase can pack everything into one array
 * with offsets. This replaces a fixed CBM_SEM_MAX_TOKENS-slot stride per
 * function (4 KB each, 7.4 GB on the kernel). */
typedef struct {
    char **items;
    size_t count;
    size_t cap;
} tok_buf_t;

static bool tok_buf_append(tok_buf_t *b, char **tokens, int count) {
    if (b->count + (size_t)count > b->cap) {
        size_t new_cap = b->cap ? b->cap * 2 : (size_t)CBM_SZ_4K;
        while (new_cap < b->count + (size_t)count) {
            new_cap *= 2;
        }
        char **grown = cbm_realloc(CBM_MEM_CLASS_SEMANTIC, b->items, new_cap * sizeof(char *));
        if (!grown) {
            return false;
        }
        b->items = grown;
        b->cap = new_cap;
    }
    memcpy(b->items + b->count, tokens, (size_t)count * sizeof(char *));
    b->count += (size_t)count;
    return true;
}

typedef struct {
    const cbm_gbuf_node_t **node_ptrs; /* node pointer per function index */
    cbm_gbuf_t *gbuf;                  /* read-only during tokenization */
    tok_buf_t *bufs;                   /* per worker: appended token pointers */
    int *tok_worker;                   /* per function: which worker's buffer */
    size_t *tok_off;                   /* per function: offset inside that buffer */
    int *token_counts;                 /* output: token count per function */
    int func_count;
    _Atomic int next_idx;
    /* Per-worker token intern pools (key==value==the one owned strdup):
     * identical tokens ("xfs", "error", ...) recur across hundreds of
     * thousands of functions; per-func strdups made all_tokens hold every
     * instance. Interned, it holds at most workers x unique tokens. */
    CBMHashTable **pools;
} tokenize_ctx_t;

static void tokenize_worker(int worker_id, void *ctx_ptr) {
    tokenize_ctx_t *tc = ctx_ptr;
    while (true) {
        int f = atomic_fetch_add_explicit(&tc->next_idx, SKIP_ONE, memory_order_relaxed);
        if (f >= tc->func_count) {
            break;
        }

        const cbm_gbuf_node_t *n = tc->node_ptrs[f];
        /* Tokenize into a per-call scratch slice, intern, then append the
         * surviving pointers to this worker's buffer. The strings are owned by
         * the worker's intern pool (or by the buffer until interned). */
        char *dst[CBM_SEM_MAX_TOKENS];
        int count = tokenize_node(n, tc->gbuf, dst, CBM_SEM_MAX_TOKENS);
        count = inject_pattern_tokens(n, tc->gbuf, dst, count, CBM_SEM_MAX_TOKENS);
        if (tc->pools && tc->pools[worker_id]) {
            CBMHashTable *pool = tc->pools[worker_id];
            for (int t = 0; t < count; t++) {
                char *canon = cbm_ht_get(pool, dst[t]);
                if (canon) {
                    cbm_free(CBM_MEM_CLASS_SEMANTIC, dst[t]);
                    dst[t] = canon;
                } else {
                    cbm_ht_set(pool, dst[t], dst[t]); /* key borrows the value */
                }
            }
        }
        tok_buf_t *b = &tc->bufs[worker_id];
        tc->tok_worker[f] = worker_id;
        tc->tok_off[f] = b->count;
        if (!tok_buf_append(b, dst, count)) {
            count = 0; /* OOM: the function contributes no tokens, never garbage */
        }
        tc->token_counts[f] = count;
    }
}

/* ── Parallel Phase 4: Build per-function vectors ────────────────── */

typedef struct {
    cbm_sem_func_t *funcs;
    char **all_tokens;
    const size_t *offsets; /* function f = all_tokens[offsets[f] ..] */
    int *token_counts;
    cbm_sem_corpus_t *corpus;
    uint8_t *qvecs; /* output: pre-quantized int8 vectors [func_count * CBM_SEM_DIM] */
    int func_count;
    _Atomic int next_idx;
} vec_build_ctx_t;

static void vec_build_worker(int worker_id, void *ctx_ptr) {
    (void)worker_id;
    vec_build_ctx_t *vc = ctx_ptr;
    while (true) {
        int f = atomic_fetch_add_explicit(&vc->next_idx, SKIP_ONE, memory_order_relaxed);
        if (f >= vc->func_count) {
            break;
        }

        int tc = vc->token_counts[f];
        char **tokens = &vc->all_tokens[vc->offsets[f]];

        /* Each token is resolved to its corpus index ONCE: both loops below ask
         * about every token, and asking by name cost a hash lookup plus a
         * strtol per question -- three per token (35 M repeated lookups of an
         * unchanged table on the Go corpus, waste sanitizer 2026-09-17). */
        int *token_index = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)tc * sizeof(int));
        for (int t = 0; token_index && t < tc; t++) {
            token_index[t] = cbm_sem_corpus_token_index(vc->corpus, tokens[t]);
        }

        /* TF-IDF terms, keyed by corpus token (cbm_sem_tfidf_terms). */
        int *indices = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)tc * sizeof(int));
        float *weights = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)tc * sizeof(float));
        int tfidf_len = cbm_sem_tfidf_terms(vc->corpus, tokens, token_index, tc, indices, weights);
        vc->funcs[f].tfidf_indices = indices;
        vc->funcs[f].tfidf_weights = weights;
        vc->funcs[f].tfidf_len = tfidf_len;

        /* RI vector: sum of enriched token vectors weighted by IDF. Built in a
         * LOCAL dense buffer; only the quantized code is retained per func
         * (the resident dense floats were ~9.4 GB on the kernel). */
        cbm_sem_vec_t ri_dense;
        memset(&ri_dense, 0, sizeof(cbm_sem_vec_t));
        for (int t = 0; t < tc; t++) {
            const cbm_sem_vec_t *ri = token_index
                                          ? cbm_sem_corpus_ri_vec_at(vc->corpus, token_index[t])
                                          : cbm_sem_corpus_ri_vec(vc->corpus, tokens[t]);
            if (ri) {
                float idf = token_index ? cbm_sem_corpus_idf_at(vc->corpus, token_index[t])
                                        : cbm_sem_corpus_idf(vc->corpus, tokens[t]);
                cbm_sem_vec_add_scaled(&ri_dense, ri, idf);
            }
        }
        cbm_free(CBM_MEM_CLASS_SEMANTIC, token_index);
        cbm_sem_normalize(&ri_dense);
        cbm_rsq_encode(ri_dense.v, &vc->funcs[f].ri_code);

        /* Int8 quantize into pre-allocated output array (parallel-safe) */
        uint8_t *qv = &vc->qvecs[(ptrdiff_t)f * CBM_SEM_DIM];
        for (int d = 0; d < CBM_SEM_DIM; d++) {
            float v = ri_dense.v[d];
            if (v > PSE_UNIT_POS) {
                v = PSE_UNIT_POS;
            }
            if (v < -PSE_UNIT_POS) {
                v = -PSE_UNIT_POS;
            }
            qv[d] = (uint8_t)(int8_t)(v * PSE_INT8_MAX);
        }
    }
}

/* ── Parallel Phase 5: LSH signatures ────────────────────────────── */

enum {
    NUM_HYPERPLANES = 64,
    SEM_LSH_BANDS = 16,
    SEM_LSH_ROWS = 4,
    SEM_BUCKET_COUNT = 65536,
    SEM_BUCKET_MASK = 65535,
    SEM_BUCKET_CAP_INIT = 16,
    SEM_MAX_CANDIDATES = 200,
};

/* Row of a hyperplane matrix in the ROTATED (quantized) basis: LSH only
 * needs signs of dots against random directions, and a random direction in
 * the rotated basis is as random as one in the original — so signatures are
 * computed from dequantized codes without keeping dense originals. */
typedef float hyperplane_row_t[CBM_RSQ_DIM];

typedef struct {
    cbm_sem_func_t *funcs;
    uint64_t *signatures;
    hyperplane_row_t *hyperplanes;
    int func_count;
    _Atomic int next_idx;
} sig_build_ctx_t;

static void sig_build_worker(int worker_id, void *ctx_ptr) {
    (void)worker_id;
    sig_build_ctx_t *sc = ctx_ptr;
    while (true) {
        int f = atomic_fetch_add_explicit(&sc->next_idx, SKIP_ONE, memory_order_relaxed);
        if (f >= sc->func_count) {
            break;
        }

        float dec[CBM_RSQ_DIM];
        cbm_rsq_decode(&sc->funcs[f].ri_code, dec);
        uint64_t sig = 0;
        for (int h = 0; h < NUM_HYPERPLANES; h++) {
            float dot = 0.0F;
            for (int d = 0; d < CBM_RSQ_DIM; d++) {
                dot += dec[d] * sc->hyperplanes[h][d];
            }
            if (dot > 0.0F) {
                sig |= (PSE_ONE_ULL << h);
            }
        }
        sc->signatures[f] = sig;
    }
}

/* ── Parallel Phase 6: Score candidates + collect edges ──────────── */

typedef struct {
    cbm_sem_func_t *funcs;
    uint64_t *signatures;
    int *edge_counts; /* budget applied sequentially in phase6b (determinism) */
    cbm_sem_config_t cfg;
    float record_floor; /* kept from here up; under the threshold: signals dump only */
    int func_count;

    /* LSH buckets (read-only during scoring) */
    struct {
        int *items;
        int count;
        int cap;
    } **band_buckets;

    /* Per-worker edge buffer */
    deferred_edge_buf_t *worker_bufs;
    int max_workers;
    _Atomic int next_idx;
} score_ctx_t;

enum {
    SCORE_SEEN_CAP = 8192,
    SCORE_SEEN_MASK = 8191,
    SCORE_SEEN_EMPTY = -1,
};

/* The open-addressed `seen` set of one worker, reused for every function it
 * scores: a slot belongs to the current function only when its stamp says so,
 * so starting a function costs nothing where re-filling 8,192 slots cost a
 * 32 KB write per function (90 k on the Go corpus, waste sanitizer
 * 2026-09-17). Same size and probe order: the same candidates, in order. */
typedef struct {
    int ids[SCORE_SEEN_CAP];
    int stamp[SCORE_SEEN_CAP];
    int current; /* function index + 1: unique per function, never 0 */
} score_seen_t;

/* Check whether `j` has already been recorded in the `seen` set for this
 * function; insert it if not.  Returns true when the insertion was fresh
 * (caller should add to candidates). */
static bool score_seen_insert(score_seen_t *seen, int j) {
    uint32_t slot = (uint32_t)j & SCORE_SEEN_MASK;
    for (int p = 0; p < SCORE_SEEN_CAP; p++) {
        uint32_t idx = (slot + (uint32_t)p) & SCORE_SEEN_MASK;
        if (seen->stamp[idx] != seen->current) {
            seen->stamp[idx] = seen->current;
            seen->ids[idx] = j;
            return true;
        }
        if (seen->ids[idx] == j) {
            return false;
        }
    }
    return false;
}

/* Collect the unique candidate function indices for node `i` by iterating
 * every LSH band and merging bucket members via `seen[]`.  Returns the
 * populated candidate count. */
static int score_collect_candidates(score_ctx_t *sc, int i, score_seen_t *seen, int *candidates,
                                    int cand_cap) {
    int cand_count = 0;
    for (int b = 0; b < SEM_LSH_BANDS && cand_count < cand_cap; b++) {
        int shift = b * SEM_LSH_ROWS;
        uint32_t band_val = (uint32_t)((sc->signatures[i] >> shift) &
                                       ((PSE_ONE_ULL << SEM_LSH_ROWS) - PSE_ONE_ULL));
        uint64_t bh = XXH3_64bits_withSeed(&band_val, sizeof(band_val), (uint64_t)b);
        uint32_t bucket_idx = (uint32_t)(bh & SEM_BUCKET_MASK);
        int bcount = sc->band_buckets[b][bucket_idx].count;
        int *bitems = sc->band_buckets[b][bucket_idx].items;
        if (bcount > SEM_MAX_CANDIDATES) {
            continue;
        }
        for (int k = 0; k < bcount && cand_count < cand_cap; k++) {
            int j = bitems[k];
            if (j <= i) {
                continue;
            }
            if (score_seen_insert(seen, j)) {
                candidates[cand_count++] = j;
            }
        }
    }
    return cand_count;
}

/* Score one candidate pair (i, j) and push a deferred candidate edge if the
 * score passes the threshold. ADMISSION (the per-node max_edges budget) is
 * deliberately NOT decided here: the old check-then-increment on shared
 * atomic counts made the admitted edge SET depend on worker scheduling —
 * multi-threaded runs lost edges vs single-threaded and differed run-to-run
 * (repro_parallel_edge_determinism). Scoring is pure math and stays parallel;
 * the budget is applied afterwards in one sequential pass over the pairs in
 * canonical (i, candidate-rank) order. */
static void score_try_emit(score_ctx_t *sc, int i, int j, int c, deferred_edge_buf_t *my_buf) {
    if (strcmp(sc->funcs[i].file_ext, sc->funcs[j].file_ext) != 0) {
        return;
    }
    float score = cbm_sem_combined_score(&sc->funcs[i], &sc->funcs[j], &sc->cfg);
    if (score < sc->record_floor) {
        return;
    }
    bool same_file = sc->funcs[i].file_path && sc->funcs[j].file_path &&
                     strcmp(sc->funcs[i].file_path, sc->funcs[j].file_path) == 0;
    deferred_buf_push(my_buf, sc->funcs[i].node_id, sc->funcs[j].node_id, score, same_file,
                      score < sc->cfg.threshold, i, j, c);
}

static void score_worker(int worker_id, void *ctx_ptr) {
    score_ctx_t *sc = ctx_ptr;
    deferred_edge_buf_t *my_buf = &sc->worker_bufs[worker_id];
    score_seen_t *seen = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, sizeof(score_seen_t));
    if (!seen) {
        return; /* the other workers claim the functions this one leaves */
    }

    while (true) {
        int i = atomic_fetch_add_explicit(&sc->next_idx, SKIP_ONE, memory_order_relaxed);
        if (i >= sc->func_count) {
            break;
        }
        seen->current = i + SKIP_ONE;
        int candidates[SEM_MAX_CANDIDATES];
        int cand_count = score_collect_candidates(sc, i, seen, candidates, SEM_MAX_CANDIDATES);
        for (int c = 0; c < cand_count; c++) {
            score_try_emit(sc, i, candidates[c], c, my_buf);
        }
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, seen);
}

/* ── Parallel Phase 1b: decode minhash/profile + build per-func vectors ── */

typedef struct {
    cbm_sem_func_t *funcs;
    const cbm_gbuf_node_t **node_ptrs;
    const cbm_gbuf_t *gbuf;
    int func_count;
    _Atomic int next_idx;
} collect_ctx_t;

static void collect_worker(int worker_id, void *ctx_ptr) {
    (void)worker_id;
    collect_ctx_t *cc = ctx_ptr;
    while (true) {
        int f = atomic_fetch_add_explicit(&cc->next_idx, PSE_MOD_64, memory_order_relaxed);
        if (f >= cc->func_count) {
            break;
        }
        int end = f + PSE_MOD_64;
        if (end > cc->func_count) {
            end = cc->func_count;
        }
        for (int i = f; i < end; i++) {
            const cbm_gbuf_node_t *n = cc->node_ptrs[i];
            decode_minhash(n->properties_json, &cc->funcs[i]);
            decode_struct_profile(n->properties_json, cc->funcs[i].struct_profile);
            cbm_sem_vec_t tmp_vec;
            build_api_vec(cc->gbuf, n->id, &tmp_vec);
            cbm_rsq_encode(tmp_vec.v, &cc->funcs[i].api_code);
            build_type_vec(n->properties_json, &tmp_vec);
            cbm_rsq_encode(tmp_vec.v, &cc->funcs[i].type_code);
            build_deco_vec(n->properties_json, &tmp_vec);
            cbm_rsq_encode(tmp_vec.v, &cc->funcs[i].deco_code);
        }
    }
}

/* ── Phase helpers (keep cbm_pipeline_pass_semantic_edges complexity low) ── */

typedef struct {
    int *items;
    int count;
    int cap;
} sem_bucket_t;

/* Canonical node order: by qualified name (unique per node), id tie-break
 * for defensiveness. Gives the semantic pass a stable, content-derived input
 * order regardless of how parallel extraction merged the graph buffer. */
static int cmp_node_ptr_by_qn(const void *pa, const void *pb) {
    const cbm_gbuf_node_t *a = *(const cbm_gbuf_node_t *const *)pa;
    const cbm_gbuf_node_t *b = *(const cbm_gbuf_node_t *const *)pb;
    const char *qa = a->qualified_name ? a->qualified_name : "";
    const char *qb = b->qualified_name ? b->qualified_name : "";
    int r = strcmp(qa, qb);
    if (r != 0) {
        return r;
    }
    if (a->id != b->id) {
        return a->id < b->id ? -1 : 1;
    }
    return 0;
}

/* Phase 1a: seed the funcs[] / node_ptrs[] arrays from all Function and
 * Method nodes in the graph buffer.  Returns the number of functions collected
 * (0 on OOM), and fills *out_funcs / *out_nodes with newly malloc'd arrays. */
static int phase1_scan_functions(cbm_gbuf_t *gbuf, cbm_sem_func_t **out_funcs,
                                 const cbm_gbuf_node_t ***out_nodes) {
    *out_funcs = NULL;
    *out_nodes = NULL;
    cbm_sem_func_t *funcs = NULL;
    const cbm_gbuf_node_t **node_ptrs = NULL;
    int func_count = 0;
    int func_cap = 0;
    const char *labels[] = {"Function", "Method", NULL};
    for (int li = 0; labels[li]; li++) {
        const cbm_gbuf_node_t **nodes = NULL;
        int node_count = 0;
        if (cbm_gbuf_find_by_label(gbuf, labels[li], &nodes, &node_count) != 0) {
            continue;
        }
        for (int i = 0; i < node_count; i++) {
            if (func_count >= func_cap) {
                int new_cap = func_cap < MAX_FUNCS_INIT ? MAX_FUNCS_INIT : func_cap * GROW;
                cbm_sem_func_t *grown = cbm_realloc(CBM_MEM_CLASS_SEMANTIC, funcs,
                                                    (size_t)new_cap * sizeof(cbm_sem_func_t));
                if (!grown) {
                    break;
                }
                funcs = grown;
                const cbm_gbuf_node_t **np_grown = cbm_realloc(
                    CBM_MEM_CLASS_SEMANTIC, node_ptrs, (size_t)new_cap * sizeof(cbm_gbuf_node_t *));
                if (!np_grown) {
                    break;
                }
                node_ptrs = np_grown;
                func_cap = new_cap;
            }
            memset(&funcs[func_count], 0, sizeof(cbm_sem_func_t));
            funcs[func_count].node_id = nodes[i]->id;
            funcs[func_count].file_path = nodes[i]->file_path;
            funcs[func_count].file_ext = file_ext(nodes[i]->file_path);
            node_ptrs[func_count] = nodes[i];
            func_count++;
        }
    }
    /* Canonicalize the func order (determinism): the label-index order above
     * is gbuf insertion order = parallel-extraction merge order, which varies
     * run to run. Everything downstream is order-sensitive — LSH bucket chain
     * order, the SEM_MAX_CANDIDATES truncation, seen[] and the admission
     * sequence — so an unstable order changes WHICH semantic edges are
     * emitted. Sort the cheap pointer array by qualified name (unique) and
     * re-derive the three fields set so far; the heavy per-func payloads are
     * filled in later phases, so no 12.7 KB structs are moved.
     * A graph with no Function/Method nodes never allocates node_ptrs, and
     * qsort's base is declared nonnull (glibc), so skip the sort outright. */
    if (func_count > 0) {
        qsort(node_ptrs, (size_t)func_count, sizeof(node_ptrs[0]), cmp_node_ptr_by_qn);
    }
    for (int k = 0; k < func_count; k++) {
        funcs[k].node_id = node_ptrs[k]->id;
        funcs[k].file_path = node_ptrs[k]->file_path;
        funcs[k].file_ext = file_ext(node_ptrs[k]->file_path);
    }
    *out_funcs = funcs;
    *out_nodes = node_ptrs;
    return func_count;
}

/* Phase 5c: partition functions into LSH buckets by their signature bands.
 * Sequential because each bucket grows its `items` array via realloc. */
static void phase5c_build_lsh_buckets(const uint64_t *signatures, int func_count,
                                      sem_bucket_t **band_buckets) {
    for (int f = 0; f < func_count; f++) {
        for (int b = 0; b < SEM_LSH_BANDS; b++) {
            int shift = b * SEM_LSH_ROWS;
            uint32_t band_val = (uint32_t)((signatures[f] >> shift) &
                                           ((PSE_ONE_ULL << SEM_LSH_ROWS) - PSE_ONE_ULL));
            uint64_t bh = XXH3_64bits_withSeed(&band_val, sizeof(band_val), (uint64_t)b);
            uint32_t bucket_idx = (uint32_t)(bh & SEM_BUCKET_MASK);
            sem_bucket_t *bucket = &band_buckets[b][bucket_idx];
            if (bucket->count >= bucket->cap) {
                int nc =
                    bucket->cap < SEM_BUCKET_CAP_INIT ? SEM_BUCKET_CAP_INIT : bucket->cap * GROW;
                int *ni =
                    cbm_realloc(CBM_MEM_CLASS_SEMANTIC, bucket->items, (size_t)nc * sizeof(int));
                if (!ni) {
                    continue;
                }
                bucket->items = ni;
                bucket->cap = nc;
            }
            bucket->items[bucket->count++] = f;
        }
    }
}

/* Phase 6b: serialize deferred edges from all worker buffers into the graph
 * buffer (sequential because gbuf isn't thread-safe). */
/* Canonical order for candidate pairs: ascending discovering-func index,
 * then ascending candidate rank — the order a sequential scoring loop over
 * canonically-sorted funcs would have produced. */
static int cmp_deferred_edge_canonical(const void *pa, const void *pb) {
    const deferred_edge_t *a = pa;
    const deferred_edge_t *b = pb;
    if (a->i != b->i) {
        return a->i < b->i ? -1 : 1;
    }
    if (a->c != b->c) {
        return a->c < b->c ? -1 : 1;
    }
    return 0;
}

/* Sequential, deterministic admission + merge: gather every above-threshold
 * pair from the worker buffers, replay them in canonical (i, rank) order,
 * and apply the per-node max_edges budget HERE — single-threaded — so the
 * admitted edge set is a pure function of the (canonically sorted) inputs,
 * independent of worker count and scheduling. */
/* CBM_SEM_PAIR_SIGNALS=<path>: one line per recorded pair (from the record
 * floor up, CBM_SEM_PAIR_SIGNALS_FLOOR, default the threshold), in canonical
 * order: both qualified names, the score, every signal value and whether the
 * pair was admitted. The measuring harness for the semantic engine: judged
 * samples are drawn from it, stratified by score. Off unless set. */
static void pair_signals_line(FILE *f, const cbm_gbuf_t *gbuf, const cbm_sem_func_t *funcs,
                              const deferred_edge_t *de, bool admitted) {
    cbm_sem_signals_t s;
    cbm_sem_signal_values(&funcs[de->i], &funcs[de->j], &s);
    const cbm_gbuf_node_t *a = cbm_gbuf_find_by_id(gbuf, de->source_id);
    const cbm_gbuf_node_t *b = cbm_gbuf_find_by_id(gbuf, de->target_id);
    (void)fprintf(
        f,
        "%s\t%s\t%.6f\t%.6f\t%.6f\t%.6f\t%.6f\t%.6f\t%.6f\t%.6f\t%.4f\t%d\t%d"
        "\t%s:%d-%d\t%s:%d-%d\n",
        a && a->qualified_name ? a->qualified_name : "?",
        b && b->qualified_name ? b->qualified_name : "?", (double)de->score, (double)s.tfidf,
        (double)s.ri, (double)s.minhash, (double)s.api, (double)s.type, (double)s.decorator,
        (double)s.struct_profile, (double)s.proximity, de->same_file ? 1 : 0, admitted ? 1 : 0,
        a && a->file_path ? a->file_path : "?", a ? a->start_line : 0, a ? a->end_line : 0,
        b && b->file_path ? b->file_path : "?", b ? b->start_line : 0, b ? b->end_line : 0);
}

static int phase6b_merge_edges(cbm_gbuf_t *gbuf, deferred_edge_buf_t *worker_bufs, int worker_count,
                               int *edge_counts, int max_edges, const cbm_sem_func_t *funcs) {
    int total_pairs = 0;
    for (int w = 0; w < worker_count; w++) {
        total_pairs += worker_bufs[w].count;
    }
    deferred_edge_t *pairs = NULL;
    if (total_pairs > 0) {
        pairs = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)total_pairs * sizeof(deferred_edge_t));
    }
    if (!pairs) {
        for (int w = 0; w < worker_count; w++) {
            deferred_buf_free(&worker_bufs[w]);
        }
        return 0;
    }
    int n = 0;
    for (int w = 0; w < worker_count; w++) {
        /* A worker that emitted no pair never allocated its buffer, and
         * memcpy's source is declared nonnull (glibc) even for zero bytes. */
        if (worker_bufs[w].count > 0) {
            memcpy(&pairs[n], worker_bufs[w].edges,
                   (size_t)worker_bufs[w].count * sizeof(deferred_edge_t));
            n += worker_bufs[w].count;
        }
        deferred_buf_free(&worker_bufs[w]);
    }
    qsort(pairs, (size_t)n, sizeof(deferred_edge_t), cmp_deferred_edge_canonical);

    /* CBM_SEM_PAIR_DUMP=<path>: every candidate that passed the threshold, with
     * its score, in canonical order. Written HERE because this loop is
     * single-threaded and already sorted, so the dump needs no lock and two
     * runs compare line by line. The (i, j) indices are stable across runs —
     * funcs[] is sorted by qualified name — where node ids are not. Exists to
     * chase the near-threshold SEMANTICALLY_RELATED flicker: find the pair that
     * flips, then compare its score between runs. */
    FILE *pair_dump = NULL;
    FILE *signals = NULL;
    {
        char dump_path[CBM_SZ_1K];
        if (cbm_safe_getenv("CBM_SEM_PAIR_DUMP", dump_path, sizeof(dump_path), NULL)) {
            pair_dump = cbm_fopen(dump_path, "w");
        }
        if (funcs && cbm_safe_getenv("CBM_SEM_PAIR_SIGNALS", dump_path, sizeof(dump_path), NULL)) {
            signals = cbm_fopen(dump_path, "w");
        }
    }

    /* Admission: best-first by score under the per-function budget
     * (cbm_sem_admit_best_first); the dumps and the edge inserts below keep
     * the canonical order. */
    float *scores = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)n * sizeof(float));
    int *fa = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)n * sizeof(int));
    int *fb = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)n * sizeof(int));
    bool *eligible = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)n * sizeof(bool));
    bool *admitted = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)n * sizeof(bool));
    bool ranked = scores && fa && fb && eligible && admitted;
    for (int e = 0; ranked && e < n; e++) {
        scores[e] = pairs[e].score;
        fa[e] = pairs[e].i;
        fb[e] = pairs[e].j;
        eligible[e] = !pairs[e].below;
    }
    ranked = ranked && cbm_sem_admit_best_first(scores, fa, fb, eligible, n, max_edges, edge_counts,
                                                admitted);
    if (!ranked) {
        cbm_log_warn("semantic_edges.admission", "reason", "out_of_memory", "pairs", itoa_log(n));
    }

    int total_edges = 0;
    for (int e = 0; ranked && e < n; e++) {
        deferred_edge_t *de = &pairs[e];
        if (de->below) {
            if (signals) {
                pair_signals_line(signals, gbuf, funcs, de, false);
            }
            continue;
        }
        if (pair_dump) {
            fprintf(pair_dump, "%d %d %.9g\n", de->i, de->j, (double)de->score);
        }
        if (signals) {
            pair_signals_line(signals, gbuf, funcs, de, admitted[e]);
        }
        if (!admitted[e]) {
            continue;
        }
        /* p from the score as stored, so one shown score carries one p. */
        char score_text[CBM_SZ_32];
        snprintf(score_text, sizeof(score_text), "%.3f", (double)de->score);
        char props[PROPS_BUF];
        snprintf(props, sizeof(props), "{\"score\":%s,\"same_file\":%s,\"p\":%.2f}", score_text,
                 de->same_file ? "true" : "false",
                 (double)cbm_sem_calibrated_p(strtof(score_text, NULL)));
        cbm_gbuf_insert_edge(gbuf, de->source_id, de->target_id, "SEMANTICALLY_RELATED", props);
        total_edges++;
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, scores);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, fa);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, fb);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, eligible);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, admitted);
    if (pair_dump) {
        (void)fclose(pair_dump);
    }
    if (signals) {
        (void)fclose(signals);
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, pairs);
    return total_edges;
}

/* Phase 3c: export co-occurrence-enriched token vectors into the graph buffer
 * so query-time lookups can use them without re-running the finalize step. */
static void phase3c_export_token_vectors(cbm_gbuf_t *gbuf, cbm_sem_corpus_t *corpus) {
    int tv_count = cbm_sem_corpus_token_count(corpus);
    for (int t = 0; t < tv_count; t++) {
        const cbm_sem_vec_t *vec = NULL;
        float idf = 0.0F;
        const char *tok = cbm_sem_corpus_token_at(corpus, t, &vec, &idf);
        if (!tok || !vec || idf <= PSE_FLOW_WEIGHT) {
            continue;
        }
        uint8_t qvec[CBM_SEM_DIM];
        for (int d = 0; d < CBM_SEM_DIM; d++) {
            float clamped = vec->v[d];
            if (clamped > PSE_UNIT_POS) {
                clamped = PSE_UNIT_POS;
            }
            if (clamped < -PSE_UNIT_POS) {
                clamped = -PSE_UNIT_POS;
            }
            qvec[d] = (uint8_t)(int8_t)(clamped * PSE_INT8_MAX);
        }
        cbm_gbuf_store_token_vector(gbuf, tok, qvec, CBM_SEM_DIM, idf);
    }
    cbm_log_info("pass.semantic.token_vectors", "count", itoa_log(tv_count));
}

/* Phase 5a: generate NUM_HYPERPLANES × CBM_SEM_DIM deterministic random
 * float hyperplanes seeded from XXH3 so signatures are reproducible. */
static hyperplane_row_t *phase5a_build_hyperplanes(void) {
    hyperplane_row_t *hyperplanes =
        cbm_alloc(CBM_MEM_CLASS_SEMANTIC, sizeof(hyperplane_row_t) * NUM_HYPERPLANES);
    if (!hyperplanes) {
        return NULL;
    }
    for (int h = 0; h < NUM_HYPERPLANES; h++) {
        for (int d = 0; d < CBM_RSQ_DIM; d++) {
            uint64_t seed = XXH3_64bits_withSeed(&d, sizeof(d), (uint64_t)h * CBM_RSQ_DIM);
            hyperplanes[h][d] = ((float)(seed & UINT32_MAX) / (float)UINT32_MAX) - PSE_ROUND_BIAS;
        }
    }
    return hyperplanes;
}

/* Phase 1b: decode per-function minhash/profile/API/type/deco vectors in
 * parallel.  Must be called after cbm_sem_ensure_ready() so the pretrained
 * token map is initialized. */
static void phase1b_decode_and_build(cbm_sem_func_t *funcs, const cbm_gbuf_node_t **node_ptrs,
                                     const cbm_gbuf_t *gbuf, int func_count, int worker_count) {
    if (func_count <= 0) {
        return;
    }
    collect_ctx_t cc = {
        .funcs = funcs,
        .node_ptrs = node_ptrs,
        .gbuf = gbuf,
        .func_count = func_count,
    };
    atomic_init(&cc.next_idx, 0);
    cbm_parallel_for_opts_t opts = {.max_workers = worker_count, .force_pthreads = false};
    cbm_parallel_for(worker_count, collect_worker, &cc, opts);
}

/* Phase 2: tokenize each function's metadata in parallel, filling
 * all_tokens[] and token_counts[].  Caller allocates the arrays. */
static void phase2_tokenize(const cbm_gbuf_node_t **node_ptrs, cbm_gbuf_t *gbuf, char ***out_tokens,
                            size_t **out_offsets, int *token_counts, int func_count,
                            int worker_count, CBMHashTable **pools) {
    *out_tokens = NULL;
    *out_offsets = NULL;
    tok_buf_t *bufs = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, (size_t)worker_count * sizeof(tok_buf_t));
    int *tok_worker = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, (size_t)func_count * sizeof(int));
    size_t *tok_off = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, (size_t)func_count * sizeof(size_t));
    size_t *offsets = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, (size_t)func_count * sizeof(size_t));
    if (!bufs || !tok_worker || !tok_off || !offsets) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, bufs);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, tok_worker);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, tok_off);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, offsets);
        for (int f = 0; f < func_count; f++) {
            token_counts[f] = 0;
        }
        return;
    }
    tokenize_ctx_t tc = {
        .node_ptrs = node_ptrs,
        .gbuf = gbuf,
        .bufs = bufs,
        .tok_worker = tok_worker,
        .tok_off = tok_off,
        .token_counts = token_counts,
        .func_count = func_count,
        .pools = pools,
    };
    atomic_init(&tc.next_idx, 0);
    cbm_parallel_for_opts_t opts = {.max_workers = worker_count, .force_pthreads = false};
    cbm_parallel_for(worker_count, tokenize_worker, &tc, opts);

    /* Pack: one array, functions addressed by offset. Worker w's buffer lands
     * at base[w]; a function's tokens sit at base[worker] + its offset. */
    size_t total = 0;
    for (int w = 0; w < worker_count; w++) {
        total += bufs[w].count;
    }
    char **packed = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (total ? total : 1) * sizeof(char *));
    if (packed) {
        size_t base = 0;
        for (int w = 0; w < worker_count; w++) {
            if (bufs[w].count) {
                memcpy(packed + base, bufs[w].items, bufs[w].count * sizeof(*bufs[w].items));
            }
            /* Copied: give the worker buffer back now, so the packing transient
             * is the packed array plus ONE worker buffer, not plus all of them. */
            cbm_free(CBM_MEM_CLASS_SEMANTIC, bufs[w].items);
            bufs[w].items = NULL;
            bufs[w].cap = base; /* reuse: base offset of this worker's tokens */
            base += bufs[w].count;
        }
        for (int f = 0; f < func_count; f++) {
            offsets[f] = bufs[tok_worker[f]].cap + tok_off[f];
        }
    } else {
        for (int f = 0; f < func_count; f++) {
            token_counts[f] = 0;
        }
    }
    for (int w = 0; w < worker_count; w++) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, bufs[w].items);
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, bufs);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, tok_worker);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, tok_off);
    *out_tokens = packed;
    *out_offsets = offsets;
}

/* Phase 4a: build per-function TF-IDF + RI vectors in parallel, producing
 * int8-quantized qvecs for subsequent storage.  Phase 4b runs sequentially
 * to store them in gbuf because gbuf is not thread-safe. */
static void phase4_build_and_store_vectors(cbm_gbuf_t *gbuf, cbm_sem_func_t *funcs,
                                           char **all_tokens, const size_t *offsets,
                                           int *token_counts, cbm_sem_corpus_t *corpus,
                                           int func_count, int worker_count) {
    uint8_t *qvecs = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)func_count * CBM_SEM_DIM);
    if (!qvecs) {
        return;
    }
    vec_build_ctx_t vc = {
        .funcs = funcs,
        .all_tokens = all_tokens,
        .offsets = offsets,
        .token_counts = token_counts,
        .corpus = corpus,
        .qvecs = qvecs,
        .func_count = func_count,
    };
    atomic_init(&vc.next_idx, 0);
    cbm_parallel_for_opts_t opts = {.max_workers = worker_count, .force_pthreads = false};
    cbm_parallel_for(worker_count, vec_build_worker, &vc, opts);
    for (int f = 0; f < func_count; f++) {
        cbm_gbuf_store_vector(gbuf, funcs[f].node_id, &qvecs[(ptrdiff_t)f * CBM_SEM_DIM],
                              CBM_SEM_DIM);
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, qvecs);
}

/* Phase 5: hyperplane generation → signatures → LSH bucket population.
 * Returns the malloc'd signatures array and band_buckets[] via out-params;
 * caller frees both. */
static void phase5_lsh_build(cbm_sem_func_t *funcs, int func_count, int worker_count,
                             uint64_t **out_signatures, sem_bucket_t ***out_buckets) {
    hyperplane_row_t *hyperplanes = phase5a_build_hyperplanes();
    uint64_t *signatures =
        cbm_calloc(CBM_MEM_CLASS_SEMANTIC, (size_t)((size_t)func_count) * (sizeof(uint64_t)));
    if (hyperplanes && signatures) {
        sig_build_ctx_t sc = {
            .funcs = funcs,
            .signatures = signatures,
            .hyperplanes = hyperplanes,
            .func_count = func_count,
        };
        atomic_init(&sc.next_idx, 0);
        cbm_parallel_for_opts_t opts = {.max_workers = worker_count, .force_pthreads = false};
        cbm_parallel_for(worker_count, sig_build_worker, &sc, opts);
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, hyperplanes);

    sem_bucket_t **band_buckets =
        cbm_calloc(CBM_MEM_CLASS_SEMANTIC, (size_t)(SEM_LSH_BANDS) * (sizeof(sem_bucket_t *)));
    if (band_buckets) {
        for (int b = 0; b < SEM_LSH_BANDS; b++) {
            band_buckets[b] = cbm_calloc(CBM_MEM_CLASS_SEMANTIC,
                                         (size_t)(SEM_BUCKET_COUNT) * (sizeof(sem_bucket_t)));
        }
        phase5c_build_lsh_buckets(signatures, func_count, band_buckets);
    }
    *out_signatures = signatures;
    *out_buckets = band_buckets;
}

/* Checksum every input the scorer reads, so two runs can be compared at the
 * seam right before scoring. cbm_sem_combined_score() reads exactly these
 * fields, so identical checksums with differing scores would be impossible.
 * Placed AFTER tf-idf is filled (phase 4), not after phase 1b where the
 * sparse vectors are still empty. Diagnostic, off unless the env var is set. */
static void sem_state_dump(const cbm_sem_func_t *funcs, int func_count) {
    /* CBM_SEM_STATE_DUMP=1: one checksum over every per-function vector, so two
     * runs can be compared at THIS seam. Scores are pure math over these
     * vectors, so if the checksums match and the scores do not, the difference
     * is downstream; if they differ, it is here or above. Diagnostic only. */
    char state_path[CBM_SZ_1K];
    if (cbm_safe_getenv("CBM_SEM_STATE_DUMP", state_path, sizeof(state_path), NULL)) {
        uint64_t h = 1469598103934665603ULL;
        uint64_t h_codes = 1469598103934665603ULL;
        uint64_t h_tfidf = 1469598103934665603ULL;
        for (int i = 0; i < func_count; i++) {
            const unsigned char *bytes[] = {
                (const unsigned char *)&funcs[i].ri_code,
                (const unsigned char *)&funcs[i].api_code,
                (const unsigned char *)&funcs[i].type_code,
                (const unsigned char *)&funcs[i].deco_code,
            };
            for (size_t b = 0; b < sizeof(bytes) / sizeof(bytes[0]); b++) {
                for (size_t k = 0; k < sizeof(cbm_rsq_code_t); k++) {
                    h ^= bytes[b][k];
                    h *= 1099511628211ULL;
                    h_codes ^= bytes[b][k];
                    h_codes *= 1099511628211ULL;
                }
            }
            /* The other scoring inputs: sparse TF-IDF, the AST profile and the
             * minhash fingerprint. Scoring reads exactly these plus the codes
             * above, so a matching checksum means the scores must match too. */
            const unsigned char *sp = (const unsigned char *)funcs[i].struct_profile;
            for (size_t k = 0; k < sizeof(funcs[i].struct_profile); k++) {
                h ^= sp[k];
                h *= 1099511628211ULL;
            }
            const unsigned char *mh = (const unsigned char *)funcs[i].minhash;
            for (size_t k = 0; k < (funcs[i].has_minhash ? sizeof(funcs[i].minhash) : 0); k++) {
                h ^= mh[k];
                h *= 1099511628211ULL;
            }
            for (int w = 0; w < funcs[i].tfidf_len; w++) {
                h ^= (uint64_t)funcs[i].tfidf_indices[w];
                h *= 1099511628211ULL;
                uint32_t bits;
                memcpy(&bits, &funcs[i].tfidf_weights[w], sizeof(bits));
                h ^= bits;
                h *= 1099511628211ULL;
                h_tfidf ^= (uint64_t)funcs[i].tfidf_indices[w];
                h_tfidf *= 1099511628211ULL;
                h_tfidf ^= bits;
                h_tfidf *= 1099511628211ULL;
            }
        }
        /* To a FILE, not the log: the semantic pass runs in the index WORKER,
         * whose log is removed when the run succeeds. */
        FILE *sf = cbm_fopen(state_path, "w");
        if (sf) {
            fprintf(sf, "all=%016llx codes=%016llx tfidf=%016llx n=%d\n", (unsigned long long)h,
                    (unsigned long long)h_codes, (unsigned long long)h_tfidf, func_count);
            (void)fclose(sf);
        }
    }
}

/* Running hash of the token SEQUENCE handed to the corpus: the set AND the
 * order, because co-occurrence enrichment reads a window around each position,
 * so the same tokens in a different order produce different vectors while
 * leaving document frequencies (and therefore idf) untouched. Diagnostic for
 * the SEMANTICALLY_RELATED flicker; off unless CBM_SEM_STATE_DUMP is set. */
static uint64_t g_sem_token_hash = 1469598103934665603ULL;

static void sem_tokens_hash(char **tokens, const size_t *offsets, const int *counts, int n) {
    char flag[CBM_SZ_1K];
    if (!cbm_safe_getenv("CBM_SEM_STATE_DUMP", flag, sizeof(flag), NULL)) {
        return;
    }
    for (int d = 0; d < n; d++) {
        for (int k = 0; k < counts[d]; k++) {
            const char *tok = tokens[offsets[d] + (size_t)k];
            for (const unsigned char *c = (const unsigned char *)tok; c && *c; c++) {
                g_sem_token_hash ^= *c;
                g_sem_token_hash *= 1099511628211ULL;
            }
            g_sem_token_hash ^= 0xff; /* token boundary */
            g_sem_token_hash *= 1099511628211ULL;
        }
        g_sem_token_hash ^= 0xfe; /* document boundary */
        g_sem_token_hash *= 1099511628211ULL;
    }
}

/* Checksum the corpus AFTER enrichment: the per-function vectors and tf-idf are
 * both derived from it, so if this differs between two runs, everything
 * downstream differs and the cause is in the corpus build rather than in
 * scoring. Diagnostic, off unless CBM_SEM_STATE_DUMP is set. */
static void sem_corpus_dump(const cbm_sem_corpus_t *corpus, const char *suffix) {
    char state_path[CBM_SZ_1K];
    if (!cbm_safe_getenv("CBM_SEM_STATE_DUMP", state_path, sizeof(state_path), NULL)) {
        return;
    }
    char path[CBM_SZ_1K];
    if (snprintf(path, sizeof(path), "%s.%s", state_path, suffix) >= (int)sizeof(path)) {
        return;
    }
    int n = cbm_sem_corpus_token_count(corpus);
    uint64_t h_vec = 1469598103934665603ULL;
    uint64_t h_idf = 1469598103934665603ULL;
    for (int i = 0; i < n; i++) {
        const cbm_sem_vec_t *v = cbm_sem_corpus_ri_vec_at(corpus, i);
        if (v) {
            const unsigned char *b = (const unsigned char *)v;
            for (size_t k = 0; k < sizeof(*v); k++) {
                h_vec ^= b[k];
                h_vec *= 1099511628211ULL;
            }
        }
        uint32_t bits;
        float idf = cbm_sem_corpus_idf_at(corpus, i);
        memcpy(&bits, &idf, sizeof(bits));
        h_idf ^= bits;
        h_idf *= 1099511628211ULL;
    }
    FILE *sf = cbm_fopen(path, "w");
    if (sf) {
        fprintf(sf, "vec=%016llx idf=%016llx toks=%016llx tokens=%d\n", (unsigned long long)h_vec,
                (unsigned long long)h_idf, (unsigned long long)g_sem_token_hash, n);
        (void)fclose(sf);
    }
}

/* Phase 6a: score candidate pairs in parallel and collect deferred edges. */
static void phase6a_score_candidates(cbm_sem_func_t *funcs, uint64_t *signatures, int *edge_counts,
                                     sem_bucket_t **band_buckets, cbm_sem_config_t cfg,
                                     float record_floor, deferred_edge_buf_t *worker_bufs,
                                     int func_count, int worker_count) {
    score_ctx_t sc = {
        .funcs = funcs,
        .signatures = signatures,
        .edge_counts = edge_counts,
        .cfg = cfg,
        .record_floor = record_floor,
        .func_count = func_count,
        .band_buckets = (void *)band_buckets,
        .worker_bufs = worker_bufs,
        .max_workers = worker_count,
    };
    atomic_init(&sc.next_idx, 0);
    cbm_parallel_for_opts_t opts = {.max_workers = worker_count, .force_pthreads = false};
    cbm_parallel_for(worker_count, score_worker, &sc, opts);
}

/* Phase 7: free LSH bucket storage (items arrays and the per-band arrays). */
static void free_lsh_buckets(sem_bucket_t **band_buckets) {
    if (!band_buckets) {
        return;
    }
    for (int b = 0; b < SEM_LSH_BANDS; b++) {
        if (!band_buckets[b]) {
            continue;
        }
        for (int h = 0; h < SEM_BUCKET_COUNT; h++) {
            cbm_free(CBM_MEM_CLASS_SEMANTIC, band_buckets[b][h].items);
        }
        cbm_free(CBM_MEM_CLASS_SEMANTIC, band_buckets[b]);
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, band_buckets);
}

/* Phases 3a/3b/3c bundled: create corpus, batch-add docs, finalize, export
 * enriched token vectors to the graph buffer.  Returns the new corpus, which
 * the caller must cbm_sem_corpus_free() later. */
static cbm_sem_corpus_t *run_corpus_phase(cbm_gbuf_t *gbuf, char **all_tokens,
                                          const size_t *offsets, int *token_counts,
                                          int func_count) {
    CBM_PROF_START(t_phase3a);
    cbm_sem_corpus_t *corpus = cbm_sem_corpus_new();
    sem_tokens_hash(all_tokens, offsets, token_counts, func_count);
    cbm_sem_corpus_add_docs_batch(corpus, all_tokens, offsets, token_counts, func_count);
    CBM_PROF_END_N("semantic_edges", "3a_corpus_batch", t_phase3a, func_count);

    CBM_PROF_START(t_phase3b);
    cbm_sem_corpus_finalize(corpus);
    sem_corpus_dump(corpus, "corpus");
    CBM_PROF_END_N("semantic_edges", "3b_corpus_finalize_seq", t_phase3b,
                   cbm_sem_corpus_token_count(corpus));

    CBM_PROF_START(t_phase3c);
    phase3c_export_token_vectors(gbuf, corpus);
    CBM_PROF_END_N("semantic_edges", "3c_token_vec_export_seq", t_phase3c,
                   cbm_sem_corpus_token_count(corpus));
    return corpus;
}

/* Phases 6a/6b bundled: parallel scoring + sequential merge of deferred
 * edges.  Returns the number of emitted edges. */
static int run_scoring_phase(cbm_gbuf_t *gbuf, cbm_sem_func_t *funcs, uint64_t *signatures,
                             sem_bucket_t **band_buckets, cbm_sem_config_t cfg, int func_count,
                             int worker_count) {
    int *edge_counts =
        cbm_calloc(CBM_MEM_CLASS_SEMANTIC, (size_t)((size_t)func_count) * (sizeof(int)));
    deferred_edge_buf_t *worker_bufs = cbm_calloc(
        CBM_MEM_CLASS_SEMANTIC, (size_t)((size_t)worker_count) * (sizeof(deferred_edge_buf_t)));
    if (!edge_counts || !worker_bufs) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, edge_counts);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, worker_bufs);
        return 0;
    }
    for (int w = 0; w < worker_count; w++) {
        deferred_buf_init(&worker_bufs[w]);
    }

    /* The measuring harness may record pairs below the threshold (never
     * admitted): CBM_SEM_PAIR_SIGNALS_FLOOR, only with CBM_SEM_PAIR_SIGNALS. */
    float record_floor = cfg.threshold;
    {
        char path[CBM_SZ_1K]; /* the dump path: a buffer too short reads as unset */
        char buf[CBM_SZ_64];
        if (cbm_safe_getenv("CBM_SEM_PAIR_SIGNALS", path, sizeof(path), NULL) &&
            cbm_safe_getenv("CBM_SEM_PAIR_SIGNALS_FLOOR", buf, sizeof(buf), NULL)) {
            float floor_value = strtof(buf, NULL);
            if (floor_value > 0.0F && floor_value < cfg.threshold) {
                record_floor = floor_value;
            }
        }
    }

    CBM_PROF_START(t_phase6a);
    sem_state_dump(funcs, func_count);
    phase6a_score_candidates(funcs, signatures, edge_counts, band_buckets, cfg, record_floor,
                             worker_bufs, func_count, worker_count);
    CBM_PROF_END_N("semantic_edges", "6a_score_parallel", t_phase6a, func_count);

    CBM_PROF_START(t_phase6b);
    int total =
        phase6b_merge_edges(gbuf, worker_bufs, worker_count, edge_counts, cfg.max_edges, funcs);
    CBM_PROF_END_N("semantic_edges", "6b_edge_merge_seq", t_phase6b, total);

    cbm_free(CBM_MEM_CLASS_SEMANTIC, worker_bufs);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, edge_counts);
    return total;
}

/* Free the per-function arrays malloc'd during phases 1b and 4a. */
static void free_token_pool_entry(const char *key, void *value, void *ud) {
    (void)key; /* key == value: one owned string per unique token */
    (void)ud;
    cbm_free(CBM_MEM_CLASS_SEMANTIC, value);
}

/* all_tokens slots BORROW their strings from the per-worker intern pools;
 * the pools own exactly one copy per unique token per worker. */
static void free_funcs_and_tokens(cbm_sem_func_t *funcs, int func_count, char **all_tokens,
                                  size_t *offsets, const int *token_counts, CBMHashTable **pools,
                                  int worker_count) {
    (void)token_counts;
    cbm_free(CBM_MEM_CLASS_SEMANTIC, offsets);
    for (int f = 0; f < func_count; f++) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, funcs[f].tfidf_indices);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, funcs[f].tfidf_weights);
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, all_tokens);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, funcs);
    if (pools) {
        for (int w = 0; w < worker_count; w++) {
            if (pools[w]) {
                cbm_ht_foreach(pools[w], free_token_pool_entry, NULL);
                cbm_ht_free(pools[w]);
            }
        }
        cbm_free(CBM_MEM_CLASS_SEMANTIC, pools);
    }
}

/* ── Memory checkpoints inside the pass (CBM_MEM_PHASES=1) ─────────── */

/* The pass is one phase mark to the pipeline; its transient lives between
 * marks. These lines put the semantic class's live/peak bytes and the charged
 * footprint at every sub-phase boundary, so the peak is attributable. */
static void sem_mem_mark(const char *step) {
    if (!cbm_mem_phases_enabled()) {
        return;
    }
    enum { MB = 1024 * 1024 };
    char live[CBM_SZ_16];
    char peak[CBM_SZ_16];
    char charged[CBM_SZ_16];
    snprintf(live, sizeof(live), "%zu", cbm_mem_class_live_bytes(CBM_MEM_CLASS_SEMANTIC) / MB);
    snprintf(peak, sizeof(peak), "%zu", cbm_mem_class_peak_bytes(CBM_MEM_CLASS_SEMANTIC) / MB);
    snprintf(charged, sizeof(charged), "%zu", cbm_mem_charged() / MB);
    cbm_log_info("mem.semantic.step", "step", step, "sem_live_mb", live, "sem_peak_mb", peak,
                 "charged_mb", charged);
}

/* ── Headroom-sized batches ───────────────────────────────────────── */

/* Phases 2-4 hold, per function, its token pointers, the corpus's per-doc
 * token ids and the quantized vector being stored: ~3 KB per function on the
 * kernel (783k functions -> the 4.9 GB transient the 15 GB budget run
 * overshot with, 2026-09-13). The retained part -- the funcs array and the
 * corpus entries -- is what scoring needs whole and stays. When the charged
 * footprint plus that transient would cross the budget, the token phases run
 * per batch of functions: tokenize -> count into the corpus -> free, then,
 * after finalize, tokenize again -> vectorize -> store -> free. Tokenization
 * is deterministic, so both passes see the same tokens and the graph does
 * not change; the run pays with the second tokenize. */
enum {
    SEM_BATCH_BYTES_PER_FUNC = 4096, /* measured 3 KB, rounded up */
    SEM_BATCH_MIN_FUNCS = 4096,
};

static int sem_batch_size(int func_count) {
    /* CBM_SEM_BATCH=<n> forces the batch size regardless of budget (the
     * batched-equals-unbatched test, small-machine iteration). */
    char forced_buf[CBM_SZ_16];
    if (cbm_safe_getenv("CBM_SEM_BATCH", forced_buf, sizeof(forced_buf), NULL)) {
        int forced = atoi(forced_buf);
        if (forced > 0 && forced < func_count) {
            cbm_log_info("mem.semantic.batches", "functions", itoa_log(func_count), "batch",
                         itoa_log(forced), "reason", "env");
            return forced;
        }
    }
    size_t budget = cbm_mem_budget();
    if (budget == 0 || func_count <= SEM_BATCH_MIN_FUNCS) {
        return func_count;
    }
    size_t charged = cbm_mem_charged();
    size_t headroom = budget > charged ? budget - charged : 0;
    size_t transient = (size_t)func_count * SEM_BATCH_BYTES_PER_FUNC;
    /* Half the headroom is the transient's share; the other half is what the
     * later phases (signatures, buckets, deferred edges) and mimalloc keep. */
    size_t share = headroom / 2;
    if (transient <= share) {
        return func_count;
    }
    size_t batch = share / SEM_BATCH_BYTES_PER_FUNC;
    if (batch < SEM_BATCH_MIN_FUNCS) {
        batch = SEM_BATCH_MIN_FUNCS;
    }
    if (batch > (size_t)func_count) {
        batch = (size_t)func_count;
    }
    enum { MB = 1024 * 1024 };
    cbm_log_info("mem.semantic.batches", "functions", itoa_log(func_count), "batch",
                 itoa_log((int)batch), "headroom_mb", itoa_log((int)(headroom / MB)),
                 "transient_mb", itoa_log((int)(transient / MB)));
    return (int)batch;
}

/* One batch of the token phases: tokenize functions [first, first+count) into
 * a fresh packed array the caller frees with sem_batch_free_tokens(). */
static void sem_batch_tokenize(const cbm_gbuf_node_t **node_ptrs, cbm_gbuf_t *gbuf, int first,
                               int count, int worker_count, CBMHashTable **pools, int *token_counts,
                               char ***out_tokens, size_t **out_offsets) {
    phase2_tokenize(node_ptrs + first, gbuf, out_tokens, out_offsets, token_counts + first, count,
                    worker_count, pools);
}

static void sem_batch_free_tokens(char **tokens, size_t *offsets) {
    cbm_free(CBM_MEM_CLASS_SEMANTIC, tokens);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, offsets);
}

/* Phases 2-4 in headroom-sized batches. The corpus is fed batch by batch
 * (its per-doc token ids are ints, kept for co-occurrence), finalized once,
 * then vectors are built batch by batch from a second tokenization. */
static cbm_sem_corpus_t *run_token_phases_batched(cbm_gbuf_t *gbuf, cbm_sem_func_t *funcs,
                                                  const cbm_gbuf_node_t **node_ptrs,
                                                  int *token_counts, int func_count, int batch,
                                                  int worker_count, CBMHashTable **token_pools) {
    CBM_PROF_START(t_phase2);
    cbm_sem_corpus_t *corpus = cbm_sem_corpus_new();
    for (int first = 0; first < func_count; first += batch) {
        int count = func_count - first < batch ? func_count - first : batch;
        char **tokens = NULL;
        size_t *offsets = NULL;
        sem_batch_tokenize(node_ptrs, gbuf, first, count, worker_count, token_pools, token_counts,
                           &tokens, &offsets);
        if (tokens && offsets) {
            sem_tokens_hash(tokens, offsets, token_counts + first, count);
            cbm_sem_corpus_add_docs_batch(corpus, tokens, offsets, token_counts + first, count);
        }
        sem_batch_free_tokens(tokens, offsets);
        cbm_mem_release_to_os(); /* the batch's pages leave the charge now, not at the mark */
    }
    CBM_PROF_END_N("semantic_edges", "2+3a_tokenize_count_batched", t_phase2, func_count);
    sem_mem_mark("3a_corpus_batched");

    CBM_PROF_START(t_phase3b);
    cbm_sem_corpus_finalize(corpus);
    sem_corpus_dump(corpus, "corpus");
    CBM_PROF_END_N("semantic_edges", "3b_corpus_finalize_seq", t_phase3b,
                   cbm_sem_corpus_token_count(corpus));
    CBM_PROF_START(t_phase3c);
    phase3c_export_token_vectors(gbuf, corpus);
    CBM_PROF_END_N("semantic_edges", "3c_token_vec_export_seq", t_phase3c,
                   cbm_sem_corpus_token_count(corpus));
    sem_mem_mark("3c_export");

    CBM_PROF_START(t_phase4);
    for (int first = 0; first < func_count; first += batch) {
        int count = func_count - first < batch ? func_count - first : batch;
        char **tokens = NULL;
        size_t *offsets = NULL;
        sem_batch_tokenize(node_ptrs, gbuf, first, count, worker_count, token_pools, token_counts,
                           &tokens, &offsets);
        if (tokens && offsets) {
            phase4_build_and_store_vectors(gbuf, funcs + first, tokens, offsets,
                                           token_counts + first, corpus, count, worker_count);
        }
        sem_batch_free_tokens(tokens, offsets);
        cbm_mem_release_to_os();
    }
    CBM_PROF_END_N("semantic_edges", "2+4_tokenize_vectorize_batched", t_phase4, func_count);
    sem_mem_mark("4_vectors");
    return corpus;
}

/* ── Pass entry point ────────────────────────────────────────────── */

/* ── Phase 6c: doc sections -> functions (candidate generator) ───── */

/* Doc -> code candidates: for every Section node that can be about code, the
 * functions whose TF-IDF terms best match the section's heading + body, over
 * the FUNCTIONS' corpus (sections never change a function-pair score).
 *   Gate: no candidates for sections whose heading names a release (v1.2), a
 *   changelog, licence, contributing, links, contents, install or support
 *   kind of section, whose body has fewer than SEM_DOC_MIN_WORDS words, or
 *   whose body is mostly links.
 *   Targets: never a test function (by path or name); candidates come from
 *   the section's key terms (held by at most one in SEM_DOC_KEY_DF_DIV
 *   functions, or SEM_DOC_KEY_DF_MIN) and are scored on every shared term.
 * Stored (doc_link_candidates, never graph edges): a section's best
 * CBM_SEM_DOC_TOP_K with tfidf >= CBM_SEM_DOC_MIN_SCORE, each with its judged
 * probability (cbm_sem_doc_calibrated_p) -- candidates for an agent to
 * verify, not links.
 * CBM_SEM_DOC_SIGNALS=<path> also dumps every section's best SEM_DOC_TOP_K:
 * section and function qualified names, tfidf, rank, shared terms, both
 * locations, and the name evidence (the function's name tokens present in the
 * section, and the highest idf among them) -- the population the judged
 * calibration samples are drawn from. */
enum {
    SEM_DOC_TOP_K = 10,
    SEM_DOC_KEY_DF_MIN = 50,
    SEM_DOC_KEY_DF_DIV = 20,
    SEM_DOC_MAX_TOKENS = 1024,
    SEM_DOC_TEXT_MAX = 8192,
    SEM_DOC_NAME_TOKENS = 16,
    SEM_DOC_MIN_WORDS = 20,
    SEM_DOC_LINK_PCT_MAX = 20, /* links per 100 body words */
    SEM_DOC_WORD_MIN_LEN = 3,
};

typedef struct {
    float score;
    int func;
    int shared;
} doc_cand_t;

typedef struct {
    int func;
    float tfidf;
    int rank;
    int shared;
    int name_shared; /* the function's NAME tokens present in the section */
    float name_idf;  /* the highest idf among them (0: none) */
} doc_hit_t;

typedef struct {
    int file;
    float tfidf;
    int rank;
    int shared;
} doc_file_hit_t;

typedef struct {
    doc_hit_t hits[SEM_DOC_TOP_K];
    int n;
    doc_hit_t local[CBM_SEM_DOC_LOCAL_K]; /* inside the home, below the top CBM_SEM_DOC_TOP_K */
    int nlocal;
    doc_file_hit_t files[CBM_SEM_DOC_FILE_K];
    int nfiles;
} doc_result_t;

static int cmp_doc_cand(const void *pa, const void *pb) {
    const doc_cand_t *a = pa;
    const doc_cand_t *b = pb;
    if (a->score != b->score) {
        return a->score > b->score ? -1 : 1;
    }
    return (a->func > b->func) - (a->func < b->func);
}

static int cmp_section_node(const void *pa, const void *pb) {
    const cbm_gbuf_node_t *a = *(const cbm_gbuf_node_t *const *)pa;
    const cbm_gbuf_node_t *b = *(const cbm_gbuf_node_t *const *)pb;
    int c = strcmp(a->qualified_name ? a->qualified_name : "",
                   b->qualified_name ? b->qualified_name : "");
    if (c) {
        return c;
    }
    return (a->start_line > b->start_line) - (a->start_line < b->start_line);
}

/* Heading kinds that are about the project, not about code: a release
 * number, or one of these words (a trailing '*' matches any ending). */
static const char *const DOC_SKIP_HEADING[] = {"changelog",
                                               "change log",
                                               "release note*",
                                               "what's new",
                                               "license",
                                               "licence",
                                               "contribut*",
                                               "acknowledg*",
                                               "credits",
                                               "sponsor*",
                                               "author*",
                                               "code of conduct",
                                               "security policy",
                                               "links",
                                               "references",
                                               "resources",
                                               "further reading",
                                               "table of contents",
                                               "contents",
                                               "toc",
                                               "installation",
                                               "install",
                                               "getting started",
                                               "support",
                                               "community",
                                               "faq",
                                               "roadmap",
                                               "todo",
                                               NULL};

static bool doc_word_char(char c) {
    return isalnum((unsigned char)c) || c == '\'';
}

/* Does lowercase heading lc contain word (or word* as a prefix) at a word start? */
static bool doc_heading_has(const char *lc, const char *word) {
    size_t wl = strlen(word);
    bool prefix = wl > 0 && word[wl - SKIP_ONE] == '*';
    size_t ml = prefix ? wl - SKIP_ONE : wl;
    for (const char *p = lc; *p; p++) {
        if (strncmp(p, word, ml) != 0) {
            continue;
        }
        if (p > lc && doc_word_char(p[-SKIP_ONE])) {
            continue;
        }
        if (!prefix && doc_word_char(p[ml])) {
            continue;
        }
        return true;
    }
    return false;
}

/* A release-number heading: "v1.2", "1.2.3", "[0.12.15]", "Gin v1.11.0". */
static bool doc_heading_is_release(const char *lc) {
    for (const char *p = lc; *p; p++) {
        if (p > lc && doc_word_char(p[-SKIP_ONE])) {
            continue;
        }
        const char *q = *p == 'v' ? p + SKIP_ONE : p;
        if (!isdigit((unsigned char)*q)) {
            continue;
        }
        while (isdigit((unsigned char)*q)) {
            q++;
        }
        if (*q == '.' && isdigit((unsigned char)q[SKIP_ONE])) {
            return true;
        }
    }
    return false;
}

/* Can this section be about code? (heading kind, body length, link share) */
static bool doc_section_gated(const cbm_gbuf_node_t *sec, char *body) {
    char lc[CBM_SZ_256];
    size_t n = 0;
    for (const char *p = sec->name ? sec->name : ""; *p && n + SKIP_ONE < sizeof(lc); p++) {
        lc[n++] = (char)tolower((unsigned char)*p);
    }
    lc[n] = '\0';
    if (doc_heading_is_release(lc)) {
        return false;
    }
    for (int k = 0; DOC_SKIP_HEADING[k]; k++) {
        if (doc_heading_has(lc, DOC_SKIP_HEADING[k])) {
            return false;
        }
    }
    if (!json_str_value(sec->properties_json, "docstring", body, SEM_DOC_TEXT_MAX)) {
        return false;
    }
    int words = 0;
    int links = 0;
    int run = 0;
    for (const char *p = body;; p++) {
        if (isalpha((unsigned char)*p)) {
            run++;
            continue;
        }
        words += run >= SEM_DOC_WORD_MIN_LEN ? SKIP_ONE : 0;
        run = 0;
        if (!*p) {
            break;
        }
        if (strncmp(p, "http://", sizeof("http://") - SKIP_ONE) == 0 ||
            strncmp(p, "https://", sizeof("https://") - SKIP_ONE) == 0 ||
            strncmp(p, "](", sizeof("](") - SKIP_ONE) == 0) {
            links++;
        }
    }
    return words >= SEM_DOC_MIN_WORDS && links * 100 < SEM_DOC_LINK_PCT_MAX * words;
}

/* A test file: in a test directory, or a test file name. */
static bool doc_is_test_path(const char *path) {
    static const char *const DIRS[] = {"test/",  "tests/",    "__tests__/", "testing/",  "spec/",
                                       "specs/", "testdata/", "fixture/",   "fixtures/", NULL};
    path = path ? path : "";
    for (int k = 0; DIRS[k]; k++) {
        for (const char *p = path; (p = strstr(p, DIRS[k])) != NULL; p++) {
            if (p == path || p[-SKIP_ONE] == '/') {
                return true;
            }
        }
    }
    const char *base = strrchr(path, '/');
    base = base ? base + SKIP_ONE : path;
    return strncmp(base, "test_", sizeof("test_") - SKIP_ONE) == 0 || strstr(base, "_test.") ||
           strstr(base, ".test.") || strstr(base, ".spec.") || strstr(base, "Test.") ||
           strstr(base, "Tests.");
}

/* A test function, by its file or name. */
static bool doc_is_test_function(const cbm_gbuf_node_t *fn) {
    if (doc_is_test_path(fn ? fn->file_path : NULL)) {
        return true;
    }
    const char *name = fn && fn->name ? fn->name : "";
    return strncmp(name, "test_", sizeof("test_") - SKIP_ONE) == 0 ||
           (strncmp(name, "Test", sizeof("Test") - SKIP_ONE) == 0 &&
            (isupper((unsigned char)name[4]) || name[4] == '_'));
}

/* The section's tokens (heading + body); the caller frees each token. */
static int doc_section_tokens(const cbm_gbuf_node_t *n, const char *body, char **tokens) {
    int count = cbm_sem_tokenize(n->name, tokens, SEM_DOC_MAX_TOKENS);
    if (body && count < SEM_DOC_MAX_TOKENS) {
        count += cbm_sem_tokenize(body, tokens + count, SEM_DOC_MAX_TOKENS - count);
    }
    return count;
}

/* Dot product and shared-term count of two ascending term lists. */
static float doc_terms_dot(const int *ia, const float *wa, int na, const int *ib, const float *wb,
                           int nb, int *shared) {
    float dot = 0.0F;
    int a = 0;
    int b = 0;
    *shared = 0;
    while (a < na && b < nb) {
        if (ia[a] == ib[b]) {
            dot += wa[a] * wb[b];
            (*shared)++;
            a++;
            b++;
        } else if (ia[a] < ib[b]) {
            a++;
        } else {
            b++;
        }
    }
    return dot;
}

static float terms_norm(const float *w, int n) {
    float s = 0.0F;
    for (int k = 0; k < n; k++) {
        s += w[k] * w[k];
    }
    return sqrtf(s);
}

/* ── Phase 6c: whole files and the doc's home folder ── */

/* Whole-file candidates: a file's terms are the sum of its non-test
 * functions' TF-IDF vectors and the TF-IDF vectors of its other definitions'
 * names (a config file's keys are Variable nodes), cut to its
 * SEM_DOC_FILE_TERMS heaviest terms (ties by token index), so a file's vector
 * costs at most what one long function's does. */
enum { SEM_DOC_FILE_TERMS = 64 };
static const char *const DOC_FILE_DEF_LABELS[] = {"Class",    "Struct", "Interface", "Enum",
                                                  "Type",     "Trait",  "Field",     "Variable",
                                                  "Constant", "Macro",  NULL};

typedef struct {
    int count;
    const cbm_gbuf_node_t **node; /* per file: its File node */
    int *off;                     /* count + 1: each file's terms in idx / w */
    int *idx;
    float *w;
    float *norm;
    int *post_off; /* nterms + 1: the files holding each term */
    int *post_f;
    CBMHashTable *by_path; /* file path (graph-owned) -> file index + 1 */
} doc_files_t;

typedef struct {
    int idx;
    float w;
} doc_term_t;

static int cmp_term_idx(const void *pa, const void *pb) {
    const doc_term_t *a = pa;
    const doc_term_t *b = pb;
    return (a->idx > b->idx) - (a->idx < b->idx);
}

static int cmp_term_heavy(const void *pa, const void *pb) {
    const doc_term_t *a = pa;
    const doc_term_t *b = pb;
    if (a->w != b->w) {
        return a->w > b->w ? -1 : 1;
    }
    return (a->idx > b->idx) - (a->idx < b->idx);
}

static void doc_files_free(doc_files_t *df) {
    cbm_free(CBM_MEM_CLASS_SEMANTIC, df->node);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, df->off);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, df->idx);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, df->w);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, df->norm);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, df->post_off);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, df->post_f);
    if (df->by_path) {
        cbm_ht_free(df->by_path);
    }
    memset(df, 0, sizeof(*df));
}

static int doc_file_of(const doc_files_t *df, const char *path) {
    return path ? (int)(intptr_t)cbm_ht_get(df->by_path, path) - SKIP_ONE : -SKIP_ONE;
}

/* The scratch terms of one file: append n terms, growing the buffer. */
typedef struct {
    doc_term_t *t;
    int n;
    int cap;
} doc_scratch_t;

static bool doc_scratch_add(doc_scratch_t *sc, const int *idx, const float *w, int n) {
    if (sc->n + n > sc->cap) {
        int cap = sc->cap ? sc->cap : CBM_SZ_256;
        while (cap < sc->n + n) {
            cap *= 2;
        }
        doc_term_t *t = cbm_realloc(CBM_MEM_CLASS_SEMANTIC, sc->t, (size_t)cap * sizeof(*t));
        if (!t) {
            return false;
        }
        sc->t = t;
        sc->cap = cap;
    }
    for (int k = 0; k < n; k++) {
        sc->t[sc->n++] = (doc_term_t){.idx = idx[k], .w = w[k]};
    }
    return true;
}

/* Sum the scratch terms per token and keep the heaviest SEM_DOC_FILE_TERMS,
 * written at df->idx/w + off in ascending token order; returns how many. */
static int doc_scratch_reduce(doc_scratch_t *sc, doc_files_t *df, int off) {
    if (sc->n == 0) {
        return 0;
    }
    qsort(sc->t, (size_t)sc->n, sizeof(*sc->t), cmp_term_idx);
    int m = 0;
    for (int k = 0; k < sc->n; k++) {
        if (m > 0 && sc->t[m - SKIP_ONE].idx == sc->t[k].idx) {
            sc->t[m - SKIP_ONE].w += sc->t[k].w;
        } else {
            sc->t[m++] = sc->t[k];
        }
    }
    if (m > SEM_DOC_FILE_TERMS) {
        qsort(sc->t, (size_t)m, sizeof(*sc->t), cmp_term_heavy);
        m = SEM_DOC_FILE_TERMS;
        qsort(sc->t, (size_t)m, sizeof(*sc->t), cmp_term_idx);
    }
    for (int k = 0; k < m; k++) {
        df->idx[off + k] = sc->t[k].idx;
        df->w[off + k] = sc->t[k].w;
    }
    return m;
}

/* The definitions (non-function) of every indexed non-test file, grouped by
 * file: def_off (count + 1) into def_node. */
static bool doc_files_defs(const cbm_gbuf_t *gbuf, const doc_files_t *df, int **def_off,
                           const cbm_gbuf_node_t ***def_node) {
    *def_off = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, ((size_t)df->count + SKIP_ONE) * sizeof(int));
    if (!*def_off) {
        return false;
    }
    for (int pass = 0; pass < 2; pass++) {
        int *cursor =
            pass ? cbm_calloc(CBM_MEM_CLASS_SEMANTIC, (size_t)df->count * sizeof(int)) : NULL;
        if (pass && !cursor) {
            return false;
        }
        for (int l = 0; DOC_FILE_DEF_LABELS[l]; l++) {
            const cbm_gbuf_node_t **nodes = NULL;
            int n = 0;
            if (cbm_gbuf_find_by_label(gbuf, DOC_FILE_DEF_LABELS[l], &nodes, &n) != 0) {
                continue;
            }
            for (int i = 0; i < n; i++) {
                int f = nodes[i]->name ? doc_file_of(df, nodes[i]->file_path) : -SKIP_ONE;
                if (f >= 0 && !pass) {
                    (*def_off)[f + SKIP_ONE]++;
                } else if (f >= 0) {
                    (*def_node)[(*def_off)[f] + cursor[f]++] = nodes[i];
                }
            }
        }
        if (!pass) {
            for (int f = 0; f < df->count; f++) {
                (*def_off)[f + SKIP_ONE] += (*def_off)[f];
            }
            *def_node = cbm_alloc(CBM_MEM_CLASS_SEMANTIC,
                                  ((size_t)(*def_off)[df->count] + SKIP_ONE) * sizeof(**def_node));
            if (!*def_node) {
                return false;
            }
        }
        cbm_free(CBM_MEM_CLASS_SEMANTIC, cursor);
    }
    return true;
}

/* The scratch terms of one definition's name. */
static bool doc_scratch_name(doc_scratch_t *sc, const cbm_sem_corpus_t *corpus, const char *name) {
    char *tok[SEM_DOC_NAME_TOKENS];
    int idx[SEM_DOC_NAME_TOKENS];
    float w[SEM_DOC_NAME_TOKENS];
    int nt = cbm_sem_tokenize(name, tok, SEM_DOC_NAME_TOKENS);
    int nw = nt > 0 ? cbm_sem_tfidf_terms(corpus, tok, NULL, nt, idx, w) : 0;
    for (int t = 0; t < nt; t++) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, tok[t]);
    }
    return doc_scratch_add(sc, idx, w, nw);
}

/* Each file's term vector, norm, and the term -> files postings. */
static bool doc_files_vectors(const cbm_gbuf_t *gbuf, doc_files_t *df, const cbm_sem_func_t *funcs,
                              const int *ffile, int func_count, const cbm_sem_corpus_t *corpus,
                              int nterms) {
    int *fn_off = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, ((size_t)df->count + SKIP_ONE) * sizeof(int));
    int *fn_list = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, ((size_t)func_count + SKIP_ONE) * sizeof(int));
    int *def_off = NULL;
    const cbm_gbuf_node_t **def_node = NULL;
    doc_scratch_t sc = {0};
    bool ok = fn_off && fn_list && doc_files_defs(gbuf, df, &def_off, &def_node);
    for (int f = 0; ok && f < func_count; f++) {
        fn_off[ffile[f] + SKIP_ONE] += ffile[f] >= 0;
    }
    for (int i = 0; ok && i < df->count; i++) {
        fn_off[i + SKIP_ONE] += fn_off[i];
    }
    for (int f = 0; ok && f < func_count; f++) {
        if (ffile[f] >= 0) {
            fn_list[fn_off[ffile[f]]++] = f; /* fn_off[i] ends at file i's end: shifted below */
        }
    }
    for (int i = df->count; ok && i > 0; i--) {
        fn_off[i] = fn_off[i - SKIP_ONE];
    }
    if (ok) {
        fn_off[0] = 0;
    }
    for (int i = 0; ok && i < df->count; i++) {
        sc.n = 0;
        for (int k = fn_off[i]; ok && k < fn_off[i + SKIP_ONE]; k++) {
            const cbm_sem_func_t *fn = &funcs[fn_list[k]];
            ok = doc_scratch_add(&sc, fn->tfidf_indices, fn->tfidf_weights, fn->tfidf_len);
        }
        for (int k = def_off[i]; ok && k < def_off[i + SKIP_ONE]; k++) {
            ok = doc_scratch_name(&sc, corpus, def_node[k]->name);
        }
        int m = ok ? doc_scratch_reduce(&sc, df, df->off[i]) : 0;
        df->off[i + SKIP_ONE] = df->off[i] + m;
        df->norm[i] = terms_norm(df->w + df->off[i], m);
        for (int k = 0; k < m; k++) {
            df->post_off[df->idx[df->off[i] + k] + SKIP_ONE]++;
        }
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, sc.t);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, fn_off);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, fn_list);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, def_off);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, def_node);
    if (!ok) {
        return false;
    }
    for (int t = 0; t < nterms; t++) {
        df->post_off[t + SKIP_ONE] += df->post_off[t];
    }
    df->post_f =
        cbm_alloc(CBM_MEM_CLASS_SEMANTIC, ((size_t)df->post_off[nterms] + SKIP_ONE) * sizeof(int));
    int *cursor = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, ((size_t)nterms + SKIP_ONE) * sizeof(int));
    ok = df->post_f && cursor;
    for (int i = 0; ok && i < df->count; i++) {
        for (int k = df->off[i]; k < df->off[i + SKIP_ONE]; k++) {
            int t = df->idx[k];
            df->post_f[df->post_off[t] + cursor[t]++] = i;
        }
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, cursor);
    return ok;
}

/* The indexed non-test files (their File nodes) with their term vectors;
 * ffile[f] = function f's file (-1: a test function or no File node). */
static bool doc_files_build(const cbm_gbuf_t *gbuf, const cbm_sem_func_t *funcs, int func_count,
                            const bool *is_test, const cbm_sem_corpus_t *corpus, int nterms,
                            doc_files_t *df, int *ffile) {
    memset(df, 0, sizeof(*df));
    const cbm_gbuf_node_t **files = NULL;
    int nfiles = 0;
    if (cbm_gbuf_find_by_label(gbuf, "File", &files, &nfiles) != 0) {
        nfiles = 0;
    }
    df->by_path = cbm_ht_create_in(CBM_MEM_CLASS_SEMANTIC, (uint32_t)nfiles + SKIP_ONE);
    df->node = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, ((size_t)nfiles + SKIP_ONE) * sizeof(*df->node));
    if (!df->by_path || !df->node) {
        return false;
    }
    for (int i = 0; i < nfiles; i++) {
        const char *path = files[i]->file_path;
        if (path && path[0] && !doc_is_test_path(path) && !cbm_ht_has(df->by_path, path)) {
            df->node[df->count] = files[i];
            cbm_ht_set(df->by_path, path, (void *)(intptr_t)(df->count + SKIP_ONE));
            df->count++;
        }
    }
    for (int f = 0; f < func_count; f++) {
        const cbm_gbuf_node_t *fn = cbm_gbuf_find_by_id(gbuf, funcs[f].node_id);
        ffile[f] = is_test[f] || !fn ? -SKIP_ONE : doc_file_of(df, fn->file_path);
    }
    size_t cap = (size_t)df->count * SEM_DOC_FILE_TERMS + SKIP_ONE;
    df->off = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, ((size_t)df->count + SKIP_ONE) * sizeof(int));
    df->idx = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, cap * sizeof(int));
    df->w = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, cap * sizeof(float));
    df->norm = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, ((size_t)df->count + SKIP_ONE) * sizeof(float));
    df->post_off = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, ((size_t)nterms + SKIP_ONE) * sizeof(int));
    return df->off && df->idx && df->w && df->norm && df->post_off &&
           doc_files_vectors(gbuf, df, funcs, ffile, func_count, corpus, nterms);
}

/* A doc's home folder: the folder whose code it documents. From the doc's
 * directory, a docs folder (doc, docs, documentation: the deepest one)
 * documents its parent; then climb while the folder holds no indexed code
 * (a non-test file with terms). "" = the repository root: the doc documents
 * the whole project. Returns an allocated string (NULL: out of memory). */
static bool doc_dir_is_docs(const char *name, size_t n) {
    static const char *const DOCS[] = {"doc", "docs", "documentation", NULL};
    for (int k = 0; DOCS[k]; k++) {
        if (strlen(DOCS[k]) == n && strncasecmp(name, DOCS[k], n) == 0) {
            return true;
        }
    }
    return false;
}

static char *doc_home(const char *doc_path, const CBMHashTable *code_dirs) {
    const char *slash = doc_path ? strrchr(doc_path, '/') : NULL;
    size_t len = slash ? (size_t)(slash - doc_path) : 0;
    char *dir = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, len + SKIP_ONE);
    if (!dir) {
        return NULL;
    }
    memcpy(dir, doc_path ? doc_path : "", len);
    dir[len] = '\0';
    size_t cut = len + SKIP_ONE; /* the deepest docs folder's start (none) */
    for (size_t a = 0; a < len;) {
        const char *end = strchr(dir + a, '/');
        size_t b = end ? (size_t)(end - dir) : len;
        if (doc_dir_is_docs(dir + a, b - a)) {
            cut = a;
        }
        a = b + SKIP_ONE;
    }
    if (cut <= len) {
        len = cut > 0 ? cut - SKIP_ONE : 0; /* the docs folder's parent */
        dir[len] = '\0';
    }
    while (len > 0 && !cbm_ht_has(code_dirs, dir)) {
        const char *up = strrchr(dir, '/');
        len = up ? (size_t)(up - dir) : 0;
        dir[len] = '\0';
    }
    return dir;
}

/* Every folder holding indexed code: the directories (and their ancestors)
 * of the files with terms; keys owned by the table's caller (freed by
 * doc_code_dirs_free). */
static void doc_free_key(const char *key, void *value, void *userdata) {
    (void)value;
    (void)userdata;
    cbm_free(CBM_MEM_CLASS_SEMANTIC, (void *)key);
}

static void doc_code_dirs_free(CBMHashTable *dirs) {
    if (dirs) {
        cbm_ht_foreach(dirs, doc_free_key, NULL);
        cbm_ht_free(dirs);
    }
}

static CBMHashTable *doc_code_dirs(const doc_files_t *df) {
    CBMHashTable *dirs = cbm_ht_create_in(CBM_MEM_CLASS_SEMANTIC, CBM_SZ_256);
    for (int i = 0; dirs && i < df->count; i++) {
        const char *path = df->node[i]->file_path;
        if (df->off[i + SKIP_ONE] == df->off[i]) {
            continue;
        }
        for (const char *p = strchr(path, '/'); p; p = strchr(p + SKIP_ONE, '/')) {
            size_t n = (size_t)(p - path);
            char *key = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, n + SKIP_ONE);
            if (!key) {
                doc_code_dirs_free(dirs);
                return NULL;
            }
            memcpy(key, path, n);
            key[n] = '\0';
            if (cbm_ht_has(dirs, key)) {
                cbm_free(CBM_MEM_CLASS_SEMANTIC, key);
            } else {
                cbm_ht_set(dirs, key, (void *)(intptr_t)SKIP_ONE);
            }
        }
    }
    return dirs;
}

/* A candidate's position: inside the home (or the home folder itself),
 * outside it, or no position (the home is the repository root). */
static cbm_sem_doc_pos_t doc_position(const char *home, const char *path) {
    if (!home || !home[0]) {
        return CBM_SEM_DOC_POS_GLOBAL;
    }
    size_t n = strlen(home);
    return path && strncmp(path, home, n) == 0 && (path[n] == '/' || path[n] == '\0')
               ? CBM_SEM_DOC_POS_LOCAL
               : CBM_SEM_DOC_POS_OUTSIDE;
}

static const char *doc_position_name(cbm_sem_doc_pos_t pos) {
    return pos == CBM_SEM_DOC_POS_LOCAL     ? "local"
           : pos == CBM_SEM_DOC_POS_OUTSIDE ? "outside"
                                            : "global";
}

/* Per section: its doc's home, whether it is its doc's first section (the
 * one a folder candidate starts from), and the Folder nodes by path. */
typedef struct {
    char **home;
    bool *first;
    CBMHashTable *folders; /* folder path (graph-owned) -> Folder node */
} doc_place_t;

static void doc_place_free(doc_place_t *pl, int nsec) {
    for (int s = 0; pl->home && s < nsec; s++) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, pl->home[s]);
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, pl->home);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, pl->first);
    if (pl->folders) {
        cbm_ht_free(pl->folders);
    }
    memset(pl, 0, sizeof(*pl));
}

static bool doc_place_build(const cbm_gbuf_t *gbuf, const cbm_gbuf_node_t **secs, int nsec,
                            const doc_files_t *df, doc_place_t *pl) {
    memset(pl, 0, sizeof(*pl));
    pl->home = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, (size_t)nsec * sizeof(*pl->home));
    pl->first = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, (size_t)nsec * sizeof(*pl->first));
    pl->folders = cbm_ht_create_in(CBM_MEM_CLASS_SEMANTIC, CBM_SZ_256);
    CBMHashTable *firsts = cbm_ht_create_in(CBM_MEM_CLASS_SEMANTIC, CBM_SZ_256);
    CBMHashTable *dirs = doc_code_dirs(df);
    bool ok = pl->home && pl->first && pl->folders && firsts && dirs;
    for (int s = 0; ok && s < nsec; s++) {
        pl->home[s] = doc_home(secs[s]->file_path, dirs);
        ok = pl->home[s] != NULL;
        const char *path = secs[s]->file_path;
        if (ok && path) {
            int prev = (int)(intptr_t)cbm_ht_get(firsts, path) - SKIP_ONE;
            if (prev < 0 || secs[s]->start_line < secs[prev]->start_line) {
                cbm_ht_set(firsts, path, (void *)(intptr_t)(s + SKIP_ONE));
            }
        }
    }
    for (int s = 0; ok && s < nsec; s++) {
        const char *path = secs[s]->file_path;
        pl->first[s] = path && (int)(intptr_t)cbm_ht_get(firsts, path) == s + SKIP_ONE;
    }
    const cbm_gbuf_node_t **folders = NULL;
    int nfold = 0;
    if (ok && cbm_gbuf_find_by_label(gbuf, "Folder", &folders, &nfold) == 0) {
        for (int i = 0; i < nfold; i++) {
            if (folders[i]->file_path && folders[i]->file_path[0]) {
                cbm_ht_set(pl->folders, folders[i]->file_path, (void *)folders[i]);
            }
        }
    }
    if (firsts) {
        cbm_ht_free(firsts);
    }
    doc_code_dirs_free(dirs);
    return ok;
}

typedef struct {
    const cbm_sem_func_t *funcs;
    int func_count;
    const cbm_sem_corpus_t *corpus;
    const cbm_gbuf_node_t **secs;
    int nsec;
    const int *post_off;
    const int *post_f;
    int key_df;
    const float *fnorm;
    const bool *is_test;
    const int *name_off; /* per function: its name tokens' corpus indices */
    const int *name_idx;
    const char *const *fpath; /* per function: its file */
    const char *const *home;  /* per section: its doc's home ("" = the root) */
    const doc_files_t *files;
    int file_key_df;
    doc_result_t *res;
    _Atomic int next;
} doc_ctx_t;

/* Name-field evidence of one hit: the function's name tokens that the
 * section holds (sidx ascending), and the highest idf among them. */
static void doc_name_evidence(const doc_ctx_t *dc, const int *sidx, int ns, doc_hit_t *hit) {
    hit->name_shared = 0;
    hit->name_idf = 0.0F;
    for (int k = dc->name_off[hit->func]; k < dc->name_off[hit->func + SKIP_ONE]; k++) {
        int t = dc->name_idx[k];
        int lo = 0;
        int hi = ns - SKIP_ONE;
        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            if (sidx[mid] == t) {
                float idf = cbm_sem_corpus_idf_at(dc->corpus, t);
                hit->name_shared++;
                hit->name_idf = idf > hit->name_idf ? idf : hit->name_idf;
                break;
            }
            if (sidx[mid] < t) {
                lo = mid + SKIP_ONE;
            } else {
                hi = mid - SKIP_ONE;
            }
        }
    }
}

/* The section's best CBM_SEM_DOC_FILE_K whole files, retrieved by the
 * section's key terms over the files' postings, scored by cosine. */
static void doc_section_files(doc_ctx_t *dc, int s, const int *sidx, const float *sw, int ns,
                              float snorm, int *fstamp, doc_cand_t *fc) {
    const doc_files_t *df = dc->files;
    doc_result_t *r = &dc->res[s];
    int nc = 0;
    for (int k = 0; k < ns && snorm > 0.0F; k++) {
        int t = sidx[k];
        if (df->post_off[t + SKIP_ONE] - df->post_off[t] > dc->file_key_df) {
            continue;
        }
        for (int p = df->post_off[t]; p < df->post_off[t + SKIP_ONE]; p++) {
            int f = df->post_f[p];
            if (fstamp[f] != s) {
                fstamp[f] = s;
                fc[nc++] = (doc_cand_t){.func = f};
            }
        }
    }
    for (int c = 0; c < nc; c++) {
        int f = fc[c].func;
        int len = df->off[f + SKIP_ONE] - df->off[f];
        float dot = doc_terms_dot(sidx, sw, ns, df->idx + df->off[f], df->w + df->off[f], len,
                                  &fc[c].shared);
        float denom = snorm * df->norm[f];
        fc[c].score = denom > 0.0F ? dot / denom : 0.0F;
    }
    if (nc > 1) {
        qsort(fc, (size_t)nc, sizeof(fc[0]), cmp_doc_cand);
    }
    for (int c = 0; c < nc && c < CBM_SEM_DOC_FILE_K; c++) {
        r->files[r->nfiles++] = (doc_file_hit_t){
            .file = fc[c].func, .tfidf = fc[c].score, .rank = c + SKIP_ONE, .shared = fc[c].shared};
    }
}

/* Up to CBM_SEM_DOC_LOCAL_K functions inside the section's home that rank
 * below the top CBM_SEM_DOC_TOP_K, at CBM_SEM_DOC_MIN_SCORE or more (cands
 * sorted best first). */
static void doc_section_local(doc_ctx_t *dc, int s, const doc_cand_t *cands, int nc,
                              const int *sidx, int ns) {
    doc_result_t *r = &dc->res[s];
    const char *home = dc->home[s];
    for (int c = CBM_SEM_DOC_TOP_K; home && home[0] && c < nc && r->nlocal < CBM_SEM_DOC_LOCAL_K;
         c++) {
        if (cands[c].score < CBM_SEM_DOC_MIN_SCORE) {
            break;
        }
        if (doc_position(home, dc->fpath[cands[c].func]) != CBM_SEM_DOC_POS_LOCAL) {
            continue;
        }
        doc_hit_t *hit = &r->local[r->nlocal++];
        *hit = (doc_hit_t){.func = cands[c].func,
                           .tfidf = cands[c].score,
                           .rank = c + SKIP_ONE,
                           .shared = cands[c].shared};
        doc_name_evidence(dc, sidx, ns, hit);
    }
}

static void doc_section_one(doc_ctx_t *dc, int s, int *stamp, doc_cand_t *cands, int *sidx,
                            float *sw, char *body, int *fstamp, doc_cand_t *fc) {
    doc_result_t *r = &dc->res[s];
    r->n = 0;
    r->nlocal = 0;
    r->nfiles = 0;
    if (!doc_section_gated(dc->secs[s], body)) {
        return;
    }
    char *tokens[SEM_DOC_MAX_TOKENS];
    int count = doc_section_tokens(dc->secs[s], body, tokens);
    int ns = cbm_sem_tfidf_terms(dc->corpus, tokens, NULL, count, sidx, sw);
    for (int t = 0; t < count; t++) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, tokens[t]);
    }
    float snorm = terms_norm(sw, ns);
    int nc = 0;
    for (int k = 0; k < ns && snorm > 0.0F; k++) {
        int t = sidx[k];
        if (dc->post_off[t + SKIP_ONE] - dc->post_off[t] > dc->key_df) {
            continue;
        }
        for (int p = dc->post_off[t]; p < dc->post_off[t + SKIP_ONE]; p++) {
            int f = dc->post_f[p];
            if (stamp[f] != s && !dc->is_test[f]) {
                stamp[f] = s;
                cands[nc++] = (doc_cand_t){.func = f};
            }
        }
    }
    for (int c = 0; c < nc; c++) {
        const cbm_sem_func_t *fn = &dc->funcs[cands[c].func];
        float dot = doc_terms_dot(sidx, sw, ns, fn->tfidf_indices, fn->tfidf_weights, fn->tfidf_len,
                                  &cands[c].shared);
        float denom = snorm * dc->fnorm[cands[c].func];
        cands[c].score = denom > 0.0F ? dot / denom : 0.0F;
    }
    if (nc > 1) {
        qsort(cands, (size_t)nc, sizeof(cands[0]), cmp_doc_cand);
    }
    for (int c = 0; c < nc && c < SEM_DOC_TOP_K; c++) {
        doc_hit_t *hit = &r->hits[r->n++];
        *hit = (doc_hit_t){.func = cands[c].func,
                           .tfidf = cands[c].score,
                           .rank = c + SKIP_ONE,
                           .shared = cands[c].shared};
        doc_name_evidence(dc, sidx, ns, hit);
    }
    doc_section_local(dc, s, cands, nc, sidx, ns);
    doc_section_files(dc, s, sidx, sw, ns, snorm, fstamp, fc);
}

static void doc_section_worker(int worker_id, void *ctx_ptr) {
    (void)worker_id;
    doc_ctx_t *dc = ctx_ptr;
    int nf = dc->files->count;
    int *stamp = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)dc->func_count * sizeof(int));
    doc_cand_t *cands =
        cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)dc->func_count * sizeof(doc_cand_t));
    int *fstamp = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, ((size_t)nf + SKIP_ONE) * sizeof(int));
    doc_cand_t *fc = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, ((size_t)nf + SKIP_ONE) * sizeof(*fc));
    int *sidx = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)SEM_DOC_MAX_TOKENS * sizeof(int));
    float *sw = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)SEM_DOC_MAX_TOKENS * sizeof(float));
    char *body = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, SEM_DOC_TEXT_MAX);
    if (stamp && cands && fstamp && fc && sidx && sw && body) {
        for (int f = 0; f < dc->func_count; f++) {
            stamp[f] = -1;
        }
        for (int f = 0; f < nf; f++) {
            fstamp[f] = -1;
        }
        while (true) {
            int s = atomic_fetch_add_explicit(&dc->next, SKIP_ONE, memory_order_relaxed);
            if (s >= dc->nsec) {
                break;
            }
            doc_section_one(dc, s, stamp, cands, sidx, sw, body, fstamp, fc);
        }
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, stamp);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, cands);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, fstamp);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, fc);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, sidx);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, sw);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, body);
}

static const char *const DOC_KIND_NAME[CBM_SEM_DOC_KIND_COUNT] = {"function", "local", "file",
                                                                  "folder"};

/* The doc's home Folder node, for the doc's first section (NULL: another
 * section, or the home is the repository root). */
static const cbm_gbuf_node_t *doc_home_folder(const doc_place_t *pl, int s) {
    return pl->first[s] && pl->home[s][0] ? cbm_ht_get(pl->folders, pl->home[s]) : NULL;
}

static cbm_sem_doc_pos_t doc_target_position(const char *home, const cbm_gbuf_node_t *target,
                                             cbm_sem_doc_kind_t kind) {
    return kind == CBM_SEM_DOC_KIND_FOLDER ? CBM_SEM_DOC_POS_LOCAL
                                           : doc_position(home, target->file_path);
}

/* One CBM_SEM_DOC_SIGNALS line: section and target qualified names, tfidf,
 * rank, shared terms, both locations, the name evidence, then the kind, the
 * position and the doc's home ("." = the repository root). */
static void doc_signal_line(FILE *out, const cbm_gbuf_node_t *sec, const cbm_gbuf_node_t *t,
                            const doc_hit_t *hit, cbm_sem_doc_kind_t kind, const char *home) {
    (void)fprintf(out, "%s\t%s\t%.6f\t%d\t%d\t%s:%d-%d\t%s:%d-%d\t%d\t%.6f\t%s\t%s\t%s\n",
                  sec->qualified_name ? sec->qualified_name : "?",
                  t && t->qualified_name ? t->qualified_name : "?", (double)hit->tfidf, hit->rank,
                  hit->shared, sec->file_path ? sec->file_path : "?", sec->start_line,
                  sec->end_line, t && t->file_path ? t->file_path : "?", t ? t->start_line : 0,
                  t ? t->end_line : 0, hit->name_shared, (double)hit->name_idf, DOC_KIND_NAME[kind],
                  t ? doc_position_name(doc_target_position(home, t, kind)) : "?",
                  home[0] ? home : ".");
}

static void doc_section_lines(FILE *out, const cbm_gbuf_t *gbuf, const doc_ctx_t *dc,
                              const doc_place_t *pl, int s) {
    const cbm_gbuf_node_t *sec = dc->secs[s];
    const doc_result_t *r = &dc->res[s];
    for (int h = 0; h < r->n + r->nlocal; h++) {
        const doc_hit_t *hit = h < r->n ? &r->hits[h] : &r->local[h - r->n];
        const cbm_gbuf_node_t *fn = cbm_gbuf_find_by_id(gbuf, dc->funcs[hit->func].node_id);
        doc_signal_line(out, sec, fn, hit,
                        h < r->n ? CBM_SEM_DOC_KIND_FUNCTION : CBM_SEM_DOC_KIND_LOCAL, pl->home[s]);
    }
    for (int h = 0; h < r->nfiles; h++) {
        const doc_file_hit_t *fh = &r->files[h];
        doc_hit_t hit = {.tfidf = fh->tfidf, .rank = fh->rank, .shared = fh->shared};
        doc_signal_line(out, sec, dc->files->node[fh->file], &hit, CBM_SEM_DOC_KIND_FILE,
                        pl->home[s]);
    }
    const cbm_gbuf_node_t *folder = doc_home_folder(pl, s);
    if (folder) {
        doc_hit_t hit = {.rank = SKIP_ONE};
        doc_signal_line(out, sec, folder, &hit, CBM_SEM_DOC_KIND_FOLDER, pl->home[s]);
    }
}

/* Does the section already link the target exactly (a MENTIONS edge)? A
 * candidate for that pair would only repeat a fact the graph holds. */
static bool doc_pair_linked(const cbm_gbuf_t *gbuf, int64_t sec_id, int64_t fn_id) {
    const cbm_gbuf_edge_t **edges = NULL;
    int n = 0;
    if (cbm_gbuf_find_edges_by_source_type(gbuf, sec_id, "MENTIONS", &edges, &n) != 0) {
        return false;
    }
    for (int i = 0; i < n; i++) {
        if (edges[i]->target_id == fn_id) {
            return true;
        }
    }
    return false;
}

typedef struct {
    cbm_doc_candidate_t *rows;
    int n;
} doc_rows_t;

/* Append the candidate (section -> target) when its judged p is above 0 and
 * the graph does not link the pair exactly; false: out of memory (the row at
 * rw->n may hold partial strings). */
static bool doc_row_add(doc_rows_t *rw, const cbm_gbuf_t *gbuf, const cbm_gbuf_node_t *sec,
                        const cbm_gbuf_node_t *target, const doc_hit_t *hit,
                        cbm_sem_doc_kind_t kind, const char *home) {
    if (!target || !target->qualified_name || !sec->qualified_name) {
        return true;
    }
    cbm_sem_doc_pos_t pos = doc_target_position(home, target, kind);
    cbm_sem_doc_format_t fmt = cbm_sem_doc_format(sec->file_path);
    float p = kind == CBM_SEM_DOC_KIND_FOLDER
                  ? cbm_sem_doc_folder_p(fmt, sec->file_path)
                  : cbm_sem_doc_calibrated_p(fmt, kind, pos, hit->tfidf);
    if (p <= 0.0F || doc_pair_linked(gbuf, sec->id, target->id)) {
        return true; /* not stored for this band, kind or format; or linked exactly */
    }
    char ev[CBM_SZ_128];
    snprintf(ev, sizeof(ev),
             "{\"shared_terms\":%d,\"name_tokens\":%d,\"kind\":\"%s\",\"position\":\"%s\"}",
             hit->shared, hit->name_shared, DOC_KIND_NAME[kind], doc_position_name(pos));
    rw->rows[rw->n] = (cbm_doc_candidate_t){
        .section_qn = cbm_mem_strdup(CBM_MEM_CLASS_STORE, sec->qualified_name),
        .target_qn = cbm_mem_strdup(CBM_MEM_CLASS_STORE, target->qualified_name),
        .rank = hit->rank,
        .score = hit->tfidf,
        .p = cbm_sem_p_2dp(p),
        .evidence = cbm_mem_strdup(CBM_MEM_CLASS_STORE, ev)};
    const cbm_doc_candidate_t *row = &rw->rows[rw->n];
    if (!row->section_qn || !row->target_qn || !row->evidence) {
        return false;
    }
    rw->n++;
    return true;
}

static bool doc_section_rows(doc_rows_t *rw, const cbm_gbuf_t *gbuf, const doc_ctx_t *dc,
                             const doc_place_t *pl, int s) {
    const cbm_gbuf_node_t *sec = dc->secs[s];
    const doc_result_t *r = &dc->res[s];
    bool ok = true;
    for (int h = 0; ok && h < r->n + r->nlocal; h++) {
        const doc_hit_t *hit = h < r->n ? &r->hits[h] : &r->local[h - r->n];
        if (h < r->n && (hit->rank > CBM_SEM_DOC_TOP_K || hit->tfidf < CBM_SEM_DOC_MIN_SCORE)) {
            continue;
        }
        ok =
            doc_row_add(rw, gbuf, sec, cbm_gbuf_find_by_id(gbuf, dc->funcs[hit->func].node_id), hit,
                        h < r->n ? CBM_SEM_DOC_KIND_FUNCTION : CBM_SEM_DOC_KIND_LOCAL, pl->home[s]);
    }
    for (int h = 0; ok && h < r->nfiles; h++) {
        const doc_file_hit_t *fh = &r->files[h];
        doc_hit_t hit = {.tfidf = fh->tfidf, .rank = fh->rank, .shared = fh->shared};
        ok = doc_row_add(rw, gbuf, sec, dc->files->node[fh->file], &hit, CBM_SEM_DOC_KIND_FILE,
                         pl->home[s]);
    }
    doc_hit_t first = {.rank = SKIP_ONE};
    return ok && doc_row_add(rw, gbuf, sec, doc_home_folder(pl, s), &first, CBM_SEM_DOC_KIND_FOLDER,
                             pl->home[s]);
}

/* The stored candidates of every section -- its best CBM_SEM_DOC_TOP_K
 * functions at CBM_SEM_DOC_MIN_SCORE or more, its local functions below them,
 * its best files, and (for a doc's first section) the doc's home folder --
 * each with its judged p, in CBM_MEM_CLASS_STORE for
 * cbm_store_doc_candidates_free; p at two decimals, as on
 * SEMANTICALLY_RELATED edges. */
static cbm_doc_candidate_t *doc_candidate_rows(const cbm_gbuf_t *gbuf, const doc_ctx_t *dc,
                                               const doc_place_t *pl, int *out_count) {
    int n = 0;
    for (int s = 0; s < dc->nsec; s++) {
        const doc_result_t *r = &dc->res[s];
        n += (r->n < CBM_SEM_DOC_TOP_K ? r->n : CBM_SEM_DOC_TOP_K) + r->nlocal + r->nfiles +
             SKIP_ONE;
    }
    *out_count = 0;
    doc_rows_t rw = {.rows = n > 0 ? cbm_calloc(CBM_MEM_CLASS_STORE, (size_t)n * sizeof(*rw.rows))
                                   : NULL};
    if (!rw.rows) {
        return NULL;
    }
    for (int s = 0; s < dc->nsec; s++) {
        if (!doc_section_rows(&rw, gbuf, dc, pl, s)) {
            cbm_store_doc_candidates_free(rw.rows, rw.n + SKIP_ONE);
            return NULL;
        }
    }
    *out_count = rw.n;
    return rw.rows;
}

static void phase6c_doc_sections(cbm_pipeline_ctx_t *ctx, const cbm_sem_func_t *funcs,
                                 int func_count, const cbm_sem_corpus_t *corpus, int worker_count) {
    const cbm_gbuf_t *gbuf = ctx->gbuf;
    if (!corpus || func_count <= 0) {
        return;
    }
    const cbm_gbuf_node_t **found = NULL;
    int nsec = 0;
    if (cbm_gbuf_find_by_label(gbuf, "Section", &found, &nsec) != 0 || nsec == 0) {
        return;
    }
    int nterms = cbm_sem_corpus_token_count(corpus);
    const cbm_gbuf_node_t **secs = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)nsec * sizeof(*secs));
    int *post_off = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, ((size_t)nterms + SKIP_ONE) * sizeof(int));
    float *fnorm = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)func_count * sizeof(float));
    bool *is_test = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)func_count * sizeof(bool));
    int *name_off =
        cbm_calloc(CBM_MEM_CLASS_SEMANTIC, ((size_t)func_count + SKIP_ONE) * sizeof(int));
    int *name_idx = cbm_alloc(CBM_MEM_CLASS_SEMANTIC,
                              ((size_t)func_count * SEM_DOC_NAME_TOKENS + SKIP_ONE) * sizeof(int));
    doc_result_t *res = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, (size_t)nsec * sizeof(doc_result_t));
    const char **fn_paths =
        cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)func_count * sizeof(*fn_paths));
    int *ffile = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)func_count * sizeof(int));
    doc_files_t files = {0};
    doc_place_t place = {0};
    int *post_f = NULL;
    int *cursor = NULL;
    FILE *out = NULL;
    if (secs && post_off && fnorm && is_test && name_off && name_idx && res && fn_paths && ffile) {
        for (int f = 0; f < func_count; f++) {
            for (int k = 0; k < funcs[f].tfidf_len; k++) {
                post_off[funcs[f].tfidf_indices[k] + SKIP_ONE]++;
            }
            fnorm[f] = terms_norm(funcs[f].tfidf_weights, funcs[f].tfidf_len);
            const cbm_gbuf_node_t *fn = cbm_gbuf_find_by_id(gbuf, funcs[f].node_id);
            is_test[f] = doc_is_test_function(fn);
            fn_paths[f] = fn && fn->file_path ? fn->file_path : "";
            char *tok[SEM_DOC_NAME_TOKENS];
            int nt = fn && fn->name ? cbm_sem_tokenize(fn->name, tok, SEM_DOC_NAME_TOKENS) : 0;
            int base = name_off[f];
            int m = 0;
            for (int t = 0; t < nt; t++) {
                int idx = cbm_sem_corpus_token_index(corpus, tok[t]);
                bool dup = false;
                for (int u = 0; u < m; u++) {
                    dup = dup || name_idx[base + u] == idx;
                }
                if (idx >= 0 && !dup) {
                    name_idx[base + m++] = idx;
                }
                cbm_free(CBM_MEM_CLASS_SEMANTIC, tok[t]);
            }
            name_off[f + SKIP_ONE] = base + m;
        }
        for (int t = 0; t < nterms; t++) {
            post_off[t + SKIP_ONE] += post_off[t];
        }
        post_f =
            cbm_alloc(CBM_MEM_CLASS_SEMANTIC, ((size_t)post_off[nterms] + SKIP_ONE) * sizeof(int));
        cursor = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, ((size_t)nterms + SKIP_ONE) * sizeof(int));
    }
    if (post_f && cursor) {
        for (int f = 0; f < func_count; f++) {
            for (int k = 0; k < funcs[f].tfidf_len; k++) {
                int t = funcs[f].tfidf_indices[k];
                post_f[post_off[t] + cursor[t]++] = f;
            }
        }
        memcpy(secs, found, (size_t)nsec * sizeof(*secs));
        qsort(secs, (size_t)nsec, sizeof(*secs), cmp_section_node);
    }
    bool placed =
        post_f && cursor &&
        doc_files_build(gbuf, funcs, func_count, is_test, corpus, nterms, &files, ffile) &&
        doc_place_build(gbuf, secs, nsec, &files, &place);
    if (placed) {
        int key_df = func_count / SEM_DOC_KEY_DF_DIV;
        if (key_df < SEM_DOC_KEY_DF_MIN) {
            key_df = SEM_DOC_KEY_DF_MIN;
        }
        int file_key_df = files.count / SEM_DOC_KEY_DF_DIV;
        if (file_key_df < SEM_DOC_KEY_DF_MIN) {
            file_key_df = SEM_DOC_KEY_DF_MIN;
        }
        doc_ctx_t dc = {.funcs = funcs,
                        .func_count = func_count,
                        .corpus = corpus,
                        .secs = secs,
                        .nsec = nsec,
                        .post_off = post_off,
                        .post_f = post_f,
                        .key_df = key_df,
                        .fnorm = fnorm,
                        .is_test = is_test,
                        .name_off = name_off,
                        .name_idx = name_idx,
                        .fpath = fn_paths,
                        .home = (const char *const *)place.home,
                        .files = &files,
                        .file_key_df = file_key_df,
                        .res = res};
        atomic_init(&dc.next, 0);
        cbm_parallel_for_opts_t opts = {.max_workers = worker_count, .force_pthreads = false};
        cbm_parallel_for(worker_count, doc_section_worker, &dc, opts);
        int row_count = 0;
        cbm_doc_candidate_t *rows = doc_candidate_rows(gbuf, &dc, &place, &row_count);
        cbm_pipeline_set_doc_candidates(ctx->pipeline, rows, row_count);
        cbm_log_info("pass.semantic.doc_candidates", "sections", itoa_log(nsec), "files",
                     itoa_log(files.count), "rows", itoa_log(row_count));
        char path[CBM_SZ_1K];
        out = cbm_safe_getenv("CBM_SEM_DOC_SIGNALS", path, sizeof(path), NULL)
                  ? cbm_fopen(path, "w")
                  : NULL;
        for (int s = 0; out && s < nsec; s++) {
            doc_section_lines(out, gbuf, &dc, &place, s);
        }
        /* CBM_SEM_DOC_FUNCS=<path>: every function with its location and the
         * test flag, for measuring candidates the TF-IDF generator does not
         * produce (judged samples only). */
        char fpath[CBM_SZ_1K];
        FILE *fout = cbm_safe_getenv("CBM_SEM_DOC_FUNCS", fpath, sizeof(fpath), NULL)
                         ? cbm_fopen(fpath, "w")
                         : NULL;
        for (int f = 0; fout && f < func_count; f++) {
            const cbm_gbuf_node_t *fn = cbm_gbuf_find_by_id(gbuf, funcs[f].node_id);
            (void)fprintf(fout, "%s\t%s:%d-%d\t%d\n",
                          fn && fn->qualified_name ? fn->qualified_name : "?",
                          fn && fn->file_path ? fn->file_path : "?", fn ? fn->start_line : 0,
                          fn ? fn->end_line : 0, is_test[f] ? 1 : 0);
        }
        if (fout) {
            (void)fclose(fout);
        }
    }
    if (out) {
        (void)fclose(out);
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, cursor);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, post_f);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, secs);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, post_off);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, fnorm);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, is_test);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, name_off);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, name_idx);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, res);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, fn_paths);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, ffile);
    doc_place_free(&place, nsec);
    doc_files_free(&files);
}

int cbm_pipeline_pass_semantic_edges(cbm_pipeline_ctx_t *ctx) {
    /* Controlled by pipeline mode (moderate/full), not env var */
    cbm_log_info("pass.start", "pass", "semantic_edges");

    cbm_gbuf_t *gbuf = ctx->gbuf;
    cbm_sem_config_t cfg = cbm_sem_get_config();

    CBM_PROF_START(t_phase1a);
    cbm_sem_func_t *funcs = NULL;
    const cbm_gbuf_node_t **node_ptrs = NULL;
    int func_count = phase1_scan_functions(gbuf, &funcs, &node_ptrs);
    CBM_PROF_END_N("semantic_edges", "1a_scan_seq", t_phase1a, func_count);

    /* Phase 1b: Decode minhash + profile + build api/type/deco vectors (PARALLEL). */
    cbm_sem_ensure_ready();
    CBM_PROF_START(t_phase1b);
    phase1b_decode_and_build(funcs, node_ptrs, gbuf, func_count, cbm_default_worker_count(false));
    CBM_PROF_END_N("semantic_edges", "1b_decode_build_parallel", t_phase1b, func_count);
    cbm_log_info("pass.semantic.collected", "functions", itoa_log(func_count));

    if (func_count < PSE_MIN_FUNCS_FOR_PAIR) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, funcs);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, node_ptrs);
        cbm_log_info("pass.done", "pass", "semantic_edges", "edges", "0");
        return 0;
    }

    sem_mem_mark("1b_decode_build");

    /* Phase 2: Tokenize all nodes (PARALLEL) */
    int worker_count = cbm_default_worker_count(false);
    char **all_tokens = NULL;   /* packed by phase2_tokenize */
    size_t *tok_offsets = NULL; /* per function: index into all_tokens */
    int *token_counts =
        cbm_calloc(CBM_MEM_CLASS_SEMANTIC, (size_t)((size_t)func_count) * (sizeof(int)));

    CBMHashTable **token_pools = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, (size_t)((size_t)worker_count) *
                                                                        (sizeof(CBMHashTable *)));
    if (token_pools) {
        for (int w = 0; w < worker_count; w++) {
            token_pools[w] = cbm_ht_create(CBM_SZ_1K);
        }
    }
    cbm_sem_corpus_t *corpus = NULL;
    int batch = sem_batch_size(func_count);
    if (batch < func_count) {
        corpus = run_token_phases_batched(gbuf, funcs, node_ptrs, token_counts, func_count, batch,
                                          worker_count, token_pools);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, node_ptrs);
    } else {
        CBM_PROF_START(t_phase2);
        phase2_tokenize(node_ptrs, gbuf, &all_tokens, &tok_offsets, token_counts, func_count,
                        worker_count, token_pools);
        CBM_PROF_END_N("semantic_edges", "2_tokenize_parallel", t_phase2, func_count);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, node_ptrs);
        sem_mem_mark("2_tokenize");

        /* Phase 3: Build corpus (batch add), finalize, export enriched token vectors. */
        corpus = run_corpus_phase(gbuf, all_tokens, tok_offsets, token_counts, func_count);
        sem_mem_mark("3_corpus");

        /* Phase 4: Build per-function TF-IDF + RI vectors (PARALLEL) and store them. */
        CBM_PROF_START(t_phase4);
        phase4_build_and_store_vectors(gbuf, funcs, all_tokens, tok_offsets, token_counts, corpus,
                                       func_count, worker_count);
        CBM_PROF_END_N("semantic_edges", "4_build_and_store_vec", t_phase4, func_count);
        sem_mem_mark("4_vectors");
    }

    cbm_log_info("pass.semantic.vectors_stored", "count", itoa_log(func_count));

    /* Phase 5: LSH hyperplanes → signatures → buckets. */
    CBM_PROF_START(t_phase5);
    uint64_t *signatures = NULL;
    sem_bucket_t **band_buckets = NULL;
    phase5_lsh_build(funcs, func_count, worker_count, &signatures, &band_buckets);
    CBM_PROF_END_N("semantic_edges", "5_lsh_build", t_phase5, func_count);

    cbm_log_info("pass.semantic.lsh_built", "functions", itoa_log(func_count), "bands",
                 itoa_log(SEM_LSH_BANDS));
    sem_mem_mark("5_lsh");

    /* Phase 6: Parallel scoring + sequential edge merge. */
    int total_edges =
        run_scoring_phase(gbuf, funcs, signatures, band_buckets, cfg, func_count, worker_count);

    sem_mem_mark("6_score");

    /* Phase 6c: doc sections -> function candidates (doc_link_candidates). */
    phase6c_doc_sections(ctx, funcs, func_count, corpus, worker_count);

    /* Phase 7: Cleanup */
    CBM_PROF_START(t_phase7);
    free_lsh_buckets(band_buckets);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, signatures);
    cbm_log_info("pass.done", "pass", "semantic_edges", "edges", itoa_log(total_edges));
    free_funcs_and_tokens(funcs, func_count, all_tokens, tok_offsets, token_counts, token_pools,
                          worker_count);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, token_counts);
    cbm_sem_corpus_free(corpus);
    CBM_PROF_END("semantic_edges", "7_cleanup", t_phase7);

    return 0;
}
