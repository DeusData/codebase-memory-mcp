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
#define XXH_INLINE_ALL
#include "xxhash/xxhash.h"

#include "pipeline/worker_pool.h"
#include "foundation/platform.h"
#include "foundation/profile.h"

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
                              bool same_file, int i, int j, int c) {
    if (buf->count >= buf->cap) {
        int nc = buf->cap < CBM_SZ_256 ? CBM_SZ_256 : buf->cap * GROW;
        deferred_edge_t *grown = realloc(buf->edges, (size_t)nc * sizeof(deferred_edge_t));
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
                                                 .i = i,
                                                 .j = j,
                                                 .c = c};
}

static void deferred_buf_free(deferred_edge_buf_t *buf) {
    free(buf->edges);
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
    tokens[count] = strdup(text);
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
static int inject_calls_pattern_tokens(const cbm_gbuf_node_t *n, const cbm_gbuf_t *gbuf,
                                       char **tokens, int count, int max_tokens) {
    if (!gbuf) {
        return count;
    }
    const cbm_gbuf_edge_t **edges = NULL;
    int ec = 0;
    if (cbm_gbuf_find_edges_by_source_type(gbuf, n->id, "CALLS", &edges, &ec) != 0) {
        return count;
    }
    for (int e = 0; e < ec && count < max_tokens; e++) {
        const cbm_gbuf_node_t *t = cbm_gbuf_find_by_id(gbuf, edges[e]->target_id);
        if (!t || !t->name) {
            continue;
        }
        count = inject_callee_tokens(t->name, tokens, count, max_tokens);
    }
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

/* Extract a JSON string value by key (simple strstr-based, no full parse). */
static const char *json_str_value(const char *json, const char *key, char *buf, int bufsize) {
    if (!json || !key) {
        return NULL;
    }
    char search[CBM_SZ_64];
    snprintf(search, sizeof(search), "\"%s\":\"", key);
    const char *start = strstr(json, search);
    if (!start) {
        return NULL;
    }
    start += strlen(search);
    const char *end = strchr(start, '"');
    if (!end) {
        return NULL;
    }
    int len = (int)(end - start);
    if (len >= bufsize) {
        len = bufsize - SKIP_ONE;
    }
    memcpy(buf, start, (size_t)len);
    buf[len] = '\0';
    return buf;
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
            out[count] = malloc((size_t)len + SKIP_ONE);
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
        free(arr[p]);
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
    const char **names = malloc((size_t)ec * sizeof(char *));
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
    free((void *)names);
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
    free((void *)names);
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
        free(ptypes[i]);
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
        free(decos[i]);
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

typedef struct {
    const cbm_gbuf_node_t **node_ptrs; /* node pointer per function index */
    cbm_gbuf_t *gbuf;                  /* read-only during tokenization */
    char **all_tokens;                 /* output: all_tokens[f * MAX + t] */
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
        /* Write directly into the shared buffer slice for this function — the
         * strdup'd tokens are owned by all_tokens[] from the moment they land
         * in this slot, which avoids a spurious analyzer "leak" diagnostic on
         * the previous stack-local relay pattern. */
        char **dst = &tc->all_tokens[(ptrdiff_t)f * CBM_SEM_MAX_TOKENS];
        int count = tokenize_node(n, tc->gbuf, dst, CBM_SEM_MAX_TOKENS);
        count = inject_pattern_tokens(n, tc->gbuf, dst, count, CBM_SEM_MAX_TOKENS);
        if (tc->pools && tc->pools[worker_id]) {
            CBMHashTable *pool = tc->pools[worker_id];
            for (int t = 0; t < count; t++) {
                char *canon = cbm_ht_get(pool, dst[t]);
                if (canon) {
                    free(dst[t]);
                    dst[t] = canon;
                } else {
                    cbm_ht_set(pool, dst[t], dst[t]); /* key borrows the value */
                }
            }
        }
        tc->token_counts[f] = count;
    }
}

/* ── Parallel Phase 4: Build per-function vectors ────────────────── */

typedef struct {
    cbm_sem_func_t *funcs;
    char **all_tokens;
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
        char **tokens = &vc->all_tokens[(ptrdiff_t)f * CBM_SEM_MAX_TOKENS];

        /* TF-IDF weights */
        int *indices = malloc((size_t)tc * sizeof(int));
        float *weights = malloc((size_t)tc * sizeof(float));
        int tfidf_len = 0;
        for (int t = 0; t < tc; t++) {
            float idf = cbm_sem_corpus_idf(vc->corpus, tokens[t]);
            if (idf > 0.0F) {
                indices[tfidf_len] = t;
                weights[tfidf_len] = idf;
                tfidf_len++;
            }
        }
        vc->funcs[f].tfidf_indices = indices;
        vc->funcs[f].tfidf_weights = weights;
        vc->funcs[f].tfidf_len = tfidf_len;

        /* RI vector: sum of enriched token vectors weighted by IDF. Built in a
         * LOCAL dense buffer; only the quantized code is retained per func
         * (the resident dense floats were ~9.4 GB on the kernel). */
        cbm_sem_vec_t ri_dense;
        memset(&ri_dense, 0, sizeof(cbm_sem_vec_t));
        for (int t = 0; t < tc; t++) {
            const cbm_sem_vec_t *ri = cbm_sem_corpus_ri_vec(vc->corpus, tokens[t]);
            if (ri) {
                float idf = cbm_sem_corpus_idf(vc->corpus, tokens[t]);
                cbm_sem_vec_add_scaled(&ri_dense, ri, idf);
            }
        }
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

/* Check whether `j` has already been recorded in the open-addressed `seen`
 * set for this function; insert it if not.  Returns true when the insertion
 * was fresh (caller should add to candidates). */
static bool score_seen_insert(int *seen, int j) {
    uint32_t slot = (uint32_t)j & SCORE_SEEN_MASK;
    for (int p = 0; p < SCORE_SEEN_CAP; p++) {
        uint32_t idx = (slot + (uint32_t)p) & SCORE_SEEN_MASK;
        if (seen[idx] == SCORE_SEEN_EMPTY) {
            seen[idx] = j;
            return true;
        }
        if (seen[idx] == j) {
            return false;
        }
    }
    return false;
}

/* Collect the unique candidate function indices for node `i` by iterating
 * every LSH band and merging bucket members via `seen[]`.  Returns the
 * populated candidate count. */
static int score_collect_candidates(score_ctx_t *sc, int i, int *seen, int *candidates,
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
    if (score < sc->cfg.threshold) {
        return;
    }
    bool same_file = sc->funcs[i].file_path && sc->funcs[j].file_path &&
                     strcmp(sc->funcs[i].file_path, sc->funcs[j].file_path) == 0;
    deferred_buf_push(my_buf, sc->funcs[i].node_id, sc->funcs[j].node_id, score, same_file, i, j,
                      c);
}

static void score_worker(int worker_id, void *ctx_ptr) {
    score_ctx_t *sc = ctx_ptr;
    deferred_edge_buf_t *my_buf = &sc->worker_bufs[worker_id];

    while (true) {
        int i = atomic_fetch_add_explicit(&sc->next_idx, SKIP_ONE, memory_order_relaxed);
        if (i >= sc->func_count) {
            break;
        }
        int seen[SCORE_SEEN_CAP];
        for (int s = 0; s < SCORE_SEEN_CAP; s++) {
            seen[s] = SCORE_SEEN_EMPTY;
        }
        int candidates[SEM_MAX_CANDIDATES];
        int cand_count = score_collect_candidates(sc, i, seen, candidates, SEM_MAX_CANDIDATES);
        for (int c = 0; c < cand_count; c++) {
            score_try_emit(sc, i, candidates[c], c, my_buf);
        }
    }
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
                cbm_sem_func_t *grown = realloc(funcs, (size_t)new_cap * sizeof(cbm_sem_func_t));
                if (!grown) {
                    break;
                }
                funcs = grown;
                const cbm_gbuf_node_t **np_grown =
                    realloc(node_ptrs, (size_t)new_cap * sizeof(cbm_gbuf_node_t *));
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
     * filled in later phases, so no 12.7 KB structs are moved. */
    qsort(node_ptrs, (size_t)func_count, sizeof(node_ptrs[0]), cmp_node_ptr_by_qn);
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
                int *ni = realloc(bucket->items, (size_t)nc * sizeof(int));
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
static int phase6b_merge_edges(cbm_gbuf_t *gbuf, deferred_edge_buf_t *worker_bufs, int worker_count,
                               int *edge_counts, int max_edges) {
    int total_pairs = 0;
    for (int w = 0; w < worker_count; w++) {
        total_pairs += worker_bufs[w].count;
    }
    deferred_edge_t *pairs = NULL;
    if (total_pairs > 0) {
        pairs = malloc((size_t)total_pairs * sizeof(deferred_edge_t));
    }
    if (!pairs) {
        for (int w = 0; w < worker_count; w++) {
            deferred_buf_free(&worker_bufs[w]);
        }
        return 0;
    }
    int n = 0;
    for (int w = 0; w < worker_count; w++) {
        memcpy(&pairs[n], worker_bufs[w].edges,
               (size_t)worker_bufs[w].count * sizeof(deferred_edge_t));
        n += worker_bufs[w].count;
        deferred_buf_free(&worker_bufs[w]);
    }
    qsort(pairs, (size_t)n, sizeof(deferred_edge_t), cmp_deferred_edge_canonical);

    int total_edges = 0;
    for (int e = 0; e < n; e++) {
        deferred_edge_t *de = &pairs[e];
        if (edge_counts[de->i] >= max_edges || edge_counts[de->j] >= max_edges) {
            continue;
        }
        char props[PROPS_BUF];
        snprintf(props, sizeof(props), "{\"score\":%.3f,\"same_file\":%s}", de->score,
                 de->same_file ? "true" : "false");
        cbm_gbuf_insert_edge(gbuf, de->source_id, de->target_id, "SEMANTICALLY_RELATED", props);
        edge_counts[de->i]++;
        edge_counts[de->j]++;
        total_edges++;
    }
    free(pairs);
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
    hyperplane_row_t *hyperplanes = malloc(sizeof(hyperplane_row_t) * NUM_HYPERPLANES);
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
static void phase2_tokenize(const cbm_gbuf_node_t **node_ptrs, cbm_gbuf_t *gbuf, char **all_tokens,
                            int *token_counts, int func_count, int worker_count,
                            CBMHashTable **pools) {
    tokenize_ctx_t tc = {
        .node_ptrs = node_ptrs,
        .gbuf = gbuf,
        .all_tokens = all_tokens,
        .token_counts = token_counts,
        .func_count = func_count,
        .pools = pools,
    };
    atomic_init(&tc.next_idx, 0);
    cbm_parallel_for_opts_t opts = {.max_workers = worker_count, .force_pthreads = false};
    cbm_parallel_for(worker_count, tokenize_worker, &tc, opts);
}

/* Phase 4a: build per-function TF-IDF + RI vectors in parallel, producing
 * int8-quantized qvecs for subsequent storage.  Phase 4b runs sequentially
 * to store them in gbuf because gbuf is not thread-safe. */
static void phase4_build_and_store_vectors(cbm_gbuf_t *gbuf, cbm_sem_func_t *funcs,
                                           char **all_tokens, int *token_counts,
                                           cbm_sem_corpus_t *corpus, int func_count,
                                           int worker_count) {
    uint8_t *qvecs = malloc((size_t)func_count * CBM_SEM_DIM);
    if (!qvecs) {
        return;
    }
    vec_build_ctx_t vc = {
        .funcs = funcs,
        .all_tokens = all_tokens,
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
    free(qvecs);
}

/* Phase 5: hyperplane generation → signatures → LSH bucket population.
 * Returns the malloc'd signatures array and band_buckets[] via out-params;
 * caller frees both. */
static void phase5_lsh_build(cbm_sem_func_t *funcs, int func_count, int worker_count,
                             uint64_t **out_signatures, sem_bucket_t ***out_buckets) {
    hyperplane_row_t *hyperplanes = phase5a_build_hyperplanes();
    uint64_t *signatures = calloc((size_t)func_count, sizeof(uint64_t));
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
    free(hyperplanes);

    sem_bucket_t **band_buckets = calloc(SEM_LSH_BANDS, sizeof(sem_bucket_t *));
    if (band_buckets) {
        for (int b = 0; b < SEM_LSH_BANDS; b++) {
            band_buckets[b] = calloc(SEM_BUCKET_COUNT, sizeof(sem_bucket_t));
        }
        phase5c_build_lsh_buckets(signatures, func_count, band_buckets);
    }
    *out_signatures = signatures;
    *out_buckets = band_buckets;
}

/* Phase 6a: score candidate pairs in parallel and collect deferred edges. */
static void phase6a_score_candidates(cbm_sem_func_t *funcs, uint64_t *signatures, int *edge_counts,
                                     sem_bucket_t **band_buckets, cbm_sem_config_t cfg,
                                     deferred_edge_buf_t *worker_bufs, int func_count,
                                     int worker_count) {
    score_ctx_t sc = {
        .funcs = funcs,
        .signatures = signatures,
        .edge_counts = edge_counts,
        .cfg = cfg,
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
            free(band_buckets[b][h].items);
        }
        free(band_buckets[b]);
    }
    free(band_buckets);
}

/* Phases 3a/3b/3c bundled: create corpus, batch-add docs, finalize, export
 * enriched token vectors to the graph buffer.  Returns the new corpus, which
 * the caller must cbm_sem_corpus_free() later. */
static cbm_sem_corpus_t *run_corpus_phase(cbm_gbuf_t *gbuf, char **all_tokens, int *token_counts,
                                          int func_count) {
    CBM_PROF_START(t_phase3a);
    cbm_sem_corpus_t *corpus = cbm_sem_corpus_new();
    cbm_sem_corpus_add_docs_batch(corpus, all_tokens, token_counts, func_count, CBM_SEM_MAX_TOKENS);
    CBM_PROF_END_N("semantic_edges", "3a_corpus_batch", t_phase3a, func_count);

    CBM_PROF_START(t_phase3b);
    cbm_sem_corpus_finalize(corpus);
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
    int *edge_counts = calloc((size_t)func_count, sizeof(int));
    deferred_edge_buf_t *worker_bufs = calloc((size_t)worker_count, sizeof(deferred_edge_buf_t));
    if (!edge_counts || !worker_bufs) {
        free(edge_counts);
        free(worker_bufs);
        return 0;
    }
    for (int w = 0; w < worker_count; w++) {
        deferred_buf_init(&worker_bufs[w]);
    }

    CBM_PROF_START(t_phase6a);
    phase6a_score_candidates(funcs, signatures, edge_counts, band_buckets, cfg, worker_bufs,
                             func_count, worker_count);
    CBM_PROF_END_N("semantic_edges", "6a_score_parallel", t_phase6a, func_count);

    CBM_PROF_START(t_phase6b);
    int total = phase6b_merge_edges(gbuf, worker_bufs, worker_count, edge_counts, cfg.max_edges);
    CBM_PROF_END_N("semantic_edges", "6b_edge_merge_seq", t_phase6b, total);

    free(worker_bufs);
    free(edge_counts);
    return total;
}

/* Free the per-function arrays malloc'd during phases 1b and 4a. */
static void free_token_pool_entry(const char *key, void *value, void *ud) {
    (void)key; /* key == value: one owned string per unique token */
    (void)ud;
    free(value);
}

/* all_tokens slots BORROW their strings from the per-worker intern pools;
 * the pools own exactly one copy per unique token per worker. */
static void free_funcs_and_tokens(cbm_sem_func_t *funcs, int func_count, char **all_tokens,
                                  const int *token_counts, CBMHashTable **pools, int worker_count) {
    (void)token_counts;
    for (int f = 0; f < func_count; f++) {
        free(funcs[f].tfidf_indices);
        free(funcs[f].tfidf_weights);
    }
    free(all_tokens);
    free(funcs);
    if (pools) {
        for (int w = 0; w < worker_count; w++) {
            if (pools[w]) {
                cbm_ht_foreach(pools[w], free_token_pool_entry, NULL);
                cbm_ht_free(pools[w]);
            }
        }
        free(pools);
    }
}

/* ── Pass entry point ────────────────────────────────────────────── */

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
        free(funcs);
        free(node_ptrs);
        cbm_log_info("pass.done", "pass", "semantic_edges", "edges", "0");
        return 0;
    }

    /* Phase 2: Tokenize all nodes (PARALLEL) */
    int worker_count = cbm_default_worker_count(false);
    char **all_tokens = malloc((size_t)func_count * sizeof(char *) * CBM_SEM_MAX_TOKENS);
    int *token_counts = calloc((size_t)func_count, sizeof(int));

    CBM_PROF_START(t_phase2);
    CBMHashTable **token_pools = calloc((size_t)worker_count, sizeof(CBMHashTable *));
    if (token_pools) {
        for (int w = 0; w < worker_count; w++) {
            token_pools[w] = cbm_ht_create(CBM_SZ_1K);
        }
    }
    phase2_tokenize(node_ptrs, gbuf, all_tokens, token_counts, func_count, worker_count,
                    token_pools);
    CBM_PROF_END_N("semantic_edges", "2_tokenize_parallel", t_phase2, func_count);
    free(node_ptrs);

    /* Phase 3: Build corpus (batch add), finalize, export enriched token vectors. */
    cbm_sem_corpus_t *corpus = run_corpus_phase(gbuf, all_tokens, token_counts, func_count);

    /* Phase 4: Build per-function TF-IDF + RI vectors (PARALLEL) and store them. */
    CBM_PROF_START(t_phase4);
    phase4_build_and_store_vectors(gbuf, funcs, all_tokens, token_counts, corpus, func_count,
                                   worker_count);
    CBM_PROF_END_N("semantic_edges", "4_build_and_store_vec", t_phase4, func_count);

    cbm_log_info("pass.semantic.vectors_stored", "count", itoa_log(func_count));

    /* Phase 5: LSH hyperplanes → signatures → buckets. */
    CBM_PROF_START(t_phase5);
    uint64_t *signatures = NULL;
    sem_bucket_t **band_buckets = NULL;
    phase5_lsh_build(funcs, func_count, worker_count, &signatures, &band_buckets);
    CBM_PROF_END_N("semantic_edges", "5_lsh_build", t_phase5, func_count);

    cbm_log_info("pass.semantic.lsh_built", "functions", itoa_log(func_count), "bands",
                 itoa_log(SEM_LSH_BANDS));

    /* Phase 6: Parallel scoring + sequential edge merge. */
    int total_edges =
        run_scoring_phase(gbuf, funcs, signatures, band_buckets, cfg, func_count, worker_count);

    /* Phase 7: Cleanup */
    CBM_PROF_START(t_phase7);
    free_lsh_buckets(band_buckets);
    free(signatures);
    cbm_log_info("pass.done", "pass", "semantic_edges", "edges", itoa_log(total_edges));
    free_funcs_and_tokens(funcs, func_count, all_tokens, token_counts, token_pools, worker_count);
    free(token_counts);
    cbm_sem_corpus_free(corpus);
    CBM_PROF_END("semantic_edges", "7_cleanup", t_phase7);

    return 0;
}
