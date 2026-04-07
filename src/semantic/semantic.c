/*
 * semantic.c — Algorithmic code embeddings: TF-IDF, Random Indexing,
 * API/Type/Decorator signatures, combined scoring, graph diffusion.
 *
 * All signals computed from graph buffer metadata — no source file reads.
 * Uses xxHash for deterministic random vectors. Pure C, zero dependencies.
 */
#include "semantic/semantic.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "foundation/profile.h"
#include "foundation/platform.h"
#include "foundation/compat_thread.h"
#include "pipeline/worker_pool.h"
#include "simhash/minhash.h"
#include "nomic/code_vectors.h"

#define XXH_INLINE_ALL
#include "xxhash/xxhash.h"

#include <ctype.h>
#include <math.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Constants ───────────────────────────────────────────────────── */

enum {
    TOKEN_BUF_LEN = 128,
    CORPUS_INIT_CAP = 4096,
    DOC_TOKENS_INIT = 64,
    RI_SEED_BASE = 0x52494E44, /* "RIND" */
};

/* Default signal weights for cbm_sem_combined_score.  Must sum to ~1.0;
 * proximity is a multiplier applied on top. */
#define CBM_SEM_W_TFIDF 0.20F
#define CBM_SEM_W_RI 0.25F
#define CBM_SEM_W_MINHASH 0.10F
#define CBM_SEM_W_API 0.15F
#define CBM_SEM_W_TYPE 0.10F
#define CBM_SEM_W_DECORATOR 0.05F
#define CBM_SEM_W_STRUCT_PROFILE 0.10F
#define CBM_SEM_W_DATAFLOW 0.05F

/* Threshold bounds for CBM_SEMANTIC_THRESHOLD env override. */
#define CBM_SEM_THRESHOLD_MIN 0.0F
#define CBM_SEM_THRESHOLD_MAX 1.0F

/* Epsilon for denominator guards in cosine math. */
#define CBM_SEM_DENOM_EPS 1e-10F

/* Int8 quantization bounds for vector storage (maps [-1.0, 1.0] to [-127, 127]). */
#define CBM_SEM_INT8_MAX 127.0F
#define CBM_SEM_INT8_MIN (-127.0F)

/* Unit vector bounds (normalized similarity / cosine value limits). */
#define CBM_SEM_UNIT_POS 1.0F
#define CBM_SEM_UNIT_NEG (-1.0F)

/* Proximity boost: same-file gets 1.0 + CBM_SEM_PROX_MAX_BOOST, distant gets 1.0. */
#define CBM_SEM_PROX_MAX_BOOST 0.10F

/* Reflective Random Indexing blend factors for pass2 (context mixing). */
#define CBM_SEM_RRI_ALPHA 0.3F
#define CBM_SEM_RRI_BETA 0.7F

/* IDF smoothing constant (log base): log2(1 + docs / doc_freq). */
#define CBM_SEM_IDF_BASE 0.5F

/* Strings of exactly two parts / dimensions used for co-occurrence window math. */
#define CBM_SEM_COOCCUR_STRIDE 2

/* Worker tile/chunk sizes for parallel cooccurrence passes. */
enum {
    CBM_SEM_WORKER_STACK_CAP = 256,
    CBM_SEM_TILE_SIZE = 40,
    CBM_SEM_SEEN_INIT_CAP = 256,
    CBM_SEM_RESOLVE_CHUNK = 64,
    CBM_SEM_COOCCUR_CHUNK = 32,
    CBM_SEM_RRI_TILE = 128,
    CBM_SEM_INT8_I_LO = -128,
    CBM_SEM_INT8_I_HI = 127,
    CBM_SEM_ATOMIC_INC = 1,
    CBM_SEM_RI_NONZERO_COUNT = 8,
};

/* Mutex-init state machine for the lazy-initialized pretrained token map.
 * State 0 = not started, 1 = initializing, 2 = initialized. */
enum {
    MTX_STATE_UNINIT = 0,
    MTX_STATE_INITIALIZING = 1,
    MTX_STATE_READY = 2,
};

/* Pretrained map ready flag (signals a one-shot atomic transition 0 → 1). */
enum { MAP_READY = 1 };

/* Numeric conversion radix for strtol (base 10 decimal). */
enum { BASE_DECIMAL = 10 };

/* ── Configuration ───────────────────────────────────────────────── */

cbm_sem_config_t cbm_sem_get_config(void) {
    cbm_sem_config_t cfg = {
        .w_tfidf = CBM_SEM_W_TFIDF,
        .w_ri = CBM_SEM_W_RI,
        .w_minhash = CBM_SEM_W_MINHASH,
        .w_api = CBM_SEM_W_API,
        .w_type = CBM_SEM_W_TYPE,
        .w_decorator = CBM_SEM_W_DECORATOR,
        .w_struct_profile = CBM_SEM_W_STRUCT_PROFILE,
        .w_dataflow = CBM_SEM_W_DATAFLOW,
        .threshold = (float)CBM_SEM_EDGE_THRESHOLD,
        .max_edges = CBM_SEM_MAX_EDGES,
    };
    const char *thresh = getenv("CBM_SEMANTIC_THRESHOLD");
    if (thresh) {
        /* strtod reports errors via endptr; reject non-numeric input silently. */
        char *end = NULL;
        double parsed = strtod(thresh, &end);
        if (end != thresh && parsed > CBM_SEM_THRESHOLD_MIN && parsed <= CBM_SEM_THRESHOLD_MAX) {
            cfg.threshold = (float)parsed;
        }
    }
    return cfg;
}

bool cbm_sem_is_enabled(void) {
    const char *val = getenv("CBM_SEMANTIC_ENABLED");
    return val && val[0] == '1';
}

/* ── Token extraction ────────────────────────────────────────────── */

/* True for characters that terminate a token regardless of case. */
static bool is_token_delim(char c) {
    return c == '.' || c == '/' || c == '_' || c == '-' || c == ' ' || c == '(' || c == ')' ||
           c == ',' || c == ':';
}

/* True for a camelCase transition: uppercase letter preceded by a lowercase. */
static bool is_camel_break(const char *name, int i) {
    if (i <= 0) {
        return false;
    }
    char c = name[i];
    char p = name[i - SKIP_ONE];
    return c >= 'A' && c <= 'Z' && p >= 'a' && p <= 'z';
}

/* Flush the current buffer as a token into out[]. */
static void flush_token(char *buf, int *blen, char **out, int *count, int max_out) {
    if (*blen > 0 && *count < max_out) {
        buf[*blen] = '\0';
        out[(*count)++] = strdup(buf);
    }
    *blen = 0;
}

int cbm_sem_tokenize(const char *name, char **out, int max_out) {
    if (!name || !out || max_out <= 0) {
        return 0;
    }
    int count = 0;
    char buf[TOKEN_BUF_LEN];
    int blen = 0;

    for (int i = 0; name[i] && count < max_out; i++) {
        char c = name[i];
        bool split = is_token_delim(c);
        bool camel = is_camel_break(name, i);
        if (split || camel) {
            flush_token(buf, &blen, out, &count, max_out);
            if (split) {
                continue;
            }
        }
        if (blen < TOKEN_BUF_LEN - SKIP_ONE && isalnum((unsigned char)c)) {
            buf[blen++] = (char)tolower((unsigned char)c);
        }
    }
    flush_token(buf, &blen, out, &count, max_out);

    /* Abbreviation expansion: add expanded forms for common code abbreviations.
     * "err" → also add "error", "ctx" → "context", etc. */
    /* Cross-language abbreviation table — covers Go, Python, JS/TS, Rust,
     * Java, C/C++, Ruby, PHP, Kotlin, Swift, Scala, C#, and common patterns. */
    static const struct {
        const char *abbrev;
        const char *expanded;
    } abbrevs[] = {
        /* Error/exception handling */
        {"err", "error"},
        {"exc", "exception"},
        {"ex", "exception"},
        /* Context/config */
        {"ctx", "context"},
        {"cfg", "config"},
        {"conf", "configuration"},
        {"env", "environment"},
        {"opt", "option"},
        {"opts", "options"},
        /* Request/response (HTTP, RPC) */
        {"req", "request"},
        {"res", "response"},
        {"resp", "response"},
        {"rsp", "response"},
        {"hdr", "header"},
        {"hdrs", "headers"},
        /* Strings/formatting */
        {"str", "string"},
        {"fmt", "format"},
        {"msg", "message"},
        {"txt", "text"},
        {"lbl", "label"},
        {"desc", "description"},
        /* Data structures */
        {"buf", "buffer"},
        {"arr", "array"},
        {"vec", "vector"},
        {"lst", "list"},
        {"dict", "dictionary"},
        {"tbl", "table"},
        {"stk", "stack"},
        {"que", "queue"},
        /* Functions/callbacks */
        {"fn", "function"},
        {"func", "function"},
        {"cb", "callback"},
        {"proc", "procedure"},
        {"ctor", "constructor"},
        {"dtor", "destructor"},
        /* Database/storage */
        {"db", "database"},
        {"col", "column"},
        {"tbl", "table"},
        {"stmt", "statement"},
        {"txn", "transaction"},
        {"trx", "transaction"},
        {"repo", "repository"},
        /* Auth/security */
        {"auth", "authentication"},
        {"authz", "authorization"},
        {"perm", "permission"},
        {"cred", "credential"},
        {"tok", "token"},
        {"pwd", "password"},
        /* Values/types */
        {"val", "value"},
        {"num", "number"},
        {"int", "integer"},
        {"bool", "boolean"},
        {"flt", "float"},
        {"dbl", "double"},
        /* Indexing/iteration */
        {"idx", "index"},
        {"iter", "iterator"},
        {"elem", "element"},
        {"cnt", "count"},
        {"len", "length"},
        {"sz", "size"},
        {"pos", "position"},
        {"off", "offset"},
        {"cap", "capacity"},
        /* Lifecycle */
        {"init", "initialize"},
        {"deinit", "deinitialize"},
        {"alloc", "allocate"},
        {"dealloc", "deallocate"},
        {"del", "delete"},
        {"rm", "remove"},
        /* Implementation/interface */
        {"impl", "implementation"},
        {"iface", "interface"},
        {"abs", "abstract"},
        {"decl", "declaration"},
        /* Parameters/attributes */
        {"param", "parameter"},
        {"arg", "argument"},
        {"attr", "attribute"},
        {"prop", "property"},
        {"ret", "return"},
        /* Source/destination */
        {"src", "source"},
        {"dst", "destination"},
        {"tgt", "target"},
        {"orig", "original"},
        {"prev", "previous"},
        {"cur", "current"},
        {"tmp", "temporary"},
        {"temp", "temporary"},
        /* Networking/IO */
        {"conn", "connection"},
        {"sess", "session"},
        {"sock", "socket"},
        {"addr", "address"},
        {"url", "uniform"},
        {"srv", "server"},
        {"cli", "client"},
        {"svc", "service"},
        {"ep", "endpoint"},
        /* Management */
        {"mgr", "manager"},
        {"ctrl", "controller"},
        {"hdlr", "handler"},
        {"sched", "scheduler"},
        {"disp", "dispatcher"},
        {"reg", "registry"},
        /* Async/concurrent */
        {"chan", "channel"},
        {"sem", "semaphore"},
        {"mtx", "mutex"},
        {"wg", "waitgroup"},
        {"sig", "signal"},
        {"evt", "event"},
        {"sub", "subscriber"},
        {"pub", "publisher"},
        /* Testing */
        {"spec", "specification"},
        {"mock", "mock"},
        {"stub", "stub"},
        {"assert", "assertion"},
        /* Logging/monitoring */
        {"log", "logging"},
        {"lvl", "level"},
        {"dbg", "debug"},
        {"wrn", "warning"},
        {"inf", "info"},
        /* Time */
        {"ts", "timestamp"},
        {"dur", "duration"},
        {"ttl", "timetolive"},
        /* Miscellaneous */
        {"ver", "version"},
        {"ns", "namespace"},
        {"pkg", "package"},
        {"mod", "module"},
        {"lib", "library"},
        {"dep", "dependency"},
        {"ref", "reference"},
        {"ptr", "pointer"},
        {"obj", "object"},
        {"doc", "document"},
        {"cmd", "command"},
        {"ops", "operations"},
        {"util", "utility"},
        {"hlp", "helper"},
        {"ext", "extension"},
        {NULL, NULL},
    };
    int orig_count = count;
    for (int t = 0; t < orig_count && count < max_out; t++) {
        for (int a = 0; abbrevs[a].abbrev; a++) {
            if (strcmp(out[t], abbrevs[a].abbrev) == 0) {
                out[count++] = strdup(abbrevs[a].expanded);
                break;
            }
        }
    }

    return count;
}

/* ── Dense vector operations ─────────────────────────────────────── */

float cbm_sem_cosine(const cbm_sem_vec_t *a, const cbm_sem_vec_t *b) {
    if (!a || !b) {
        return 0.0F;
    }
    float dot = 0.0F;
    float mag_a = 0.0F;
    float mag_b = 0.0F;
    for (int i = 0; i < CBM_SEM_DIM; i++) {
        dot += a->v[i] * b->v[i];
        mag_a += a->v[i] * a->v[i];
        mag_b += b->v[i] * b->v[i];
    }
    float denom = sqrtf(mag_a) * sqrtf(mag_b);
    if (denom < CBM_SEM_DENOM_EPS) {
        return 0.0F;
    }
    return dot / denom;
}

/* Pretrained token lookup table — built lazily on first use. */
static CBMHashTable *g_pretrained_map = NULL;
static _Atomic int g_pretrained_ready = 0;
static cbm_mutex_t g_pretrained_mtx;
static _Atomic int g_pretrained_mtx_init = 0;

/* Thread-safe lazy init of the pretrained token lookup map.
 * Uses double-checked locking: fast path reads an atomic flag. */
static void ensure_pretrained_map(void) {
    if (atomic_load_explicit(&g_pretrained_ready, memory_order_acquire)) {
        return;
    }
    /* First-time init of the mutex itself (also needs to be thread-safe) */
    int expected = MTX_STATE_UNINIT;
    if (atomic_compare_exchange_strong_explicit(&g_pretrained_mtx_init, &expected,
                                                MTX_STATE_INITIALIZING, memory_order_acq_rel,
                                                memory_order_acquire)) {
        cbm_mutex_init(&g_pretrained_mtx);
        atomic_store_explicit(&g_pretrained_mtx_init, MTX_STATE_READY, memory_order_release);
    } else {
        /* Spin until another thread finishes initializing the mutex */
        while (atomic_load_explicit(&g_pretrained_mtx_init, memory_order_acquire) !=
               MTX_STATE_READY) {
            /* brief spin */
        }
    }
    cbm_mutex_lock(&g_pretrained_mtx);
    if (!atomic_load_explicit(&g_pretrained_ready, memory_order_acquire)) {
        g_pretrained_map = cbm_ht_create(PRETRAINED_TOKEN_COUNT);
        char idx_buf[CBM_SZ_16];
        for (int i = 0; i < PRETRAINED_TOKEN_COUNT; i++) {
            const char *tok = PRETRAINED_TOKENS[i];
            if (tok && tok[0]) {
                snprintf(idx_buf, sizeof(idx_buf), "%d", i);
                cbm_ht_set(g_pretrained_map, strdup(tok), strdup(idx_buf));
            }
        }
        atomic_store_explicit(&g_pretrained_ready, MAP_READY, memory_order_release);
    }
    cbm_mutex_unlock(&g_pretrained_mtx);
}

void cbm_sem_ensure_ready(void) {
    ensure_pretrained_map();
}

void cbm_sem_random_index(const char *token, cbm_sem_vec_t *out) {
    memset(out, 0, sizeof(*out));
    if (!token) {
        return;
    }

    /* Try pretrained nomic-embed-code vector first (768d, distilled from 7B). */
    ensure_pretrained_map();
    const char *idx_str = cbm_ht_get(g_pretrained_map, token);
    if (idx_str) {
        char *end = NULL;
        long idx = strtol(idx_str, &end, BASE_DECIMAL);
        if (end != idx_str && idx >= 0 && idx < PRETRAINED_TOKEN_COUNT) {
            const int8_t *pvec = pretrained_vec_at((int)idx);
            for (int d = 0; d < CBM_SEM_DIM && d < PRETRAINED_DIM; d++) {
                out->v[d] = (float)pvec[d] / CBM_SEM_INT8_MAX;
            }
            return;
        }
    }

    /* Fallback: sparse random vector for tokens not in pretrained vocab. */
    uint64_t seed = XXH3_64bits(token, strlen(token));
    for (int i = 0; i < CBM_SEM_SPARSE_NNZE; i++) {
        uint64_t h = XXH3_64bits_withSeed(&i, sizeof(i), seed + RI_SEED_BASE);
        int pos = (int)(h % CBM_SEM_DIM);
        float sign = (h & SKIP_ONE) ? CBM_SEM_UNIT_POS : -CBM_SEM_UNIT_POS;
        out->v[pos] += sign;
    }
}

void cbm_sem_normalize(cbm_sem_vec_t *v) {
    if (!v) {
        return;
    }
    float mag = 0.0F;
    for (int i = 0; i < CBM_SEM_DIM; i++) {
        mag += v->v[i] * v->v[i];
    }
    mag = sqrtf(mag);
    if (mag < CBM_SEM_DENOM_EPS) {
        return;
    }
    float inv = CBM_SEM_UNIT_POS / mag;
    for (int i = 0; i < CBM_SEM_DIM; i++) {
        v->v[i] *= inv;
    }
}

void cbm_sem_vec_add_scaled(cbm_sem_vec_t *dst, const cbm_sem_vec_t *src, float scale) {
    if (!dst || !src) {
        return;
    }
    for (int i = 0; i < CBM_SEM_DIM; i++) {
        dst->v[i] += scale * src->v[i];
    }
}

/* ── Corpus (IDF + Random Indexing enrichment) ───────────────────── */

typedef struct {
    char *token;
    int doc_freq;
    cbm_sem_vec_t enriched_vec; /* context-enriched via co-occurrence */
} corpus_entry_t;

struct cbm_sem_corpus {
    CBMHashTable *token_map; /* token → index into entries[] */
    corpus_entry_t *entries;
    int entry_count;
    int entry_cap;
    int doc_count;
    bool finalized;

    /* Per-document token lists for co-occurrence pass */
    int **doc_token_ids;
    int *doc_token_counts;
    int doc_cap;
};

static int corpus_get_or_add(cbm_sem_corpus_t *c, const char *token) {
    char idx_buf[CBM_SZ_16];
    const char *existing = cbm_ht_get(c->token_map, token);
    if (existing) {
        char *end = NULL;
        long parsed = strtol(existing, &end, BASE_DECIMAL);
        return (end != existing) ? (int)parsed : CBM_NOT_FOUND;
    }
    if (c->entry_count >= c->entry_cap) {
        int new_cap = c->entry_cap < CORPUS_INIT_CAP ? CORPUS_INIT_CAP : c->entry_cap * PAIR_LEN;
        corpus_entry_t *grown = realloc(c->entries, (size_t)new_cap * sizeof(corpus_entry_t));
        if (!grown) {
            return CBM_NOT_FOUND;
        }
        c->entries = grown;
        c->entry_cap = new_cap;
    }
    int idx = c->entry_count++;
    c->entries[idx].token = strdup(token);
    c->entries[idx].doc_freq = 0;
    memset(&c->entries[idx].enriched_vec, 0, sizeof(cbm_sem_vec_t));
    snprintf(idx_buf, sizeof(idx_buf), "%d", idx);
    cbm_ht_set(c->token_map, strdup(token), strdup(idx_buf));
    return idx;
}

cbm_sem_corpus_t *cbm_sem_corpus_new(void) {
    cbm_sem_corpus_t *c = calloc(SKIP_ONE, sizeof(cbm_sem_corpus_t));
    if (c) {
        c->token_map = cbm_ht_create(CORPUS_INIT_CAP);
    }
    return c;
}

void cbm_sem_corpus_add_doc(cbm_sem_corpus_t *corpus, const char **tokens, int count) {
    if (!corpus || !tokens || count <= 0) {
        return;
    }
    /* Track document for co-occurrence pass */
    if (corpus->doc_count >= corpus->doc_cap) {
        int new_cap =
            corpus->doc_cap < DOC_TOKENS_INIT ? DOC_TOKENS_INIT : corpus->doc_cap * PAIR_LEN;
        int **grown_ids = realloc(corpus->doc_token_ids, (size_t)new_cap * sizeof(int *));
        int *grown_counts = realloc(corpus->doc_token_counts, (size_t)new_cap * sizeof(int));
        if (!grown_ids || !grown_counts) {
            free(grown_ids);
            free(grown_counts);
            return;
        }
        corpus->doc_token_ids = grown_ids;
        corpus->doc_token_counts = grown_counts;
        corpus->doc_cap = new_cap;
    }
    int doc_idx = corpus->doc_count++;
    corpus->doc_token_ids[doc_idx] = malloc((size_t)count * sizeof(int));
    corpus->doc_token_counts[doc_idx] = count;

    /* Per-doc unique set for IDF */
    int *seen = calloc((size_t)corpus->entry_cap + (size_t)count + CORPUS_INIT_CAP, sizeof(int));
    int seen_count = 0;

    for (int i = 0; i < count; i++) {
        int tid = corpus_get_or_add(corpus, tokens[i]);
        corpus->doc_token_ids[doc_idx][i] = tid;
        if (tid < 0) {
            continue;
        }
        /* Check uniqueness for IDF (simple linear scan — tokens per doc is small) */
        bool is_new = true;
        for (int j = 0; j < seen_count; j++) {
            if (seen[j] == tid) {
                is_new = false;
                break;
            }
        }
        if (is_new) {
            seen[seen_count++] = tid;
            corpus->entries[tid].doc_freq++;
        }
    }
    free(seen);
}

/* ── Parallel corpus batch build ──────────────────────────────────── */
/* Strategy:
 *   Phase A (SEQUENTIAL): Scan all documents once to build the global
 *     token_map (inserts unique tokens, assigns global IDs). This is
 *     inherently sequential (hash table mutation), but much faster than
 *     the current per-doc add_doc because we avoid the per-doc malloc of
 *     the `seen` array and per-doc bookkeeping.
 *   Phase B (PARALLEL): Each worker processes a chunk of docs, translates
 *     tokens → global IDs via read-only token_map lookups, fills
 *     doc_token_ids[d], and accumulates doc_freq contributions via atomics.
 */

typedef struct {
    cbm_sem_corpus_t *corpus;
    char **all_tokens;
    const int *token_counts;
    int max_tokens;
    int doc_count;
    _Atomic int *doc_freq_atomic; /* per-entry atomic counter (entry_count long) */
    _Atomic int next_idx;
} batch_resolve_ctx_t;

/* Resolve one document: look up each token's global ID, fill the corpus
 * doc_token_ids[d], and bump the per-token doc_freq counter atomically.  The
 * caller is responsible for ensuring `seen` has capacity for `count` ints
 * before calling (the worker grows its per-thread scratch buffer). */
static void batch_resolve_one_doc(batch_resolve_ctx_t *bc, int doc_index, int *seen) {
    int count = bc->token_counts[doc_index];
    if (count <= 0) {
        bc->corpus->doc_token_ids[doc_index] = NULL;
        bc->corpus->doc_token_counts[doc_index] = 0;
        return;
    }
    int *ids = malloc((size_t)count * sizeof(int));
    bc->corpus->doc_token_ids[doc_index] = ids;
    bc->corpus->doc_token_counts[doc_index] = count;

    int seen_count = 0;
    char **tokens = &bc->all_tokens[(ptrdiff_t)doc_index * bc->max_tokens];
    for (int i = 0; i < count; i++) {
        const char *idx_str = cbm_ht_get(bc->corpus->token_map, tokens[i]);
        int tid = CBM_NOT_FOUND;
        if (idx_str) {
            char *end = NULL;
            long parsed = strtol(idx_str, &end, BASE_DECIMAL);
            if (end != idx_str) {
                tid = (int)parsed;
            }
        }
        ids[i] = tid;
        if (tid < 0) {
            continue;
        }
        /* Unique-per-doc check for IDF */
        bool is_new = true;
        for (int j = 0; j < seen_count; j++) {
            if (seen[j] == tid) {
                is_new = false;
                break;
            }
        }
        if (is_new) {
            seen[seen_count++] = tid;
            atomic_fetch_add_explicit(&bc->doc_freq_atomic[tid], CBM_SEM_ATOMIC_INC,
                                      memory_order_relaxed);
        }
    }
}

static void batch_resolve_worker(int worker_id, void *ctx_ptr) {
    (void)worker_id;
    batch_resolve_ctx_t *bc = ctx_ptr;
    /* Per-worker scratch for unique-per-doc tracking */
    int local_seen_cap = CBM_SEM_SEEN_INIT_CAP;
    int *seen = malloc((size_t)local_seen_cap * sizeof(int));
    if (!seen) {
        return;
    }

    while (true) {
        int start =
            atomic_fetch_add_explicit(&bc->next_idx, CBM_SEM_RESOLVE_CHUNK, memory_order_relaxed);
        if (start >= bc->doc_count) {
            break;
        }
        int end = start + CBM_SEM_RESOLVE_CHUNK;
        if (end > bc->doc_count) {
            end = bc->doc_count;
        }
        for (int d = start; d < end; d++) {
            int count = bc->token_counts[d];
            if (count > local_seen_cap) {
                int *grown = realloc(seen, (size_t)count * sizeof(int));
                if (!grown) {
                    continue;
                }
                seen = grown;
                local_seen_cap = count;
            }
            batch_resolve_one_doc(bc, d, seen);
        }
    }
    free(seen);
}

void cbm_sem_corpus_add_docs_batch(cbm_sem_corpus_t *corpus, char **all_tokens,
                                   const int *token_counts, int doc_count, int max_tokens_per_doc) {
    if (!corpus || !all_tokens || !token_counts || doc_count <= 0) {
        return;
    }

    /* Phase A (SEQUENTIAL): Build token_map and allocate doc arrays.
     * Hash table mutation can't be parallelized; strdup+insert is the cost. */
    if (corpus->doc_cap < corpus->doc_count + doc_count) {
        int new_cap = corpus->doc_count + doc_count;
        int **grown_ids = realloc(corpus->doc_token_ids, (size_t)new_cap * sizeof(int *));
        int *grown_counts = realloc(corpus->doc_token_counts, (size_t)new_cap * sizeof(int));
        if (!grown_ids || !grown_counts) {
            free(grown_ids);
            free(grown_counts);
            return;
        }
        corpus->doc_token_ids = grown_ids;
        corpus->doc_token_counts = grown_counts;
        corpus->doc_cap = new_cap;
    }
    int base_doc = corpus->doc_count;
    corpus->doc_count += doc_count;

    for (int d = 0; d < doc_count; d++) {
        int count = token_counts[d];
        char **tokens = &all_tokens[(ptrdiff_t)d * max_tokens_per_doc];
        for (int i = 0; i < count; i++) {
            /* Inserts token into token_map if new; we discard return here —
             * Phase B will re-lookup in read-only mode to get the ID. */
            (void)corpus_get_or_add(corpus, tokens[i]);
        }
    }

    /* Phase B (PARALLEL): Resolve tokens → IDs and count doc_freq per entry.
     * token_map is now read-only; each worker owns its doc range (no writes
     * to shared state except atomic doc_freq counters). */
    _Atomic int *doc_freq_atomic = calloc((size_t)corpus->entry_count, sizeof(_Atomic int));
    if (!doc_freq_atomic) {
        /* OOM fallback: sequential path. Roll back doc_count first since
         * add_doc increments it itself. */
        corpus->doc_count = base_doc;
        for (int d = 0; d < doc_count; d++) {
            int count = token_counts[d];
            char **tokens = &all_tokens[(ptrdiff_t)d * max_tokens_per_doc];
            cbm_sem_corpus_add_doc(corpus, (const char **)tokens, count);
        }
        return;
    }

    int worker_count = cbm_default_worker_count(false);
    batch_resolve_ctx_t bc = {
        .corpus = corpus,
        .all_tokens = all_tokens,
        .token_counts = token_counts,
        .max_tokens = max_tokens_per_doc,
        .doc_count = doc_count,
        .doc_freq_atomic = doc_freq_atomic,
    };
    atomic_init(&bc.next_idx, 0);
    /* Temporarily re-base doc arrays so workers write to base_doc..base_doc+doc_count */
    corpus->doc_token_ids += base_doc;
    corpus->doc_token_counts += base_doc;
    cbm_parallel_for_opts_t opts = {.max_workers = worker_count, .force_pthreads = false};
    cbm_parallel_for(worker_count, batch_resolve_worker, &bc, opts);
    corpus->doc_token_ids -= base_doc;
    corpus->doc_token_counts -= base_doc;

    /* Phase C (SEQUENTIAL reduce): atomic counters → entries[].doc_freq */
    for (int i = 0; i < corpus->entry_count; i++) {
        corpus->entries[i].doc_freq +=
            atomic_load_explicit(&doc_freq_atomic[i], memory_order_relaxed);
    }
    free(doc_freq_atomic);
}

/* ── Parallel corpus_finalize ─────────────────────────────────────── */
/* Strategy:
 *   1. Precompute base RI vectors into a shared array (eliminates ~333M
 *      redundant cbm_sem_random_index calls on kernel-scale corpora).
 *   2. Co-occurrence passes: partition TARGET tokens across workers so each
 *      worker writes to a disjoint range of enriched_vec (zero contention).
 *      Each worker still scans all documents but only accumulates for targets
 *      in its range. Inner vector add is the parallelized work.
 *   3. Normalize/blend loops are trivially parallel per-entry.
 */

/* Reverse index: for each token id, a list of (doc_id, position_in_doc) pairs.
 * Built once, reused for both cooccur passes. Eliminates the O(num_chunks × doc_count)
 * redundant outer scan in the old algorithm. */
typedef struct {
    int32_t doc_id;
    int32_t pos;
} cooccur_pos_t;

typedef struct {
    int *offsets;        /* offsets[entry_count + 1], prefix sum of occurrences */
    cooccur_pos_t *flat; /* flat array of positions, total = offsets[entry_count] */
} reverse_index_t;

/* Tagged source vector: sparse (~30% of tokens, 8 inline nonzeros) or dense int8
 * reference into PRETRAINED_VECTOR_BLOB (no copy). Dense path converts int8→float
 * on the fly in the hot loop. Tested with both int8 and float32 blob storage
 * formats — cooccur passes are memory-bandwidth-bound, so the int8 format (4x
 * less source traffic) is equivalent in wall time despite the conversion cost,
 * while saving 90 MB of binary size. */
typedef struct {
    uint8_t is_sparse; /* 1 = sparse path, 0 = dense int8 reference */
    uint8_t nnz;       /* number of nonzeros used in sparse path */
    uint16_t _pad;
    uint16_t indices[CBM_SEM_SPARSE_NNZE]; /* 8 * 2 = 16 bytes */
    float values[CBM_SEM_SPARSE_NNZE];     /* 8 * 4 = 32 bytes */
    const int8_t *dense_int8;              /* points into PRETRAINED_VECTOR_BLOB */
} cbm_sem_src_entry_t;

/* Inline helper: initialize a target vector from a sparse/dense source. */
static inline void sem_target_init_from_src(cbm_sem_vec_t *dst, const cbm_sem_src_entry_t *src) {
    memset(dst, 0, sizeof(*dst));
    if (src->is_sparse) {
        for (int k = 0; k < src->nnz; k++) {
            dst->v[src->indices[k]] = src->values[k];
        }
    } else {
        const int8_t *s = src->dense_int8;
        const float inv127 = CBM_SEM_UNIT_POS / CBM_SEM_INT8_MAX;
        for (int d = 0; d < CBM_SEM_DIM; d++) {
            dst->v[d] = inv127 * (float)s[d];
        }
    }
}

/* Inline helper: add weighted source into target.
 * Sparse path: ~8 operations, ~48 bytes source memory traffic.
 * Dense path: 768 mul-adds with int8→float conversion, ~768 bytes traffic. */
static inline void sem_vec_add_src_scaled(cbm_sem_vec_t *dst, const cbm_sem_src_entry_t *src,
                                          float scale) {
    if (src->is_sparse) {
        for (int k = 0; k < src->nnz; k++) {
            dst->v[src->indices[k]] += scale * src->values[k];
        }
    } else {
        const int8_t *s = src->dense_int8;
        const float mul = scale * (CBM_SEM_UNIT_POS / CBM_SEM_INT8_MAX);
        for (int d = 0; d < CBM_SEM_DIM; d++) {
            dst->v[d] += mul * (float)s[d];
        }
    }
}

/* Pass 1 context: uses sparse/int8 tagged sources (most memory-efficient). */
typedef struct {
    corpus_entry_t *entries;
    const cbm_sem_src_entry_t *src_entries; /* sparse or int8-dense per token */
    int **doc_token_ids;
    const int *doc_token_counts;
    const reverse_index_t *rev;
    int doc_count;
    int entry_count;
    _Atomic int next_chunk;
    int num_chunks;
    int chunk_size;

    /* Cache-blocked tiling parameters */
    int tile_size; /* targets per L2-resident tile */
} cooccur_sparse_ctx_t;

/* Accumulate co-occurrence context for a single target token into `target`.
 * Reads neighbors within ±CBM_SEM_WINDOW positions across all documents that
 * reference this token id via the reverse index. */
static void cooccur_sparse_one_target(cooccur_sparse_ctx_t *cc, int tid, cbm_sem_vec_t *target) {
    int occ_start = cc->rev->offsets[tid];
    int occ_end = cc->rev->offsets[tid + SKIP_ONE];
    for (int p = occ_start; p < occ_end; p++) {
        int d = cc->rev->flat[p].doc_id;
        int i = cc->rev->flat[p].pos;
        int *ids = cc->doc_token_ids[d];
        int len = cc->doc_token_counts[d];
        for (int w = -CBM_SEM_WINDOW; w <= CBM_SEM_WINDOW; w++) {
            if (w == 0) {
                continue;
            }
            int j = i + w;
            if (j < 0 || j >= len) {
                continue;
            }
            int nid = ids[j];
            if (nid < 0) {
                continue;
            }
            float weight = CBM_SEM_UNIT_POS / (float)abs(w);
            sem_vec_add_src_scaled(target, &cc->src_entries[nid], weight);
        }
    }
}

static void cooccur_worker_sparse(int worker_id, void *ctx_ptr) {
    (void)worker_id;
    cooccur_sparse_ctx_t *cc = ctx_ptr;
    while (true) {
        int ci =
            atomic_fetch_add_explicit(&cc->next_chunk, CBM_SEM_ATOMIC_INC, memory_order_relaxed);
        if (ci >= cc->num_chunks) {
            break;
        }
        int chunk_start = ci * cc->chunk_size;
        int chunk_end = chunk_start + cc->chunk_size;
        if (chunk_end > cc->entry_count) {
            chunk_end = cc->entry_count;
        }

        /* Cache-blocked target tiling: process tile_size targets at a time so
         * their vectors stay resident in L2 cache during their accumulation. */
        for (int tile_start = chunk_start; tile_start < chunk_end; tile_start += cc->tile_size) {
            int tile_end = tile_start + cc->tile_size;
            if (tile_end > chunk_end) {
                tile_end = chunk_end;
            }
            for (int tid = tile_start; tid < tile_end; tid++) {
                sem_target_init_from_src(&cc->entries[tid].enriched_vec, &cc->src_entries[tid]);
                cooccur_sparse_one_target(cc, tid, &cc->entries[tid].enriched_vec);
            }
        }
    }
}

/* Pass 2 (RRI) context: uses int8-quantized pass1 vectors as source.
 * Pass1 outputs are dense float32 post-normalization, values in [-1,1].
 * We quantize to int8 once (×127) to cut source memory traffic 4x. */
typedef struct {
    corpus_entry_t *entries;
    const int8_t *pass1_q; /* [entry_count × CBM_SEM_DIM] int8 quantized pass1 */
    int **doc_token_ids;
    const int *doc_token_counts;
    const reverse_index_t *rev;
    int doc_count;
    int entry_count;
    _Atomic int next_chunk;
    int num_chunks;
    int chunk_size;
    int tile_size;
} cooccur_int8_ctx_t;

static inline void sem_vec_add_int8_scaled(cbm_sem_vec_t *dst, const int8_t *src, float scale) {
    const float mul = scale * (CBM_SEM_UNIT_POS / CBM_SEM_INT8_MAX);
    for (int d = 0; d < CBM_SEM_DIM; d++) {
        dst->v[d] += mul * (float)src[d];
    }
}

/* RRI pass 2 accumulator for a single target token, reading int8-quantized
 * pass1 vectors as the source. */
static void cooccur_int8_one_target(cooccur_int8_ctx_t *cc, int tid, cbm_sem_vec_t *target) {
    int occ_start = cc->rev->offsets[tid];
    int occ_end = cc->rev->offsets[tid + SKIP_ONE];
    for (int p = occ_start; p < occ_end; p++) {
        int d = cc->rev->flat[p].doc_id;
        int i = cc->rev->flat[p].pos;
        int *ids = cc->doc_token_ids[d];
        int len = cc->doc_token_counts[d];
        for (int w = -CBM_SEM_WINDOW; w <= CBM_SEM_WINDOW; w++) {
            if (w == 0) {
                continue;
            }
            int j = i + w;
            if (j < 0 || j >= len) {
                continue;
            }
            int nid = ids[j];
            if (nid < 0) {
                continue;
            }
            float weight = CBM_SEM_UNIT_POS / (float)abs(w);
            sem_vec_add_int8_scaled(target, &cc->pass1_q[(size_t)nid * CBM_SEM_DIM], weight);
        }
    }
}

static void cooccur_worker_int8(int worker_id, void *ctx_ptr) {
    (void)worker_id;
    cooccur_int8_ctx_t *cc = ctx_ptr;
    while (true) {
        int ci =
            atomic_fetch_add_explicit(&cc->next_chunk, CBM_SEM_ATOMIC_INC, memory_order_relaxed);
        if (ci >= cc->num_chunks) {
            break;
        }
        int chunk_start = ci * cc->chunk_size;
        int chunk_end = chunk_start + cc->chunk_size;
        if (chunk_end > cc->entry_count) {
            chunk_end = cc->entry_count;
        }

        for (int tile_start = chunk_start; tile_start < chunk_end; tile_start += cc->tile_size) {
            int tile_end = tile_start + cc->tile_size;
            if (tile_end > chunk_end) {
                tile_end = chunk_end;
            }
            for (int tid = tile_start; tid < tile_end; tid++) {
                /* RRI pass 2 starts from zero (no self-init) */
                memset(&cc->entries[tid].enriched_vec, 0, sizeof(cbm_sem_vec_t));
                cooccur_int8_one_target(cc, tid, &cc->entries[tid].enriched_vec);
            }
        }
    }
}

typedef struct {
    corpus_entry_t *entries;
    int entry_count;
    _Atomic int next_idx;
} norm_ctx_t;

static void normalize_worker(int worker_id, void *ctx_ptr) {
    (void)worker_id;
    norm_ctx_t *nc = ctx_ptr;
    while (true) {
        int start = atomic_fetch_add_explicit(&nc->next_idx, CBM_SEM_WORKER_STACK_CAP,
                                              memory_order_relaxed);
        if (start >= nc->entry_count) {
            break;
        }
        int end = start + CBM_SEM_WORKER_STACK_CAP;
        if (end > nc->entry_count) {
            end = nc->entry_count;
        }
        for (int i = start; i < end; i++) {
            cbm_sem_normalize(&nc->entries[i].enriched_vec);
        }
    }
}

typedef struct {
    corpus_entry_t *entries;
    const cbm_sem_vec_t *pass1;
    int entry_count;
    _Atomic int next_idx;
} blend_ctx_t;

static void blend_worker(int worker_id, void *ctx_ptr) {
    (void)worker_id;
    blend_ctx_t *bc = ctx_ptr;
    while (true) {
        int start = atomic_fetch_add_explicit(&bc->next_idx, CBM_SEM_WORKER_STACK_CAP,
                                              memory_order_relaxed);
        if (start >= bc->entry_count) {
            break;
        }
        int end = start + CBM_SEM_WORKER_STACK_CAP;
        if (end > bc->entry_count) {
            end = bc->entry_count;
        }
        for (int i = start; i < end; i++) {
            cbm_sem_normalize(&bc->entries[i].enriched_vec);
            for (int d = 0; d < CBM_SEM_DIM; d++) {
                bc->entries[i].enriched_vec.v[d] =
                    (CBM_SEM_RRI_BETA * bc->pass1[i].v[d]) +
                    (CBM_SEM_RRI_ALPHA * bc->entries[i].enriched_vec.v[d]);
            }
        }
    }
}

typedef struct {
    corpus_entry_t *entries;
    cbm_sem_src_entry_t *src_entries;
    int entry_count;
    _Atomic int next_idx;
} src_build_ctx_t;

/* Build one src_entry for a token: dense float32 reference if in nomic vocab,
 * sparse inline representation otherwise. Collisions in the sparse hash are
 * merged and zeros filtered so the final representation is exactly the same
 * mathematical vector that the old dense path produced. */
static void build_src_entry(const char *token, cbm_sem_src_entry_t *out) {
    memset(out, 0, sizeof(*out));
    if (!token) {
        out->is_sparse = SKIP_ONE;
        out->nnz = 0;
        return;
    }
    /* Dense path: direct int8 pointer into pretrained blob (zero-copy). */
    const char *idx_str = cbm_ht_get(g_pretrained_map, token);
    if (idx_str) {
        char *end = NULL;
        long idx = strtol(idx_str, &end, BASE_DECIMAL);
        if (end != idx_str && idx >= 0 && idx < PRETRAINED_TOKEN_COUNT) {
            out->is_sparse = 0;
            out->dense_int8 = pretrained_vec_at((int)idx);
            return;
        }
    }
    /* Sparse path: compute 8 hash positions with collision merging. */
    out->is_sparse = SKIP_ONE;
    uint16_t tmp_idx[CBM_SEM_SPARSE_NNZE];
    float tmp_val[CBM_SEM_SPARSE_NNZE];
    int count = 0;
    uint64_t seed = XXH3_64bits(token, strlen(token));
    for (int i = 0; i < CBM_SEM_SPARSE_NNZE; i++) {
        uint64_t h = XXH3_64bits_withSeed(&i, sizeof(i), seed + RI_SEED_BASE);
        int pos = (int)(h % CBM_SEM_DIM);
        float sign = (h & SKIP_ONE) ? CBM_SEM_UNIT_POS : -CBM_SEM_UNIT_POS;
        /* Merge collisions */
        int found = CBM_NOT_FOUND;
        for (int j = 0; j < count; j++) {
            if (tmp_idx[j] == (uint16_t)pos) {
                found = j;
                break;
            }
        }
        if (found >= 0) {
            tmp_val[found] += sign;
        } else {
            tmp_idx[count] = (uint16_t)pos;
            tmp_val[count] = sign;
            count++;
        }
    }
    /* Filter zeros */
    int nnz = 0;
    for (int j = 0; j < count; j++) {
        if (tmp_val[j] != 0.0F) {
            out->indices[nnz] = tmp_idx[j];
            out->values[nnz] = tmp_val[j];
            nnz++;
        }
    }
    out->nnz = (uint8_t)nnz;
}

static void src_build_worker(int worker_id, void *ctx_ptr) {
    (void)worker_id;
    src_build_ctx_t *sc = ctx_ptr;
    while (true) {
        int start = atomic_fetch_add_explicit(&sc->next_idx, CBM_SEM_WORKER_STACK_CAP,
                                              memory_order_relaxed);
        if (start >= sc->entry_count) {
            break;
        }
        int end = start + CBM_SEM_WORKER_STACK_CAP;
        if (end > sc->entry_count) {
            end = sc->entry_count;
        }
        for (int i = start; i < end; i++) {
            build_src_entry(sc->entries[i].token, &sc->src_entries[i]);
        }
    }
}

/* Quantize pass1 float vectors to int8 (× 127) for use as pass2 source.
 * Input vectors are unit-normalized, so values are in [-1, 1] → int8 preserves
 * ~99% precision. 4x less memory traffic for pass2. */
typedef struct {
    const corpus_entry_t *entries;
    int8_t *pass1_q;
    int entry_count;
    _Atomic int next_idx;
} pass1_quant_ctx_t;

static void pass1_quantize_worker(int worker_id, void *ctx_ptr) {
    (void)worker_id;
    pass1_quant_ctx_t *qc = ctx_ptr;
    while (true) {
        int start =
            atomic_fetch_add_explicit(&qc->next_idx, CBM_SEM_RRI_TILE, memory_order_relaxed);
        if (start >= qc->entry_count) {
            break;
        }
        int end = start + CBM_SEM_RRI_TILE;
        if (end > qc->entry_count) {
            end = qc->entry_count;
        }
        for (int i = start; i < end; i++) {
            const cbm_sem_vec_t *src = &qc->entries[i].enriched_vec;
            int8_t *dst = &qc->pass1_q[(size_t)i * CBM_SEM_DIM];
            for (int d = 0; d < CBM_SEM_DIM; d++) {
                float v = src->v[d] * CBM_SEM_INT8_MAX;
                if (v > CBM_SEM_INT8_MAX) {
                    v = CBM_SEM_INT8_MAX;
                }
                if (v < -CBM_SEM_INT8_MAX) {
                    v = -CBM_SEM_INT8_MAX;
                }
                dst[d] = (int8_t)(v >= 0 ? v + CBM_SEM_IDF_BASE : v - CBM_SEM_IDF_BASE);
            }
        }
    }
}

/* Build reverse index: token_id → list of (doc_id, position) pairs.
 * SEQUENTIAL (fast: just pointer arithmetic + flat array fill). */
static reverse_index_t *build_reverse_index(cbm_sem_corpus_t *corpus) {
    reverse_index_t *rev = calloc(SKIP_ONE, sizeof(reverse_index_t));
    if (!rev) {
        return NULL;
    }
    /* Phase A: count occurrences per token */
    int *counts = calloc((size_t)corpus->entry_count + SKIP_ONE, sizeof(int));
    if (!counts) {
        free(rev);
        return NULL;
    }
    long total = 0;
    for (int d = 0; d < corpus->doc_count; d++) {
        int *ids = corpus->doc_token_ids[d];
        int len = corpus->doc_token_counts[d];
        for (int i = 0; i < len; i++) {
            int tid = ids[i];
            if (tid >= 0 && tid < corpus->entry_count) {
                counts[tid]++;
                total++;
            }
        }
    }
    /* Phase B: exclusive prefix sum → offsets[] */
    rev->offsets = malloc(((size_t)corpus->entry_count + SKIP_ONE) * sizeof(int));
    if (!rev->offsets) {
        free(counts);
        free(rev);
        return NULL;
    }
    int running = 0;
    for (int t = 0; t < corpus->entry_count; t++) {
        rev->offsets[t] = running;
        running += counts[t];
        counts[t] = 0; /* reuse as per-token fill cursor */
    }
    rev->offsets[corpus->entry_count] = running;
    /* Phase C: fill flat array. Ensure allocation size > 0 even for empty
     * corpora (avoids malloc(0) which is implementation-defined). */
    size_t flat_bytes = (total > 0 ? (size_t)total : SKIP_ONE) * sizeof(cooccur_pos_t);
    rev->flat = malloc(flat_bytes);
    if (!rev->flat) {
        free(rev->offsets);
        free(counts);
        free(rev);
        return NULL;
    }
    for (int d = 0; d < corpus->doc_count; d++) {
        int *ids = corpus->doc_token_ids[d];
        int len = corpus->doc_token_counts[d];
        for (int i = 0; i < len; i++) {
            int tid = ids[i];
            if (tid >= 0 && tid < corpus->entry_count) {
                int slot = rev->offsets[tid] + counts[tid]++;
                rev->flat[slot].doc_id = (int32_t)d;
                rev->flat[slot].pos = (int32_t)i;
            }
        }
    }
    free(counts);
    return rev;
}

static void free_reverse_index(reverse_index_t *rev) {
    if (!rev) {
        return;
    }
    free(rev->offsets);
    free(rev->flat);
    free(rev);
}

/* Bundle of parameters shared by the finalize sub-phases. */
typedef struct {
    cbm_sem_corpus_t *corpus;
    reverse_index_t *rev;
    cbm_sem_src_entry_t *src_entries;
    int worker_count;
    int num_chunks;
    int chunk_size;
    int tile_size;
    cbm_parallel_for_opts_t opts;
} finalize_params_t;

/* Sub-phase 1: build tagged source vectors (sparse or dense-int8) in parallel. */
static void finalize_build_sources(finalize_params_t *p) {
    src_build_ctx_t sc = {
        .entries = p->corpus->entries,
        .src_entries = p->src_entries,
        .entry_count = p->corpus->entry_count,
    };
    atomic_init(&sc.next_idx, 0);
    cbm_parallel_for(p->worker_count, src_build_worker, &sc, p->opts);
}

/* Sub-phases 2+3: co-occurrence pass 1 + normalize. */
static void finalize_pass1(finalize_params_t *p) {
    cooccur_sparse_ctx_t cc = {
        .entries = p->corpus->entries,
        .src_entries = p->src_entries,
        .doc_token_ids = p->corpus->doc_token_ids,
        .doc_token_counts = p->corpus->doc_token_counts,
        .rev = p->rev,
        .doc_count = p->corpus->doc_count,
        .entry_count = p->corpus->entry_count,
        .num_chunks = p->num_chunks,
        .chunk_size = p->chunk_size,
        .tile_size = p->tile_size,
    };
    atomic_init(&cc.next_chunk, 0);
    cbm_parallel_for(p->worker_count, cooccur_worker_sparse, &cc, p->opts);

    norm_ctx_t nc = {.entries = p->corpus->entries, .entry_count = p->corpus->entry_count};
    atomic_init(&nc.next_idx, 0);
    cbm_parallel_for(p->worker_count, normalize_worker, &nc, p->opts);
}

/* Sub-phases 4+5: quantize pass1 to int8, run RRI pass 2, blend + normalize. */
static void finalize_pass2(finalize_params_t *p) {
    int8_t *pass1_q = malloc((size_t)p->corpus->entry_count * CBM_SEM_DIM * sizeof(int8_t));
    if (pass1_q) {
        pass1_quant_ctx_t qc = {
            .entries = p->corpus->entries,
            .pass1_q = pass1_q,
            .entry_count = p->corpus->entry_count,
        };
        atomic_init(&qc.next_idx, 0);
        cbm_parallel_for(p->worker_count, pass1_quantize_worker, &qc, p->opts);
    }

    cbm_sem_vec_t *pass1 = malloc((size_t)p->corpus->entry_count * sizeof(cbm_sem_vec_t));
    if (pass1) {
        for (int i = 0; i < p->corpus->entry_count; i++) {
            pass1[i] = p->corpus->entries[i].enriched_vec;
        }
    }

    if (pass1_q) {
        cooccur_int8_ctx_t cc = {
            .entries = p->corpus->entries,
            .pass1_q = pass1_q,
            .doc_token_ids = p->corpus->doc_token_ids,
            .doc_token_counts = p->corpus->doc_token_counts,
            .rev = p->rev,
            .doc_count = p->corpus->doc_count,
            .entry_count = p->corpus->entry_count,
            .num_chunks = p->num_chunks,
            .chunk_size = p->chunk_size,
            .tile_size = p->tile_size,
        };
        atomic_init(&cc.next_chunk, 0);
        cbm_parallel_for(p->worker_count, cooccur_worker_int8, &cc, p->opts);
    }

    if (pass1) {
        blend_ctx_t bc = {
            .entries = p->corpus->entries,
            .pass1 = pass1,
            .entry_count = p->corpus->entry_count,
        };
        atomic_init(&bc.next_idx, 0);
        cbm_parallel_for(p->worker_count, blend_worker, &bc, p->opts);
        free(pass1);
    }
    free(pass1_q);

    norm_ctx_t nc = {.entries = p->corpus->entries, .entry_count = p->corpus->entry_count};
    atomic_init(&nc.next_idx, 0);
    cbm_parallel_for(p->worker_count, normalize_worker, &nc, p->opts);
}

void cbm_sem_corpus_finalize(cbm_sem_corpus_t *corpus) {
    if (!corpus || corpus->finalized) {
        return;
    }

    /* Eager init before parallel dispatch to avoid lazy-init races */
    ensure_pretrained_map();

    int worker_count = cbm_default_worker_count(false);
    cbm_parallel_for_opts_t opts = {.max_workers = worker_count, .force_pthreads = false};

    /* Finer chunks = better load balancing for skewed token distributions. */
    int num_chunks = worker_count * CBM_SEM_COOCCUR_CHUNK;
    if (num_chunks > corpus->entry_count) {
        num_chunks = corpus->entry_count;
    }
    if (num_chunks < SKIP_ONE) {
        num_chunks = SKIP_ONE;
    }
    int chunk_size = (corpus->entry_count + num_chunks - SKIP_ONE) / num_chunks;

    reverse_index_t *rev = build_reverse_index(corpus);
    if (!rev) {
        corpus->finalized = true;
        return;
    }
    cbm_sem_src_entry_t *src_entries =
        calloc((size_t)corpus->entry_count, sizeof(cbm_sem_src_entry_t));
    if (!src_entries) {
        free_reverse_index(rev);
        corpus->finalized = true;
        return;
    }

    finalize_params_t params = {
        .corpus = corpus,
        .rev = rev,
        .src_entries = src_entries,
        .worker_count = worker_count,
        .num_chunks = num_chunks,
        .chunk_size = chunk_size,
        .tile_size = CBM_SEM_TILE_SIZE,
        .opts = opts,
    };
    finalize_build_sources(&params);
    finalize_pass1(&params);
    finalize_pass2(&params);

    free(src_entries);
    free_reverse_index(rev);
    corpus->finalized = true;
}

/* Parse a decimal index string via strtol, returning CBM_NOT_FOUND on parse
 * failure.  Centralizes the atoi-replacement pattern for token_map lookups. */
static int parse_token_index(const char *idx_str) {
    if (!idx_str) {
        return CBM_NOT_FOUND;
    }
    char *end = NULL;
    long parsed = strtol(idx_str, &end, BASE_DECIMAL);
    return (end != idx_str) ? (int)parsed : CBM_NOT_FOUND;
}

float cbm_sem_corpus_idf(const cbm_sem_corpus_t *corpus, const char *token) {
    if (!corpus || !token || corpus->doc_count == 0) {
        return 0.0F;
    }
    int idx = parse_token_index(cbm_ht_get(corpus->token_map, token));
    if (idx < 0 || idx >= corpus->entry_count) {
        return 0.0F;
    }
    int df = corpus->entries[idx].doc_freq;
    if (df <= 0) {
        return 0.0F;
    }
    return logf((float)corpus->doc_count / (float)df);
}

const cbm_sem_vec_t *cbm_sem_corpus_ri_vec(const cbm_sem_corpus_t *corpus, const char *token) {
    if (!corpus || !token) {
        return NULL;
    }
    int idx = parse_token_index(cbm_ht_get(corpus->token_map, token));
    if (idx < 0 || idx >= corpus->entry_count) {
        return NULL;
    }
    return &corpus->entries[idx].enriched_vec;
}

int cbm_sem_corpus_doc_count(const cbm_sem_corpus_t *corpus) {
    return corpus ? corpus->doc_count : 0;
}

int cbm_sem_corpus_token_count(const cbm_sem_corpus_t *corpus) {
    return corpus ? corpus->entry_count : 0;
}

const char *cbm_sem_corpus_token_at(const cbm_sem_corpus_t *corpus, int index,
                                    const cbm_sem_vec_t **out_vec, float *out_idf) {
    if (!corpus || index < 0 || index >= corpus->entry_count) {
        return NULL;
    }
    if (out_vec) {
        *out_vec = &corpus->entries[index].enriched_vec;
    }
    if (out_idf && corpus->doc_count > 0) {
        int df = corpus->entries[index].doc_freq;
        *out_idf = df > 0 ? logf((float)corpus->doc_count / (float)df) : 0.0F;
    }
    return corpus->entries[index].token;
}

static void free_ht_kv(const char *key, void *value, void *userdata) {
    (void)userdata;
    free((void *)key);
    free(value);
}

void cbm_sem_corpus_free(cbm_sem_corpus_t *corpus) {
    if (!corpus) {
        return;
    }
    for (int i = 0; i < corpus->entry_count; i++) {
        free(corpus->entries[i].token);
    }
    free(corpus->entries);
    for (int d = 0; d < corpus->doc_count; d++) {
        free(corpus->doc_token_ids[d]);
    }
    free(corpus->doc_token_ids);
    free(corpus->doc_token_counts);
    if (corpus->token_map) {
        cbm_ht_foreach(corpus->token_map, free_ht_kv, NULL);
        cbm_ht_free(corpus->token_map);
    }
    free(corpus);
}

/* ── Combined scoring ────────────────────────────────────────────── */

float cbm_sem_proximity(const char *path_a, const char *path_b) {
    if (!path_a || !path_b) {
        return CBM_SEM_UNIT_POS;
    }
    /* Count shared directory components */
    int shared = 0;
    int total_a = 0;
    int total_b = 0;
    const char *a = path_a;
    const char *b = path_b;
    while (*a && *b && *a == *b) {
        if (*a == '/') {
            shared++;
        }
        a++;
        b++;
    }
    for (const char *p = path_a; *p; p++) {
        if (*p == '/') {
            total_a++;
        }
    }
    for (const char *p = path_b; *p; p++) {
        if (*p == '/') {
            total_b++;
        }
    }
    int max_total = total_a > total_b ? total_a : total_b;
    if (max_total == 0) {
        return CBM_SEM_UNIT_POS;
    }
    float ratio = (float)shared / (float)max_total;
    /* Same file = 1.10, same dir = 1.05, distant = 1.00 */
    return CBM_SEM_UNIT_POS + (ratio * CBM_SEM_PROX_MAX_BOOST);
}

/* Cosine similarity for small float arrays (AST profile). */
static float small_cosine(const float *a, const float *b, int dims) {
    float dot = 0.0F;
    float ma = 0.0F;
    float mb = 0.0F;
    for (int i = 0; i < dims; i++) {
        dot += a[i] * b[i];
        ma += a[i] * a[i];
        mb += b[i] * b[i];
    }
    float denom = sqrtf(ma) * sqrtf(mb);
    return denom < CBM_SEM_DENOM_EPS ? 0.0F : dot / denom;
}

/* Sparse cosine over two pre-sorted (index, weight) vectors.  Returns 0 when
 * either side is empty or the magnitude product is below the epsilon guard. */
static float sparse_tfidf_cosine(const cbm_sem_func_t *a, const cbm_sem_func_t *b) {
    if (a->tfidf_len <= 0 || b->tfidf_len <= 0) {
        return 0.0F;
    }
    float dot = 0.0F;
    float ma = 0.0F;
    float mb = 0.0F;
    int ia = 0;
    int ib = 0;
    while (ia < a->tfidf_len && ib < b->tfidf_len) {
        if (a->tfidf_indices[ia] == b->tfidf_indices[ib]) {
            dot += a->tfidf_weights[ia] * b->tfidf_weights[ib];
            ia++;
            ib++;
        } else if (a->tfidf_indices[ia] < b->tfidf_indices[ib]) {
            ia++;
        } else {
            ib++;
        }
    }
    for (int i = 0; i < a->tfidf_len; i++) {
        ma += a->tfidf_weights[i] * a->tfidf_weights[i];
    }
    for (int i = 0; i < b->tfidf_len; i++) {
        mb += b->tfidf_weights[i] * b->tfidf_weights[i];
    }
    float denom = sqrtf(ma) * sqrtf(mb);
    return denom > CBM_SEM_DENOM_EPS ? (dot / denom) : 0.0F;
}

float cbm_sem_combined_score(const cbm_sem_func_t *a, const cbm_sem_func_t *b,
                             const cbm_sem_config_t *cfg) {
    if (!a || !b || !cfg) {
        return 0.0F;
    }

    /* Short-circuit: if MinHash Jaccard is already above the SIMILAR_TO threshold,
     * the pass_similarity pipeline already emitted a SIMILAR_TO edge for this pair.
     * Returning 0 here avoids flooding top-k with cross-service copy-paste boilerplate
     * (logging_middleware, shared push/pull handlers) that SIMILAR_TO already covers,
     * freeing the edge budget for true semantic leaps (vocabulary-bridged relations). */
    if (a->has_minhash && b->has_minhash) {
        double early_j = cbm_minhash_jaccard((const cbm_minhash_t *)a->minhash,
                                             (const cbm_minhash_t *)b->minhash);
        if (early_j >= CBM_MINHASH_JACCARD_THRESHOLD) {
            return 0.0F;
        }
    }

    float score = cfg->w_tfidf * sparse_tfidf_cosine(a, b);

    /* Signal 2: Random Indexing */
    score += cfg->w_ri * cbm_sem_cosine(&a->ri_vec, &b->ri_vec);

    /* Signal 3: MinHash Jaccard */
    if (a->has_minhash && b->has_minhash) {
        double j = cbm_minhash_jaccard((const cbm_minhash_t *)a->minhash,
                                       (const cbm_minhash_t *)b->minhash);
        score += cfg->w_minhash * (float)j;
    }

    /* Signal 4: API Signatures */
    score += cfg->w_api * cbm_sem_cosine(&a->api_vec, &b->api_vec);

    /* Signal 5: Type Signatures */
    score += cfg->w_type * cbm_sem_cosine(&a->type_vec, &b->type_vec);

    /* Signal 7: Decorator Pattern */
    score += cfg->w_decorator * cbm_sem_cosine(&a->deco_vec, &b->deco_vec);

    /* Signal 8+9+11: Structural profile + data flow + Halstead */
    float sp_score = small_cosine(a->struct_profile, b->struct_profile, CBM_SEM_AST_PROFILE_DIMS);
    score += cfg->w_struct_profile * sp_score;

    /* Signal 6: Module proximity (multiplier, not additive).
     * Proximity returns [1.0, 1.10] — a same-file/same-dir boost for ranking.
     * Clamp the final product to [0, 1] so the output stays within valid
     * cosine-similarity range.  Without the clamp, a 0.95 base × 1.10 proximity
     * would emit a 1.045 score which violates the semantic of "similarity" and
     * breaks downstream consumers that expect a normalized value. */
    score *= cbm_sem_proximity(a->file_path, b->file_path);
    if (score > CBM_SEM_UNIT_POS) {
        score = CBM_SEM_UNIT_POS;
    }
    if (score < 0.0F) {
        score = 0.0F;
    }

    return score;
}

/* ── Graph diffusion ─────────────────────────────────────────────── */

void cbm_sem_diffuse(cbm_sem_vec_t *combined, const cbm_sem_vec_t *neighbors, int neighbor_count,
                     float alpha) {
    if (!combined || !neighbors || neighbor_count <= 0) {
        return;
    }
    /* Blend: combined = (1-α) × combined + α × mean(neighbors) */
    cbm_sem_vec_t mean;
    memset(&mean, 0, sizeof(mean));
    for (int n = 0; n < neighbor_count; n++) {
        for (int i = 0; i < CBM_SEM_DIM; i++) {
            mean.v[i] += neighbors[n].v[i];
        }
    }
    float inv_n = CBM_SEM_UNIT_POS / (float)neighbor_count;
    float one_minus_alpha = CBM_SEM_UNIT_POS - alpha;
    for (int i = 0; i < CBM_SEM_DIM; i++) {
        combined->v[i] = (one_minus_alpha * combined->v[i]) + (alpha * mean.v[i] * inv_n);
    }
    cbm_sem_normalize(combined);
}
