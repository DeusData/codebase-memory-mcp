/*
 * graph_buffer.c — In-memory graph buffer for pipeline indexing.
 *
 * Uses foundation hash tables for O(1) node lookup by QN and edge dedup.
 * Uses dynamic arrays for ordered iteration and secondary indexes.
 *
 * Memory ownership: each node/edge is individually heap-allocated so that
 * pointers stored in hash tables remain stable when the pointer-array grows.
 * The buffer frees everything in cbm_gbuf_free().
 */
#include "foundation/constants.h"

enum {
    GB_ERR = -1,
    GB_COL_2 = 2,
    GB_COL_3 = 3,
    GB_COL_4 = 4,
    GB_COL_5 = 5,
    GB_COL_6 = 6,
    GB_COL_7 = 7,
    GB_URL_PATH_PREFIX = 12, /* strlen(""url_path":"") */
    GB_MIN_FOR_DEDUP = 2,    /* need at least 2 vectors to sort+dedup */
    GB_DEDUP_LOOKAHEAD = 1,  /* compare current with next element */
};

/* Sentinel for an edge property blob carrying no parseable "confidence": any
 * blob that states one outranks a blob that does not. */
#define CBM_EDGE_CONF_ABSENT (-1.0)
#include "graph_buffer/graph_buffer.h"
#include <yyjson/yyjson.h> // url_path extraction must match json_extract semantics
#include "store/store.h"
#include "sqlite_writer.h"
#include "foundation/hash_table.h"
#include "foundation/compat.h"
#include "foundation/log.h"
#include "foundation/dyn_array.h"
#include "foundation/profile.h"
#include "foundation/mem.h"
#include "foundation/mem_core.h"
#include <sqlite3.h>

#include <stdatomic.h>
#include <stdint.h> // int64_t
#include <stdio.h>
#include <stdlib.h>
#include <string.h> // strdup
#include <time.h>

static inline void *intptr_to_ptr(intptr_t v) {
    void *p;
    memcpy(&p, &v, sizeof(p));
    return p;
}

/* ── Internal types ──────────────────────────────────────────────── */

/* Edge key for dedup hash table — composite key as string "srcID:tgtID:type",
 * plus ":local_name" for IMPORTS edges (#768). 256 bytes fit two int64s, the
 * type and a ~200-char local_name verbatim; longer local_names are re-keyed
 * with a hash of the full name in make_edge_key (never silently truncated). */
#define EDGE_KEY_BUF CBM_SZ_256

/* Per-type or per-key edge list stored in hash tables as values */
typedef struct {
    uint64_t h0;
    uint64_t h1;
    cbm_gbuf_edge_t *edge; /* NULL = empty slot */
} edge_key_slot_t;

typedef struct {
    edge_key_slot_t *slots;
    size_t cap; /* power of two, 0 = not allocated */
    size_t count;
} edge_key_map_t;

typedef CBM_DYN_ARRAY(const cbm_gbuf_edge_t *) edge_ptr_array_t;

/* Per-label or per-name node list */
typedef CBM_DYN_ARRAY(const cbm_gbuf_node_t *) node_ptr_array_t;

struct cbm_gbuf {
    char *project;
    char *root_path;
    int64_t next_id;
    _Atomic int64_t *shared_ids; /* NULL = use next_id, non-NULL = atomic source */

    /* Node storage: array of pointers to individually heap-allocated nodes.
     * This ensures pointers stored in hash tables remain valid when the
     * pointer array reallocs (only the pointer array moves, not the nodes). */
    CBM_DYN_ARRAY(cbm_gbuf_node_t *) nodes;

    /* Primary index: QN → cbm_gbuf_node_t* */
    CBMHashTable *node_by_qn;
    /* Primary index: "id" string → cbm_gbuf_node_t* */
    /* Dense id → node array (ids are sequential from alloc_next_id, shared
     * with edges → holes where edges took ids). Replaces a hash table keyed
     * on STRDUP'D DECIMAL STRINGS of the id — ~0.44 GB of buckets + key
     * strings at kernel scale, plus a snprintf+strdup+hash on every one of
     * the ~18 hot find_by_id call sites. */
    cbm_gbuf_node_t **by_id;
    /* Worker buffers (cbm_gbuf_new_worker) keep no by_id array: their ids
     * come from the shared counter, so a dense array would span the whole
     * global id space in every worker and double in lockstep. */
    bool by_id_off;
    int64_t by_id_cap;

    /* Secondary node indexes */
    CBMHashTable *nodes_by_label; /* key: label, value: (node_ptr_array_t*) */
    CBMHashTable *nodes_by_name;  /* key: name, value: (node_ptr_array_t*) */

    /* Edge storage: array of pointers to individually heap-allocated edges */
    CBM_DYN_ARRAY(cbm_gbuf_edge_t *) edges;

    /* Edge dedup index: "srcID:tgtID:type" → cbm_gbuf_edge_t* */
    edge_key_map_t edge_by_key; /* dedup: 128-bit key hash -> edge */

    /* Edge secondary indexes: composite keys → edge_ptr_array_t */
    CBMHashTable *edges_by_source_type; /* "srcID:type" → edge_ptr_array_t* */
    CBMHashTable *edges_by_target_type; /* "tgtID:type" → edge_ptr_array_t* */
    CBMHashTable *edges_by_type;        /* "type" → edge_ptr_array_t* */

    /* String intern pool for highly-repetitive fields (node label/file_path,
     * edge type). Maps string content → owned canonical copy, collapsing
     * O(nodes+edges) duplicate allocations to O(distinct). The pool owns the
     * copies; interned pointers are stable for the buffer lifetime and are NOT
     * freed by free_node_strings/free_edge_strings — only once in cbm_gbuf_free. */
    CBMHashTable *intern_pool;

    /* Vector storage for semantic embeddings (filled by pass_semantic_edges,
     * consumed by cbm_write_db during dump). */
    CBMDumpVector *dump_vectors;
    int dump_vector_count;
    int dump_vector_cap;

    /* Token vector storage for enriched RI vectors (query-time lookup). */
    CBMDumpTokenVec *dump_token_vecs;
    int dump_token_vec_count;
    int dump_token_vec_cap;
};

/* ── Helpers ─────────────────────────────────────────────────────── */

static char *heap_strdup(const char *s) {
    return cbm_mem_strdup(CBM_MEM_CLASS_GBUF_STRING, s ? s : "{}");
}

/* Intern a repetitive string into the buffer's pool: identical content collapses
 * to a single heap copy owned by the pool. NULL maps to "{}" (matches
 * heap_strdup). The returned pointer is stable for the buffer's lifetime and
 * must never be freed or mutated by callers. Returns NULL only on OOM. */
static const char *gb_intern(cbm_gbuf_t *gb, const char *s) {
    const char *key = s ? s : "{}";
    const char *found = cbm_ht_get(gb->intern_pool, key);
    if (found) {
        return found;
    }
    char *copy = cbm_mem_strdup(CBM_MEM_CLASS_GBUF_STRING, key);
    if (copy) {
        cbm_ht_set(gb->intern_pool, copy, copy); /* key == value == owned copy */
    }
    return copy;
}

static void make_id_key(char *buf, size_t bufsz, int64_t id) {
    snprintf(buf, bufsz, "%lld", (long long)id);
}

/* FNV-1a 64-bit over a byte slice — for re-keying oversized local_names. */
static uint64_t fnv1a64(const char *s, size_t len) {
    uint64_t h = 14695981039346656037ULL;
    for (size_t i = 0; i < len; i++) {
        h ^= (uint8_t)s[i];
        h *= 1099511628211ULL;
    }
    return h;
}

/* IMPORTS edges carry exactly one imported symbol's local_name (#768): two
 * named imports from the same specifier resolve to the same (source,
 * target) pair but are distinct symbols. Key on local_name too so the
 * second import doesn't dedup-collide with and overwrite the first —
 * every pass that walks IMPORTS edges (pass_calls.c, pass_usages.c,
 * pass_semantic.c, pass_lsp_cross.c) expects one local_name per edge, so
 * losing an edge here silently breaks cross-file call resolution for
 * whichever symbol got dropped, not just "who imports X" queries. Other
 * edge types keep the plain (source,target,type) key: collapsing repeat
 * edges of the same type between the same two nodes (e.g. multiple call
 * sites) into one is the existing, intended dedup behavior there.
 *
 * A local_name too long for the key buffer is re-keyed with an FNV-1a hash
 * of the FULL name instead of being truncated — a truncated key would
 * collide two long names sharing a prefix and silently drop an edge again.
 * The hash key is prefixed with byte 0x01, which cannot appear in the raw
 * JSON slice (control characters must be \u-escaped in JSON), so hash keys
 * can never collide with verbatim keys. */
static void make_edge_key(char *buf, size_t bufsz, int64_t src, int64_t tgt, const char *type,
                          const char *properties_json) {
    if (properties_json && strcmp(type, "IMPORTS") == 0) {
        static const char local_name_key[] = "\"local_name\":\"";
        const char *ln = strstr(properties_json, local_name_key);
        if (ln) {
            ln += sizeof(local_name_key) - 1;
            const char *end = strchr(ln, '"');
            size_t ln_len = end ? (size_t)(end - ln) : strlen(ln);
            int n = snprintf(buf, bufsz, "%lld:%lld:%s:%.*s", (long long)src, (long long)tgt, type,
                             (int)ln_len, ln);
            if (n < 0 || (size_t)n >= bufsz) {
                snprintf(buf, bufsz, "%lld:%lld:%s:\x01%016llx", (long long)src, (long long)tgt,
                         type, (unsigned long long)fnv1a64(ln, ln_len));
            }
            return;
        }
    }
    snprintf(buf, bufsz, "%lld:%lld:%s", (long long)src, (long long)tgt, type);
}

static void make_src_type_key(char *buf, size_t bufsz, int64_t src, const char *type) {
    snprintf(buf, bufsz, "%lld:%s", (long long)src, type);
}

/* ── Edge dedup index: 128-bit key hash -> edge, open addressing ──────
 *
 * The dedup key is "src:tgt:type:props" (make_edge_key). Keeping it as a
 * string per edge cost the kernel 15.8M heap strings (~1.2 GB live, and the
 * same again transiently in worker buffers during merge). Two independent
 * 64-bit FNV-1a hashes of the composed key take 16 bytes; a false match needs
 * both to collide, ~n^2 / 2^129 for n edges -- below any other failure mode of
 * the index. Linear probing, backward-shift deletion (no tombstones), load
 * kept under 1/2. Values are the edge pointers (stable heap records). */

static void edge_key_hash(const char *key, uint64_t *h0, uint64_t *h1) {
    uint64_t a = 1469598103934665603ULL;
    uint64_t b = 0x9E3779B97F4A7C15ULL;
    for (const unsigned char *p = (const unsigned char *)key; *p; p++) {
        a ^= *p;
        a *= 1099511628211ULL;
        b = (b ^ *p) * 0xFF51AFD7ED558CCDULL;
        b ^= b >> 29;
    }
    *h0 = a;
    *h1 = b ^ (a >> 17);
}

static void edge_key_map_free(edge_key_map_t *m) {
    cbm_free(CBM_MEM_CLASS_GBUF_INDEX, m->slots);
    m->slots = NULL;
    m->cap = 0;
    m->count = 0;
}

static edge_key_slot_t *edge_key_map_find(const edge_key_map_t *m, uint64_t h0, uint64_t h1) {
    if (!m->slots) {
        return NULL;
    }
    size_t mask = m->cap - SKIP_ONE;
    for (size_t i = (size_t)h0 & mask;; i = (i + SKIP_ONE) & mask) {
        edge_key_slot_t *s = &m->slots[i];
        if (!s->edge) {
            return NULL;
        }
        if (s->h0 == h0 && s->h1 == h1) {
            return s;
        }
    }
}

static bool edge_key_map_grow(edge_key_map_t *m) {
    size_t new_cap = m->cap ? m->cap * PAIR_LEN : CBM_SZ_1K;
    edge_key_slot_t *slots = cbm_calloc(CBM_MEM_CLASS_GBUF_INDEX, new_cap * sizeof(*slots));
    if (!slots) {
        return false;
    }
    size_t mask = new_cap - SKIP_ONE;
    for (size_t i = 0; i < m->cap; i++) {
        edge_key_slot_t *s = &m->slots[i];
        if (!s->edge) {
            continue;
        }
        size_t j = (size_t)s->h0 & mask;
        while (slots[j].edge) {
            j = (j + SKIP_ONE) & mask;
        }
        slots[j] = *s;
    }
    cbm_free(CBM_MEM_CLASS_GBUF_INDEX, m->slots);
    m->slots = slots;
    m->cap = new_cap;
    return true;
}

static bool edge_key_map_set(edge_key_map_t *m, uint64_t h0, uint64_t h1, cbm_gbuf_edge_t *edge) {
    if ((m->count + SKIP_ONE) * PAIR_LEN > m->cap && !edge_key_map_grow(m)) {
        return false;
    }
    size_t mask = m->cap - SKIP_ONE;
    size_t i = (size_t)h0 & mask;
    while (m->slots[i].edge) {
        if (m->slots[i].h0 == h0 && m->slots[i].h1 == h1) {
            m->slots[i].edge = edge;
            return true;
        }
        i = (i + SKIP_ONE) & mask;
    }
    m->slots[i].h0 = h0;
    m->slots[i].h1 = h1;
    m->slots[i].edge = edge;
    m->count++;
    return true;
}

static void edge_key_map_delete(edge_key_map_t *m, uint64_t h0, uint64_t h1) {
    edge_key_slot_t *s = edge_key_map_find(m, h0, h1);
    if (!s) {
        return;
    }
    size_t mask = m->cap - SKIP_ONE;
    size_t i = (size_t)(s - m->slots);
    m->slots[i].edge = NULL;
    m->count--;
    /* Backward shift: pull later entries of the same probe run into the hole. */
    for (size_t j = (i + SKIP_ONE) & mask; m->slots[j].edge; j = (j + SKIP_ONE) & mask) {
        size_t home = (size_t)m->slots[j].h0 & mask;
        /* entry at j may move to i if its home is not in (i, j] cyclically */
        bool movable = (i <= j) ? (home <= i || home > j) : (home <= i && home > j);
        if (movable) {
            m->slots[i] = m->slots[j];
            m->slots[j].edge = NULL;
            i = j;
        }
    }
}

/* Get or create a node_ptr_array_t in a hash table */
static void node_array_push(node_ptr_array_t *arr, const cbm_gbuf_node_t *node);

static node_ptr_array_t *get_or_create_node_array(CBMHashTable *ht, const char *key) {
    if (!ht) {
        return NULL; /* worker buffer: no secondary indexes (cbm_gbuf_new_worker) */
    }
    node_ptr_array_t *arr = cbm_ht_get(ht, key);
    if (!arr) {
        arr = cbm_calloc(CBM_MEM_CLASS_GBUF_INDEX, sizeof(node_ptr_array_t));
        cbm_ht_set(ht, cbm_mem_strdup(CBM_MEM_CLASS_GBUF_INDEX, key), arr);
    }
    return arr;
}

/* Get or create an edge_ptr_array_t in a hash table */
static edge_ptr_array_t *get_or_create_edge_array(CBMHashTable *ht, const char *key) {
    if (!ht) {
        return NULL;
    }
    edge_ptr_array_t *arr = cbm_ht_get(ht, key);
    if (!arr) {
        arr = cbm_calloc(CBM_MEM_CLASS_GBUF_INDEX, sizeof(edge_ptr_array_t));
        cbm_ht_set(ht, cbm_mem_strdup(CBM_MEM_CLASS_GBUF_INDEX, key), arr);
    }
    return arr;
}

/* Free a node_ptr_array_t (callback for hash table iteration) */
static void free_node_array(const char *key, void *value, void *ud) {
    (void)ud;
    node_ptr_array_t *arr = value;
    if (arr) {
        cbm_da_free(arr);
        cbm_free(CBM_MEM_CLASS_GBUF_INDEX, arr);
    }
    cbm_free(CBM_MEM_CLASS_GBUF_INDEX, (void *)key);
}

/* Free an edge_ptr_array_t (callback) */
static void free_edge_array(const char *key, void *value, void *ud) {
    (void)ud;
    edge_ptr_array_t *arr = value;
    if (arr) {
        cbm_da_free(arr);
        cbm_free(CBM_MEM_CLASS_GBUF_INDEX, arr);
    }
    cbm_free(CBM_MEM_CLASS_GBUF_INDEX, (void *)key);
}

/* Free index keys only (deleted_set id keys). */
static void free_index_key(const char *key, void *value, void *ud) {
    (void)value;
    (void)ud;
    cbm_free(CBM_MEM_CLASS_GBUF_INDEX, (void *)key);
}

/* Free one intern-pool entry: key == value == the single owned string copy
 * that nodes/edges borrowed for label/file_path/type. */
static void free_intern_entry(const char *key, void *value, void *ud) {
    (void)value;
    (void)ud;
    cbm_free(CBM_MEM_CLASS_GBUF_STRING, (void *)key);
}

/* Free a single node's owned strings. label and file_path are interned
 * (pool-owned) — NOT freed here; the pool frees them once in cbm_gbuf_free. */
static void free_node_strings(cbm_gbuf_node_t *n) {
    cbm_free(CBM_MEM_CLASS_GBUF_STRING, n->name);
    cbm_free(CBM_MEM_CLASS_GBUF_STRING, n->qualified_name);
    cbm_free(CBM_MEM_CLASS_GBUF_STRING, n->properties_json);
}

/* An edge owns no strings: type AND properties_json are interned (pool-owned;
 * the pool frees each once in cbm_gbuf_free). Edge properties repeat massively
 * -- on the Go corpus 1.8M edges carry 343k distinct property strings and 854k
 * are exactly "{}" -- and an edge's properties are never mutated in place. */
static void free_edge_strings(cbm_gbuf_edge_t *e) {
    (void)e;
}

/* Allocate the next buffer-local or shared-atomic ID. */
static int64_t alloc_next_id(cbm_gbuf_t *gb) {
    if (gb->shared_ids) {
        return atomic_fetch_add_explicit(gb->shared_ids, SKIP_ONE, memory_order_relaxed);
    }
    return gb->next_id++;
}

/* Swap-remove an edge from a pointer array by ID. */
static void remove_edge_from_ptr_array(edge_ptr_array_t *arr, int64_t edge_id) {
    if (!arr) {
        return;
    }
    for (int j = 0; j < arr->count; j++) {
        if (arr->items[j]->id == edge_id) {
            arr->items[j] = arr->items[--arr->count];
            return;
        }
    }
}

/* Swap-remove a node from a node_ptr_array by ID. */
static void remove_node_from_ptr_array(node_ptr_array_t *arr, int64_t node_id) {
    if (!arr) {
        return;
    }
    for (int j = 0; j < arr->count; j++) {
        if (arr->items[j]->id == node_id) {
            arr->items[j] = arr->items[--arr->count];
            return;
        }
    }
}

/* Remove an edge from all indexes (dedup + source_type + target_type + type). */
static void unindex_edge(cbm_gbuf_t *gb, const cbm_gbuf_edge_t *e) {
    char key[EDGE_KEY_BUF];

    make_edge_key(key, sizeof(key), e->source_id, e->target_id, e->type, e->properties_json);
    uint64_t h0;
    uint64_t h1;
    edge_key_hash(key, &h0, &h1);
    edge_key_map_delete(&gb->edge_by_key, h0, h1);

    make_src_type_key(key, sizeof(key), e->source_id, e->type);
    remove_edge_from_ptr_array(cbm_ht_get(gb->edges_by_source_type, key), e->id);

    make_src_type_key(key, sizeof(key), e->target_id, e->type);
    remove_edge_from_ptr_array(cbm_ht_get(gb->edges_by_target_type, key), e->id);

    remove_edge_from_ptr_array(cbm_ht_get(gb->edges_by_type, e->type), e->id);
}

/* Cascade-delete all edges touching nodes in deleted_set. */
static void cascade_delete_edges(cbm_gbuf_t *gb, CBMHashTable *deleted_set) {
    int write_idx = 0;
    for (int i = 0; i < gb->edges.count; i++) {
        cbm_gbuf_edge_t *e = gb->edges.items[i];
        char src_id[CBM_SZ_32];
        char tgt_id[CBM_SZ_32];
        make_id_key(src_id, sizeof(src_id), e->source_id);
        make_id_key(tgt_id, sizeof(tgt_id), e->target_id);

        if (cbm_ht_get(deleted_set, src_id) || cbm_ht_get(deleted_set, tgt_id)) {
            unindex_edge(gb, e);
            free_edge_strings(e);
            cbm_free(CBM_MEM_CLASS_GBUF_EDGE, e);
        } else {
            gb->edges.items[write_idx++] = gb->edges.items[i];
        }
    }
    gb->edges.count = write_idx;
}

/* Register a node in primary (QN, ID) and secondary (label, name) indexes. */
static void register_node_in_indexes(cbm_gbuf_t *gb, cbm_gbuf_node_t *node) {
    cbm_ht_set(gb->node_by_qn, node->qualified_name, node);

    if (!gb->by_id_off) {
        if (node->id >= gb->by_id_cap) {
            int64_t nc = gb->by_id_cap > 0 ? gb->by_id_cap : CBM_SZ_1K;
            while (nc <= node->id) {
                nc *= 2;
            }
            cbm_gbuf_node_t **grown =
                cbm_realloc(CBM_MEM_CLASS_GBUF_INDEX, gb->by_id, (size_t)nc * sizeof(*grown));
            if (grown) {
                memset(grown + gb->by_id_cap, 0, (size_t)(nc - gb->by_id_cap) * sizeof(*grown));
                gb->by_id = grown;
                gb->by_id_cap = nc;
            }
        }
        if (node->id >= 0 && node->id < gb->by_id_cap) {
            gb->by_id[node->id] = node;
        }
    }

    node_ptr_array_t *by_label =
        get_or_create_node_array(gb->nodes_by_label, node->label ? node->label : "");
    node_array_push(by_label, node);

    node_ptr_array_t *by_name =
        get_or_create_node_array(gb->nodes_by_name, node->name ? node->name : "");
    node_array_push(by_name, node);
}

/* Per-key index arrays start at two slots: on the kernel the secondary
 * indexes hold ~18M of these arrays and most keys have one or two entries,
 * so the default 8-slot start was 1 GB of empty headroom. NULL = the buffer
 * has no secondary indexes (worker buffers). */
enum { GB_INDEX_ARRAY_FIRST_CAP = 2 };
static void edge_array_push(edge_ptr_array_t *arr, const cbm_gbuf_edge_t *edge) {
    if (!arr) {
        return;
    }
    cbm_da_push_min(arr, edge, GB_INDEX_ARRAY_FIRST_CAP);
}
static void node_array_push(node_ptr_array_t *arr, const cbm_gbuf_node_t *node) {
    if (!arr) {
        return;
    }
    cbm_da_push_min(arr, node, GB_INDEX_ARRAY_FIRST_CAP);
}

/* Index an edge by one key into a hash table bucket. */
static void index_edge_by_key(CBMHashTable *ht, const char *key, cbm_gbuf_edge_t *edge) {
    edge_ptr_array_t *arr = get_or_create_edge_array(ht, key);
    edge_array_push(arr, (const cbm_gbuf_edge_t *)edge);
}

/* Register an edge in secondary indexes (source_type, target_type, type). */
static void register_edge_in_indexes(cbm_gbuf_t *gb, cbm_gbuf_edge_t *edge) {
    char key[EDGE_KEY_BUF];

    make_src_type_key(key, sizeof(key), edge->source_id, edge->type);
    index_edge_by_key(gb->edges_by_source_type, key, edge);

    make_src_type_key(key, sizeof(key), edge->target_id, edge->type);
    index_edge_by_key(gb->edges_by_target_type, key, edge);

    index_edge_by_key(gb->edges_by_type, edge->type, edge);
}

/* Rebuild edge secondary indexes from scratch (after bulk deletion). */
static void rebuild_edge_secondary_indexes(cbm_gbuf_t *gb) {
    if (!gb->edges_by_type) {
        return; /* worker buffer: nothing to rebuild */
    }
    cbm_ht_foreach(gb->edges_by_source_type, free_edge_array, NULL);
    cbm_ht_free(gb->edges_by_source_type);
    cbm_ht_foreach(gb->edges_by_target_type, free_edge_array, NULL);
    cbm_ht_free(gb->edges_by_target_type);
    cbm_ht_foreach(gb->edges_by_type, free_edge_array, NULL);
    cbm_ht_free(gb->edges_by_type);

    gb->edges_by_source_type = cbm_ht_create_in(CBM_MEM_CLASS_GBUF_INDEX, CBM_SZ_256);
    gb->edges_by_target_type = cbm_ht_create_in(CBM_MEM_CLASS_GBUF_INDEX, CBM_SZ_256);
    gb->edges_by_type = cbm_ht_create_in(CBM_MEM_CLASS_GBUF_INDEX, CBM_SZ_32);

    for (int i = 0; i < gb->edges.count; i++) {
        register_edge_in_indexes(gb, gb->edges.items[i]);
    }
}

/* Release all lookup hash tables (used by dump after building arrays). */
static void release_gbuf_indexes(cbm_gbuf_t *gb) {
    cbm_ht_free(gb->node_by_qn);
    gb->node_by_qn = NULL;
    cbm_free(CBM_MEM_CLASS_GBUF_INDEX, gb->by_id);
    gb->by_id = NULL;
    gb->by_id_cap = 0;
    cbm_ht_foreach(gb->nodes_by_label, free_node_array, NULL);
    cbm_ht_free(gb->nodes_by_label);
    gb->nodes_by_label = NULL;
    cbm_ht_foreach(gb->nodes_by_name, free_node_array, NULL);
    cbm_ht_free(gb->nodes_by_name);
    gb->nodes_by_name = NULL;
    edge_key_map_free(&gb->edge_by_key);
    cbm_ht_foreach(gb->edges_by_source_type, free_edge_array, NULL);
    cbm_ht_free(gb->edges_by_source_type);
    gb->edges_by_source_type = NULL;
    cbm_ht_foreach(gb->edges_by_target_type, free_edge_array, NULL);
    cbm_ht_free(gb->edges_by_target_type);
    gb->edges_by_target_type = NULL;
    cbm_ht_foreach(gb->edges_by_type, free_edge_array, NULL);
    cbm_ht_free(gb->edges_by_type);
    gb->edges_by_type = NULL;
}

/* ── Lifecycle ──────────────────────────────────────────────────── */

cbm_gbuf_t *cbm_gbuf_new(const char *project, const char *root_path) {
    cbm_gbuf_t *gb = cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(cbm_gbuf_t));
    if (!gb) {
        return NULL;
    }

    gb->project = cbm_mem_strdup(CBM_MEM_CLASS_GBUF_STRING, project ? project : "");
    gb->root_path = cbm_mem_strdup(CBM_MEM_CLASS_GBUF_STRING, root_path ? root_path : "");
    gb->next_id = SKIP_ONE;
    gb->shared_ids = NULL;

    gb->node_by_qn = cbm_ht_create_in(CBM_MEM_CLASS_GBUF_INDEX, CBM_SZ_256);
    gb->by_id = NULL;
    gb->by_id_cap = 0;
    gb->nodes_by_label = cbm_ht_create_in(CBM_MEM_CLASS_GBUF_INDEX, CBM_SZ_32);
    gb->nodes_by_name = cbm_ht_create_in(CBM_MEM_CLASS_GBUF_INDEX, CBM_SZ_256);

    gb->edges_by_source_type = cbm_ht_create_in(CBM_MEM_CLASS_GBUF_INDEX, CBM_SZ_256);
    gb->edges_by_target_type = cbm_ht_create_in(CBM_MEM_CLASS_GBUF_INDEX, CBM_SZ_256);
    gb->edges_by_type = cbm_ht_create_in(CBM_MEM_CLASS_GBUF_INDEX, CBM_SZ_32);

    gb->intern_pool = cbm_ht_create_in(CBM_MEM_CLASS_GBUF_STRING, CBM_SZ_1K);

    return gb;
}

cbm_gbuf_t *cbm_gbuf_new_shared_ids(const char *project, const char *root_path,
                                    _Atomic int64_t *id_source) {
    cbm_gbuf_t *gb = cbm_gbuf_new(project, root_path);
    if (gb && id_source) {
        gb->shared_ids = id_source;
    }
    return gb;
}

cbm_gbuf_t *cbm_gbuf_new_worker(const char *project, const char *root_path,
                                _Atomic int64_t *id_source) {
    cbm_gbuf_t *gb = cbm_gbuf_new_shared_ids(project, root_path, id_source);
    if (!gb) {
        return NULL;
    }
    /* A worker buffer is appended to and merged; it is never asked by label,
     * name or edge type. Its secondary indexes were pure cost: on the kernel
     * the index class peaked at 7.1 GB during resolve against 2.5 GB live in
     * the main buffer, the difference being 18 workers' throwaway indexes.
     * node_by_qn and edge_by_key stay: upsert and dedup need them. */
    cbm_ht_free(gb->nodes_by_label);
    cbm_ht_free(gb->nodes_by_name);
    cbm_ht_free(gb->edges_by_source_type);
    cbm_ht_free(gb->edges_by_target_type);
    cbm_ht_free(gb->edges_by_type);
    gb->nodes_by_label = NULL;
    gb->nodes_by_name = NULL;
    gb->edges_by_source_type = NULL;
    gb->edges_by_target_type = NULL;
    gb->edges_by_type = NULL;
    /* Nor by id: with ids from the shared counter, a dense id -> node array
     * in every worker spans the whole global id space (8.5M ids x 8 B x 18
     * workers on the kernel) and all of them double in the same instant --
     * a 1 GB step between two reads of the memory gate, which is where the
     * 15 GB budget was missed (2026-09-14). Nothing asks a worker buffer by
     * id before the merge; the main buffer answers after it. */
    cbm_free(CBM_MEM_CLASS_GBUF_INDEX, gb->by_id);
    gb->by_id = NULL;
    gb->by_id_cap = 0;
    gb->by_id_off = true;
    return gb;
}

void cbm_gbuf_free(cbm_gbuf_t *gb) {
    if (!gb) {
        return;
    }

    /* Free each individually-allocated node */
    for (int i = 0; i < gb->nodes.count; i++) {
        cbm_gbuf_node_t *n = gb->nodes.items[i];
        free_node_strings(n);
        cbm_free(CBM_MEM_CLASS_GBUF_NODE, n);
    }
    cbm_da_free(&gb->nodes);

    /* Free each individually-allocated edge */
    for (int i = 0; i < gb->edges.count; i++) {
        cbm_gbuf_edge_t *e = gb->edges.items[i];
        free_edge_strings(e);
        cbm_free(CBM_MEM_CLASS_GBUF_EDGE, e);
    }
    cbm_da_free(&gb->edges);

    /* Free hash tables — may be NULL if already released by dump_to_sqlite */
    if (gb->node_by_qn) {
        cbm_ht_free(gb->node_by_qn);
    }
    cbm_free(CBM_MEM_CLASS_GBUF_INDEX, gb->by_id);
    if (gb->nodes_by_label) {
        cbm_ht_foreach(gb->nodes_by_label, free_node_array, NULL);
        cbm_ht_free(gb->nodes_by_label);
    }
    if (gb->nodes_by_name) {
        cbm_ht_foreach(gb->nodes_by_name, free_node_array, NULL);
        cbm_ht_free(gb->nodes_by_name);
    }
    edge_key_map_free(&gb->edge_by_key);
    if (gb->edges_by_source_type) {
        cbm_ht_foreach(gb->edges_by_source_type, free_edge_array, NULL);
        cbm_ht_free(gb->edges_by_source_type);
    }
    if (gb->edges_by_target_type) {
        cbm_ht_foreach(gb->edges_by_target_type, free_edge_array, NULL);
        cbm_ht_free(gb->edges_by_target_type);
    }
    if (gb->edges_by_type) {
        cbm_ht_foreach(gb->edges_by_type, free_edge_array, NULL);
        cbm_ht_free(gb->edges_by_type);
    }

    /* Free vector storage */
    for (int i = 0; i < gb->dump_vector_count; i++) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, (void *)gb->dump_vectors[i].vector);
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, gb->dump_vectors);

    /* Free token vector storage */
    for (int i = 0; i < gb->dump_token_vec_count; i++) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, (void *)gb->dump_token_vecs[i].token);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, (void *)gb->dump_token_vecs[i].vector);
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, gb->dump_token_vecs);

    /* Free interned strings (node label/file_path, edge type) — pool owns one
     * copy each (key == value), freed exactly once via free_intern_entry. Done
     * after nodes/edges since they borrowed these pointers. */
    if (gb->intern_pool) {
        cbm_ht_foreach(gb->intern_pool, free_intern_entry, NULL);
        cbm_ht_free(gb->intern_pool);
    }

    cbm_free(CBM_MEM_CLASS_GBUF_STRING, gb->project);
    cbm_free(CBM_MEM_CLASS_GBUF_STRING, gb->root_path);
    cbm_free(CBM_MEM_CLASS_OTHER, gb);
}

/* ── Vector storage ──────────────────────────────────────────────── */

int cbm_gbuf_store_vector(cbm_gbuf_t *gb, int64_t node_id, const uint8_t *vector, int vector_len) {
    if (!gb || !vector || vector_len <= 0) {
        return GB_ERR;
    }
    enum { VEC_INIT_CAP = 1024, VEC_GROW = 2 };
    if (gb->dump_vector_count >= gb->dump_vector_cap) {
        int new_cap =
            gb->dump_vector_cap < VEC_INIT_CAP ? VEC_INIT_CAP : gb->dump_vector_cap * VEC_GROW;
        CBMDumpVector *grown = cbm_realloc(CBM_MEM_CLASS_SEMANTIC, gb->dump_vectors,
                                           (size_t)new_cap * sizeof(CBMDumpVector));
        if (!grown) {
            return GB_ERR;
        }
        gb->dump_vectors = grown;
        gb->dump_vector_cap = new_cap;
    }
    /* Copy vector data */
    uint8_t *vec_copy = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)vector_len);
    if (!vec_copy) {
        return GB_ERR;
    }
    memcpy(vec_copy, vector, (size_t)vector_len);

    gb->dump_vectors[gb->dump_vector_count++] = (CBMDumpVector){
        .node_id = node_id,
        .project = gb->project, /* borrowed — valid until gbuf_free */
        .vector = vec_copy,
        .vector_len = vector_len,
    };
    return 0;
}

int cbm_gbuf_store_token_vector(cbm_gbuf_t *gb, const char *token, const uint8_t *vector,
                                int vector_len, float idf) {
    if (!gb || !token || !vector || vector_len <= 0) {
        return GB_ERR;
    }
    enum { TV_INIT_CAP = 256, TV_GROW = 2 };
    if (gb->dump_token_vec_count >= gb->dump_token_vec_cap) {
        int new_cap =
            gb->dump_token_vec_cap < TV_INIT_CAP ? TV_INIT_CAP : gb->dump_token_vec_cap * TV_GROW;
        CBMDumpTokenVec *grown = cbm_realloc(CBM_MEM_CLASS_SEMANTIC, gb->dump_token_vecs,
                                             (size_t)new_cap * sizeof(CBMDumpTokenVec));
        if (!grown) {
            return GB_ERR;
        }
        gb->dump_token_vecs = grown;
        gb->dump_token_vec_cap = new_cap;
    }
    uint8_t *vec_copy = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)vector_len);
    if (!vec_copy) {
        return GB_ERR;
    }
    memcpy(vec_copy, vector, (size_t)vector_len);

    int idx = gb->dump_token_vec_count;
    gb->dump_token_vecs[idx] = (CBMDumpTokenVec){
        .id = idx + SKIP_ONE, /* 1-based sequential ID */
        .project = gb->project,
        .token = cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, token),
        .vector = vec_copy,
        .vector_len = vector_len,
        .idf = idf,
    };
    gb->dump_token_vec_count++;
    return 0;
}

/* ── ID accessors ────────────────────────────────────────────────── */

int64_t cbm_gbuf_next_id(const cbm_gbuf_t *gb) {
    if (!gb) {
        return SKIP_ONE;
    }
    if (gb->shared_ids) {
        return atomic_load(gb->shared_ids);
    }
    return gb->next_id;
}

void cbm_gbuf_set_next_id(cbm_gbuf_t *gb, int64_t next_id) {
    if (!gb) {
        return;
    }
    gb->next_id = next_id;
}

/* ── Node operations ─────────────────────────────────────────────── */

int64_t cbm_gbuf_upsert_node(cbm_gbuf_t *gb, const char *label, const char *name,
                             const char *qualified_name, const char *file_path, int start_line,
                             int end_line, const char *properties_json) {
    if (!gb || !qualified_name) {
        return 0;
    }

    /* Check if node already exists */
    cbm_gbuf_node_t *existing = cbm_ht_get(gb->node_by_qn, qualified_name);
    if (existing) {
        /* Don't let a per-file "Module" def touch a structural directory node
         * ("Project" root or "Folder"). In a directory-based-module language
         * (Go/Java) a file's module_qn equals its directory QN: a root file →
         * the project name (== the "Project" node's QN); a file in pkg/ →
         * proj.pkg (== the "pkg/" Folder node's QN). Its always-emitted Module
         * def collides here; the directory node is the package/module container
         * and must keep its structural label AND its own name/file_path/range.
         * Updating those in place set the shared node's file_path to whichever
         * same-package file happened to be processed LAST (worker-order
         * dependent) — the nondeterministic file attribution behind #787 — and
         * left the Folder node exposed to delete-nodes-by-file on incremental
         * reindex of that file. Skip the update entirely. (Both the sequential
         * upsert and the parallel local-gbuf merge route through this function.) */
        if (existing->label && label && strcmp(label, "Module") == 0 &&
            (strcmp(existing->label, "Project") == 0 || strcmp(existing->label, "Folder") == 0)) {
            return existing->id;
        }
        /* Same-QN arrival: distinct source entities can share a QN (C: a
         * struct, a function and a macro with one name), and the same entity
         * can be re-upserted with fresh content. The old code let the LAST
         * arrival overwrite — under parallel extraction the merge order
         * varies run to run, so WHICH entity survived flickered (xfs:
         * Function-node count 4998 vs 5015 across two runs) and every
         * order-sensitive consumer downstream inherited it. Pick the
         * survivor by a canonical CONTENT rule instead, a pure function of
         * the two candidates: smallest file_path, then LARGEST start_line,
         * then largest name/label (a mixed-direction composite is still a
         * total order, so the pick is commutative and scheduling-free).
         * Line-descending within a file keeps the classic upsert contract —
         * a later definition in the same file (macro redefinition, refresh
         * of the same entity with new content) replaces the earlier one,
         * deterministically, because intra-file arrival order is fixed. A
         * full tie is the same entity re-upserted → refresh in place.
         * Kind-disambiguated QNs (the real cure) remain a follow-up. */
        int c = strcmp(file_path ? file_path : "", existing->file_path ? existing->file_path : "");
        if (c == 0) {
            c = existing->start_line - start_line;
        }
        if (c == 0) {
            c = strcmp(existing->name ? existing->name : "", name ? name : "");
        }
        if (c == 0) {
            c = strcmp(existing->label ? existing->label : "", label ? label : "");
        }
        if (c > 0) {
            return existing->id; /* existing entity is the canonical winner */
        }
        /* Update in-place. name/properties are strdup'd BEFORE freeing old ones
         * (callers may pass existing->name as an argument). label/file_path are
         * interned: gb_intern returns a stable pool pointer (idempotent even when
         * label == existing->label), so the old value is replaced, never freed.
         * When the surviving label/name changes, keep the secondary indexes
         * consistent (the old code left the node listed under its ORIGINAL
         * label/name — cbm_gbuf_find_by_label/name then missed or mis-listed it,
         * which is how the flickering Function set reached the semantic pass). */
        char *new_name = heap_strdup(name);
        char *new_props = properties_json ? heap_strdup(properties_json) : NULL;
        const char *new_label_interned = gb_intern(gb, label);
        bool label_changed = !existing->label || !new_label_interned ||
                             strcmp(existing->label, new_label_interned) != 0;
        bool name_changed = !existing->name || !new_name || strcmp(existing->name, new_name) != 0;
        if (label_changed) {
            remove_node_from_ptr_array(
                cbm_ht_get(gb->nodes_by_label, existing->label ? existing->label : ""),
                existing->id);
        }
        if (name_changed) {
            remove_node_from_ptr_array(
                cbm_ht_get(gb->nodes_by_name, existing->name ? existing->name : ""), existing->id);
        }
        existing->label = (char *)new_label_interned;
        cbm_free(CBM_MEM_CLASS_GBUF_STRING, existing->name);
        existing->name = new_name;
        existing->file_path = (char *)gb_intern(gb, file_path);
        existing->start_line = start_line;
        existing->end_line = end_line;
        if (new_props) {
            cbm_free(CBM_MEM_CLASS_GBUF_STRING, existing->properties_json);
            existing->properties_json = new_props;
        }
        if (label_changed) {
            node_ptr_array_t *by_label = get_or_create_node_array(
                gb->nodes_by_label, existing->label ? existing->label : "");
            node_array_push(by_label, existing);
        }
        if (name_changed) {
            node_ptr_array_t *by_name =
                get_or_create_node_array(gb->nodes_by_name, existing->name ? existing->name : "");
            node_array_push(by_name, existing);
        }
        return existing->id;
    }

    /* Heap-allocate a new node (pointer stays stable across array growth) */
    cbm_gbuf_node_t *node = cbm_calloc(CBM_MEM_CLASS_GBUF_NODE, sizeof(cbm_gbuf_node_t));
    if (!node) {
        return 0;
    }

    int64_t id = alloc_next_id(gb);
    node->id = id;
    node->label = (char *)gb_intern(gb, label);
    node->name = heap_strdup(name);
    node->qualified_name = heap_strdup(qualified_name);
    node->file_path = (char *)gb_intern(gb, file_path);
    node->start_line = start_line;
    node->end_line = end_line;
    node->properties_json = heap_strdup(properties_json);

    /* Store pointer in array and register in all indexes */
    cbm_da_push(&gb->nodes, node);
    register_node_in_indexes(gb, node);

    return id;
}

int cbm_gbuf_node_set_properties_json(cbm_gbuf_node_t *node, const char *json) {
    if (!node) {
        return GB_ERR;
    }
    char *copy = heap_strdup(json);
    if (!copy) {
        return GB_ERR;
    }
    cbm_free(CBM_MEM_CLASS_GBUF_STRING, node->properties_json);
    node->properties_json = copy;
    return 0;
}

const cbm_gbuf_node_t *cbm_gbuf_find_by_qn(const cbm_gbuf_t *gb, const char *qn) {
    if (!gb || !qn) {
        return NULL;
    }
    return cbm_ht_get(gb->node_by_qn, qn);
}

const cbm_gbuf_node_t *cbm_gbuf_find_by_id(const cbm_gbuf_t *gb, int64_t id) {
    if (!gb || !gb->by_id || id < 0 || id >= gb->by_id_cap) {
        return NULL;
    }
    return gb->by_id[id];
}

int cbm_gbuf_find_by_label(const cbm_gbuf_t *gb, const char *label, const cbm_gbuf_node_t ***out,
                           int *count) {
    if (!gb || !out || !count) {
        return CBM_NOT_FOUND;
    }
    node_ptr_array_t *arr = cbm_ht_get(gb->nodes_by_label, label ? label : "");
    if (arr && arr->count > 0) {
        *out = arr->items;
        *count = arr->count;
    } else {
        *out = NULL;
        *count = 0;
    }
    return 0;
}

int cbm_gbuf_find_by_name(const cbm_gbuf_t *gb, const char *name, const cbm_gbuf_node_t ***out,
                          int *count) {
    if (!gb || !out || !count) {
        return CBM_NOT_FOUND;
    }
    node_ptr_array_t *arr = cbm_ht_get(gb->nodes_by_name, name ? name : "");
    if (arr && arr->count > 0) {
        *out = arr->items;
        *count = arr->count;
    } else {
        *out = NULL;
        *count = 0;
    }
    return 0;
}

int cbm_gbuf_node_count(const cbm_gbuf_t *gb) {
    /* Use QN hash table count since it's authoritative (handles deletes) */
    return gb ? (int)cbm_ht_count(gb->node_by_qn) : 0;
}

int cbm_gbuf_delete_by_label(cbm_gbuf_t *gb, const char *label) {
    if (!gb || !label) {
        return CBM_NOT_FOUND;
    }

    node_ptr_array_t *arr = cbm_ht_get(gb->nodes_by_label, label);
    if (!arr || arr->count == 0) {
        return 0;
    }

    /* Build hash set of deleted node IDs for O(1) lookup */
    CBMHashTable *deleted_set = cbm_ht_create(arr->count);
    for (int i = 0; i < arr->count; i++) {
        const cbm_gbuf_node_t *n = arr->items[i];

        char id_buf[CBM_SZ_32];
        make_id_key(id_buf, sizeof(id_buf), n->id);
        cbm_ht_set(deleted_set, cbm_mem_strdup(CBM_MEM_CLASS_GBUF_INDEX, id_buf),
                   intptr_to_ptr(SKIP_ONE));

        /* Remove from primary indexes */
        cbm_ht_delete(gb->node_by_qn, n->qualified_name);
        if (n->id >= 0 && n->id < gb->by_id_cap) {
            gb->by_id[n->id] = NULL;
        }
    }

    /* Clear the label array */
    cbm_da_clear(arr);

    /* Cascade-delete edges referencing deleted nodes */
    cascade_delete_edges(gb, deleted_set);

    cbm_ht_foreach(deleted_set, free_index_key, NULL);
    cbm_ht_free(deleted_set);
    return 0;
}

int cbm_gbuf_delete_by_file(cbm_gbuf_t *gb, const char *file_path) {
    if (!gb || !file_path) {
        return CBM_NOT_FOUND;
    }

    /* Collect IDs of nodes in this file */
    CBMHashTable *deleted_set = cbm_ht_create(CBM_SZ_64);
    int deleted_count = 0;
    int scanned = 0;

    for (int i = 0; i < gb->nodes.count; i++) {
        cbm_gbuf_node_t *n = gb->nodes.items[i];
        scanned++;
        if (!n->file_path || strcmp(n->file_path, file_path) != 0) {
            continue;
        }
        if (!n->qualified_name || !cbm_ht_get(gb->node_by_qn, n->qualified_name)) {
            continue;
        }

        char id_buf[CBM_SZ_32];
        make_id_key(id_buf, sizeof(id_buf), n->id);
        cbm_ht_set(deleted_set, cbm_mem_strdup(CBM_MEM_CLASS_GBUF_INDEX, id_buf),
                   intptr_to_ptr(SKIP_ONE));

        /* Remove from secondary indexes */
        remove_node_from_ptr_array(cbm_ht_get(gb->nodes_by_label, n->label), n->id);
        remove_node_from_ptr_array(cbm_ht_get(gb->nodes_by_name, n->name), n->id);

        /* Remove from primary indexes */
        cbm_ht_delete(gb->node_by_qn, n->qualified_name);
        if (n->id >= 0 && n->id < gb->by_id_cap) {
            gb->by_id[n->id] = NULL;
        }

        /* NULL out QN so dump's liveness check (cbm_ht_get by QN) fails
         * even if a new node with the same QN is inserted later via merge. */
        cbm_free(CBM_MEM_CLASS_GBUF_STRING, n->qualified_name);
        n->qualified_name = NULL;
        deleted_count++;
    }

    if (deleted_count == 0) {
        cbm_ht_free(deleted_set);
        return 0;
    }

    /* Cascade-delete edges referencing deleted nodes */
    cascade_delete_edges(gb, deleted_set);

    cbm_ht_foreach(deleted_set, free_index_key, NULL);
    cbm_ht_free(deleted_set);
    {
        char s_buf[CBM_SZ_16];
        char d_buf[CBM_SZ_16];
        snprintf(s_buf, sizeof(s_buf), "%d", scanned);
        snprintf(d_buf, sizeof(d_buf), "%d", deleted_count);
        cbm_log_info("gbuf.delete_by_file", "file", file_path, "scanned", s_buf, "deleted", d_buf);
    }
    return deleted_count;
}

int cbm_gbuf_load_from_db(cbm_gbuf_t *gb, const char *db_path, const char *project) {
    if (!gb || !db_path || !project) {
        return CBM_NOT_FOUND;
    }

    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store) {
        return CBM_NOT_FOUND;
    }

    sqlite3 *db = cbm_store_get_db(store);
    if (!db) {
        cbm_store_close(store);
        return CBM_NOT_FOUND;
    }

    /* First pass: find max node ID for mapping array */
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, "SELECT MAX(id) FROM nodes WHERE project = ?", CBM_NOT_FOUND, &stmt,
                           NULL) != SQLITE_OK) {
        cbm_store_close(store);
        return CBM_NOT_FOUND;
    }
    sqlite3_bind_text(stmt, SKIP_ONE, project, CBM_NOT_FOUND, SQLITE_STATIC);
    int64_t max_old_id = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        max_old_id = sqlite3_column_int64(stmt, 0);
    }
    sqlite3_finalize(stmt);

    int64_t *old_to_new =
        cbm_calloc(CBM_MEM_CLASS_GBUF_INDEX, (size_t)(max_old_id + SKIP_ONE) * sizeof(int64_t));
    if (!old_to_new) {
        cbm_store_close(store);
        return CBM_NOT_FOUND;
    }

    /* Load all nodes */
    if (sqlite3_prepare_v2(
            db,
            "SELECT id, label, name, qualified_name, file_path, start_line, end_line, properties "
            "FROM nodes WHERE project = ? ORDER BY id",
            CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        cbm_free(CBM_MEM_CLASS_GBUF_INDEX, old_to_new);
        cbm_store_close(store);
        return CBM_NOT_FOUND;
    }
    sqlite3_bind_text(stmt, SKIP_ONE, project, CBM_NOT_FOUND, SQLITE_STATIC);

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int64_t old_id = sqlite3_column_int64(stmt, 0);
        const char *label = (const char *)sqlite3_column_text(stmt, SKIP_ONE);
        const char *name = (const char *)sqlite3_column_text(stmt, GB_COL_2);
        const char *qn = (const char *)sqlite3_column_text(stmt, GB_COL_3);
        const char *fp = (const char *)sqlite3_column_text(stmt, GB_COL_4);
        int sl = sqlite3_column_int(stmt, GB_COL_5);
        int el = sqlite3_column_int(stmt, GB_COL_6);
        const char *props = (const char *)sqlite3_column_text(stmt, GB_COL_7);

        int64_t new_id = cbm_gbuf_upsert_node(gb, label, name, qn, fp, sl, el, props);
        if (new_id > 0 && old_id <= max_old_id) {
            old_to_new[old_id] = new_id;
        }
    }
    sqlite3_finalize(stmt);

    /* Load all edges, remap IDs */
    if (sqlite3_prepare_v2(db,
                           "SELECT source_id, target_id, type, properties "
                           "FROM edges WHERE project = ?",
                           CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        cbm_free(CBM_MEM_CLASS_GBUF_INDEX, old_to_new);
        cbm_store_close(store);
        return CBM_NOT_FOUND;
    }
    sqlite3_bind_text(stmt, SKIP_ONE, project, CBM_NOT_FOUND, SQLITE_STATIC);

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int64_t old_src = sqlite3_column_int64(stmt, 0);
        int64_t old_tgt = sqlite3_column_int64(stmt, SKIP_ONE);
        const char *type = (const char *)sqlite3_column_text(stmt, GB_COL_2);
        const char *props = (const char *)sqlite3_column_text(stmt, GB_COL_3);

        int64_t new_src = (old_src <= max_old_id) ? old_to_new[old_src] : 0;
        int64_t new_tgt = (old_tgt <= max_old_id) ? old_to_new[old_tgt] : 0;
        if (new_src > 0 && new_tgt > 0) {
            cbm_gbuf_insert_edge(gb, new_src, new_tgt, type, props);
        }
    }
    sqlite3_finalize(stmt);

    cbm_free(CBM_MEM_CLASS_GBUF_INDEX, old_to_new);
    cbm_store_close(store);
    return 0;
}

void cbm_gbuf_foreach_node(const cbm_gbuf_t *gb, cbm_gbuf_node_visitor_fn fn, void *userdata) {
    if (!gb || !fn) {
        return;
    }
    for (int i = 0; i < gb->nodes.count; i++) {
        const cbm_gbuf_node_t *n = gb->nodes.items[i];
        if (n->qualified_name && cbm_ht_get(gb->node_by_qn, n->qualified_name)) {
            fn(n, userdata);
        }
    }
}

void cbm_gbuf_foreach_edge(const cbm_gbuf_t *gb, cbm_gbuf_edge_visitor_fn fn, void *userdata) {
    if (!gb || !fn) {
        return;
    }
    for (int i = 0; i < gb->edges.count; i++) {
        fn(gb->edges.items[i], userdata);
    }
}

/* ── Edge operations ─────────────────────────────────────────────── */

/* Read "confidence":<double> out of an edge property blob. Absent/unparseable
 * reads as -1 so any edge that carries a confidence outranks one that does
 * not. */
static double edge_props_confidence(const char *props_json) {
    if (!props_json) {
        return CBM_EDGE_CONF_ABSENT;
    }
    static const char conf_key[] = "\"confidence\":";
    const char *p = strstr(props_json, conf_key);
    if (!p) {
        return CBM_EDGE_CONF_ABSENT;
    }
    /* strtod answers 0.0 for text it cannot read, and 0.0 is a real
     * confidence that beats CBM_EDGE_CONF_ABSENT. So a blob carrying
     * "confidence":null used to outrank a clean blob that carries no
     * confidence at all, and displace it. Ask strtod where it stopped: an
     * end pointer that never moved means it read nothing. */
    const char *value = p + sizeof(conf_key) - SKIP_ONE;
    char *end = NULL;
    double conf = strtod(value, &end);
    if (end == value) {
        return CBM_EDGE_CONF_ABSENT;
    }
    return conf;
}

/* Decide whether an incoming property blob replaces the stored one on a
 * duplicate edge key.
 *
 * confidence/strategy/via are not part of the edge key, and the same logical
 * edge is legitimately minted by more than one strategy carrying different
 * values — LSP resolution and registry-textual matching both produce CALLS
 * edges. Per-worker edge buffers merge in worker-slot order, so any rule that
 * depends on arrival order makes the surviving attributes a function of thread
 * scheduling.
 *
 * This rule is a total order over the two candidates, hence commutative and
 * associative — the outcome is independent of arrival order:
 *   1. higher "confidence" wins, which also enforces the intended precedence
 *      that an LSP-resolved call outranks a textual match;
 *   2. on equal confidence, the lexicographically greater blob wins, giving a
 *      deterministic choice between otherwise equal candidates.
 * An empty or absent incoming blob never displaces stored properties. */
static bool edge_props_should_replace(const char *existing_json, const char *incoming_json) {
    if (!incoming_json || strcmp(incoming_json, "{}") == 0) {
        return false;
    }
    if (!existing_json || strcmp(existing_json, "{}") == 0) {
        return true;
    }
    double inc = edge_props_confidence(incoming_json);
    double cur = edge_props_confidence(existing_json);
    if (inc != cur) {
        return inc > cur;
    }
    return strcmp(incoming_json, existing_json) > 0;
}

int64_t cbm_gbuf_insert_edge(cbm_gbuf_t *gb, int64_t source_id, int64_t target_id, const char *type,
                             const char *properties_json) {
    if (!gb || !type) {
        return 0;
    }

    /* Check for dedup */
    char key[EDGE_KEY_BUF];
    make_edge_key(key, sizeof(key), source_id, target_id, type, properties_json);

    uint64_t h0;
    uint64_t h1;
    edge_key_hash(key, &h0, &h1);
    edge_key_slot_t *hit = edge_key_map_find(&gb->edge_by_key, h0, h1);
    cbm_gbuf_edge_t *existing = hit ? hit->edge : NULL;
    if (existing) {
        if (edge_props_should_replace(existing->properties_json, properties_json)) {
            existing->properties_json = (char *)gb_intern(gb, properties_json);
        }
        return existing->id;
    }

    /* Heap-allocate a new edge (pointer stays stable) */
    cbm_gbuf_edge_t *edge = cbm_calloc(CBM_MEM_CLASS_GBUF_EDGE, sizeof(cbm_gbuf_edge_t));
    if (!edge) {
        return 0;
    }

    int64_t id = alloc_next_id(gb);
    edge->id = id;
    edge->source_id = source_id;
    edge->target_id = target_id;
    edge->type = (char *)gb_intern(gb, type);
    edge->properties_json = (char *)gb_intern(gb, properties_json);

    /* Store pointer in array */
    cbm_da_push(&gb->edges, edge);

    /* Dedup index */
    (void)edge_key_map_set(&gb->edge_by_key, h0, h1, edge);

    /* Secondary indexes */
    register_edge_in_indexes(gb, edge);

    return id;
}

int cbm_gbuf_find_edges_by_source_type(const cbm_gbuf_t *gb, int64_t source_id, const char *type,
                                       const cbm_gbuf_edge_t ***out, int *count) {
    if (!gb || !out || !count) {
        return CBM_NOT_FOUND;
    }
    char key[EDGE_KEY_BUF];
    make_src_type_key(key, sizeof(key), source_id, type);
    edge_ptr_array_t *arr = cbm_ht_get(gb->edges_by_source_type, key);
    if (arr && arr->count > 0) {
        *out = arr->items;
        *count = arr->count;
    } else {
        *out = NULL;
        *count = 0;
    }
    return 0;
}

int cbm_gbuf_find_edges_by_target_type(const cbm_gbuf_t *gb, int64_t target_id, const char *type,
                                       const cbm_gbuf_edge_t ***out, int *count) {
    if (!gb || !out || !count) {
        return CBM_NOT_FOUND;
    }
    char key[EDGE_KEY_BUF];
    make_src_type_key(key, sizeof(key), target_id, type);
    edge_ptr_array_t *arr = cbm_ht_get(gb->edges_by_target_type, key);
    if (arr && arr->count > 0) {
        *out = arr->items;
        *count = arr->count;
    } else {
        *out = NULL;
        *count = 0;
    }
    return 0;
}

int cbm_gbuf_find_edges_by_type(const cbm_gbuf_t *gb, const char *type,
                                const cbm_gbuf_edge_t ***out, int *count) {
    if (!gb || !out || !count) {
        return CBM_NOT_FOUND;
    }
    edge_ptr_array_t *arr = cbm_ht_get(gb->edges_by_type, type);
    if (arr && arr->count > 0) {
        *out = arr->items;
        *count = arr->count;
    } else {
        *out = NULL;
        *count = 0;
    }
    return 0;
}

int cbm_gbuf_edge_count(const cbm_gbuf_t *gb) {
    return gb ? gb->edges.count : 0;
}

int cbm_gbuf_edge_count_by_type(const cbm_gbuf_t *gb, const char *type) {
    if (!gb || !type) {
        return 0;
    }
    edge_ptr_array_t *arr = cbm_ht_get(gb->edges_by_type, type);
    return arr ? arr->count : 0;
}

int cbm_gbuf_delete_edges_by_type(cbm_gbuf_t *gb, const char *type) {
    if (!gb || !type) {
        return CBM_NOT_FOUND;
    }

    /* Remove edges of the given type from array and dedup index */
    int write_idx = 0;
    for (int i = 0; i < gb->edges.count; i++) {
        cbm_gbuf_edge_t *e = gb->edges.items[i];
        if (strcmp(e->type, type) == 0) {
            char key[EDGE_KEY_BUF];
            make_edge_key(key, sizeof(key), e->source_id, e->target_id, e->type,
                          e->properties_json);
            uint64_t h0;
            uint64_t h1;
            edge_key_hash(key, &h0, &h1);
            edge_key_map_delete(&gb->edge_by_key, h0, h1);
            free_edge_strings(e);
            cbm_free(CBM_MEM_CLASS_GBUF_EDGE, e);
        } else {
            gb->edges.items[write_idx++] = gb->edges.items[i];
        }
    }
    gb->edges.count = write_idx;

    /* Rebuild edge secondary indexes */
    rebuild_edge_secondary_indexes(gb);

    return 0;
}

/* ── Merge ───────────────────────────────────────────────────────── */

/* Free remap hash table entries (key = heap string, value = heap int64_t*) */
static void free_remap_entry(const char *key, void *val, void *ud) {
    (void)ud;
    cbm_free(CBM_MEM_CLASS_GBUF_INDEX, (void *)key);
    cbm_free(CBM_MEM_CLASS_GBUF_INDEX, val);
}

/* Handle QN collision: update dst node fields (src wins), record remap if IDs differ.
 * label/file_path are re-interned into dst's pool (sn's pointers belong to src). */
static void merge_update_existing(cbm_gbuf_t *dst, cbm_gbuf_node_t *existing,
                                  const cbm_gbuf_node_t *sn, CBMHashTable **remap) {
    /* Same guard as cbm_gbuf_upsert_node: a per-file "Module" def coming from a
     * worker-local gbuf must not touch the structural directory node ("Project"
     * root or "Folder") that shares its QN in a directory-based-module language
     * (Java/Go). pass_structure seeds Folder/Project nodes on the MAIN gbuf
     * before the parallel extract, so every worker's always-emitted Module def
     * for that package collides here; unconditional "src wins" relabelled the
     * directory node to Module and set its file_path to whichever worker merged
     * LAST — the nondeterministic USAGE-source misattribution of #787. Keep the
     * structural node intact; the ID remap below still redirects the worker's
     * edges onto the canonical node. */
    bool module_on_container =
        existing->label && sn->label && strcmp(sn->label, "Module") == 0 &&
        (strcmp(existing->label, "Project") == 0 || strcmp(existing->label, "Folder") == 0);
    if (!module_on_container) {
        /* Canonical collision winner (determinism) — mirrors
         * cbm_gbuf_upsert_node exactly. Distinct source entities can share a
         * QN (C: struct/function/macro with one name); unconditional "src
         * wins" made the survivor depend on worker merge order, flickering
         * the node set (and every downstream consumer) run to run. Winner =
         * smallest file_path, then LARGEST start_line, then largest
         * name/label — one total order, commutative, scheduling-free; a full
         * tie is the same entity → refresh from src. */
        int c = strcmp(sn->file_path ? sn->file_path : "",
                       existing->file_path ? existing->file_path : "");
        if (c == 0) {
            c = existing->start_line - sn->start_line;
        }
        if (c == 0) {
            c = strcmp(existing->name ? existing->name : "", sn->name ? sn->name : "");
        }
        if (c == 0) {
            c = strcmp(existing->label ? existing->label : "", sn->label ? sn->label : "");
        }
        bool sn_wins = c <= 0;
        if (sn_wins) {
            /* Keep the secondary indexes consistent when the surviving
             * label/name changes (the old code left the node listed under its
             * original label/name, so find_by_label/name mis-listed it). */
            const char *new_label = gb_intern(dst, sn->label);
            bool label_changed =
                !existing->label || !new_label || strcmp(existing->label, new_label) != 0;
            bool name_changed =
                !existing->name || !sn->name || strcmp(existing->name, sn->name) != 0;
            if (label_changed) {
                remove_node_from_ptr_array(
                    cbm_ht_get(dst->nodes_by_label, existing->label ? existing->label : ""),
                    existing->id);
            }
            if (name_changed) {
                remove_node_from_ptr_array(
                    cbm_ht_get(dst->nodes_by_name, existing->name ? existing->name : ""),
                    existing->id);
            }
            existing->label = (char *)new_label;
            cbm_free(CBM_MEM_CLASS_GBUF_STRING, existing->name);
            existing->name = heap_strdup(sn->name);
            existing->file_path = (char *)gb_intern(dst, sn->file_path);
            existing->start_line = sn->start_line;
            existing->end_line = sn->end_line;
            if (sn->properties_json) {
                cbm_free(CBM_MEM_CLASS_GBUF_STRING, existing->properties_json);
                existing->properties_json = heap_strdup(sn->properties_json);
            }
            if (label_changed) {
                node_ptr_array_t *by_label = get_or_create_node_array(
                    dst->nodes_by_label, existing->label ? existing->label : "");
                node_array_push(by_label, existing);
            }
            if (name_changed) {
                node_ptr_array_t *by_name = get_or_create_node_array(
                    dst->nodes_by_name, existing->name ? existing->name : "");
                node_array_push(by_name, existing);
            }
        }
    }

    if (sn->id != existing->id) {
        if (!*remap) {
            *remap = cbm_ht_create(CBM_SZ_32);
        }
        char key[CBM_SZ_32];
        make_id_key(key, sizeof(key), sn->id);
        int64_t *val = cbm_alloc(CBM_MEM_CLASS_GBUF_INDEX, sizeof(int64_t));
        *val = existing->id;
        cbm_ht_set(*remap, cbm_mem_strdup(CBM_MEM_CLASS_GBUF_INDEX, key), val);
    }
}

/* Copy a non-colliding src node into dst with its original ID. */
static void merge_copy_new_node(cbm_gbuf_t *dst, const cbm_gbuf_node_t *sn) {
    cbm_gbuf_node_t *node = cbm_calloc(CBM_MEM_CLASS_GBUF_NODE, sizeof(cbm_gbuf_node_t));
    if (!node) {
        return;
    }

    node->id = sn->id;
    node->label = (char *)gb_intern(dst, sn->label);
    node->name = heap_strdup(sn->name);
    node->qualified_name = heap_strdup(sn->qualified_name);
    node->file_path = (char *)gb_intern(dst, sn->file_path);
    node->start_line = sn->start_line;
    node->end_line = sn->end_line;
    node->properties_json = heap_strdup(sn->properties_json);

    cbm_da_push(&dst->nodes, node);
    register_node_in_indexes(dst, node);

    if (node->id >= dst->next_id) {
        dst->next_id = node->id + SKIP_ONE;
    }
}

/* Remap edge IDs using the collision remap table and insert into dst. */
static void merge_remap_edges(cbm_gbuf_t *dst, cbm_gbuf_t *src, CBMHashTable *remap) {
    for (int i = 0; i < src->edges.count; i++) {
        cbm_gbuf_edge_t *se = src->edges.items[i];

        int64_t new_src = se->source_id;
        int64_t new_tgt = se->target_id;

        if (remap) {
            char key[CBM_SZ_32];
            make_id_key(key, sizeof(key), se->source_id);
            int64_t *remapped = cbm_ht_get(remap, key);
            if (remapped) {
                new_src = *remapped;
            }

            make_id_key(key, sizeof(key), se->target_id);
            remapped = cbm_ht_get(remap, key);
            if (remapped) {
                new_tgt = *remapped;
            }
        }

        cbm_gbuf_insert_edge(dst, new_src, new_tgt, se->type, se->properties_json);
    }
}

int cbm_gbuf_merge(cbm_gbuf_t *dst, cbm_gbuf_t *src) {
    if (!dst || !src) {
        return CBM_NOT_FOUND;
    }
    if (src->nodes.count == 0 && src->edges.count == 0) {
        return 0;
    }

    /* ID remap for QN-colliding nodes: "src_id" → (int64_t*) dst_id.
     * Only populated when a src node's QN already exists in dst. */
    CBMHashTable *remap = NULL;

    for (int i = 0; i < src->nodes.count; i++) {
        cbm_gbuf_node_t *sn = src->nodes.items[i];
        if (!sn->qualified_name) {
            continue;
        }

        /* Skip nodes deleted from QN index */
        if (!cbm_ht_get(src->node_by_qn, sn->qualified_name)) {
            continue;
        }

        cbm_gbuf_node_t *existing = cbm_ht_get(dst->node_by_qn, sn->qualified_name);
        if (existing) {
            merge_update_existing(dst, existing, sn, &remap);
        } else {
            merge_copy_new_node(dst, sn);
        }
    }

    /* Merge edges with optional ID remapping */
    merge_remap_edges(dst, src, remap);

    if (remap) {
        cbm_ht_foreach(remap, free_remap_entry, NULL);
        cbm_ht_free(remap);
    }

    return 0;
}

/* ── Dump / Flush ────────────────────────────────────────────────── */

/* Extract a string property from a properties JSON string.
 * Returns heap-allocated string or NULL. Caller must free.
 * Parses real JSON: the dump writer feeds these values into indexes whose
 * backing columns are GENERATED AS json_extract(properties,'$.<key>').
 * Naive byte slicing returned the ESCAPED text (and cut at embedded \\")
 * while json_extract yields the unescaped value — the mismatch left rows
 * "missing from index idx_edges_url_path" under PRAGMA integrity_check.
 * key_quoted ("\"key\"") is a fast pre-filter to skip the JSON parse. */
static char *extract_prop_string(const char *props, const char *key_quoted, const char *key) {
    if (!props || !strstr(props, key_quoted)) {
        return NULL;
    }
    yyjson_doc *doc = yyjson_read(props, strlen(props), 0);
    if (!doc) {
        return NULL;
    }
    char *out = NULL;
    yyjson_val *v = yyjson_obj_get(yyjson_doc_get_root(doc), key);
    if (v && yyjson_is_str(v)) {
        const char *sv = yyjson_get_str(v);
        out = cbm_mem_strdup(CBM_MEM_CLASS_DUMP, sv);
    }
    yyjson_doc_free(doc);
    return out;
}

static char *extract_url_path(const char *props) {
    return extract_prop_string(props, "\"url_path\"", "url_path");
}

/* local_name feeds the hand-built sqlite_autoindex_edges_1 — its backing
 * column local_name_gen is GENERATED only for IMPORTS edges (#768). */
static char *extract_local_name(const char *props) {
    return extract_prop_string(props, "\"local_name\"", "local_name");
}

/* Remap a temp edge ID to its final sequential ID, or 0 if out of range. */
static int64_t remap_id(const int64_t *temp_to_final, int64_t max_temp_id, int64_t temp_id) {
    return (temp_id < max_temp_id) ? temp_to_final[temp_id] : 0;
}

/* Build dump-ready node array with sequential IDs. Populates temp_to_final mapping. */
static int cmp_dump_vectors_by_id(const void *a, const void *b) {
    int64_t da = ((const CBMDumpVector *)a)->node_id;
    int64_t db = ((const CBMDumpVector *)b)->node_id;
    return (da > db) - (da < db);
}

static CBMDumpNode *build_dump_nodes(cbm_gbuf_t *gb, int live_count, int64_t *temp_to_final,
                                     int64_t max_temp_id, int *out_count,
                                     cbm_gbuf_node_t ***src_out) {
    size_t cap = (size_t)(live_count > 0 ? live_count : SKIP_ONE);
    CBMDumpNode *dump_nodes = cbm_alloc(CBM_MEM_CLASS_DUMP, cap * sizeof(CBMDumpNode));
    /* Parallel gbuf-node pointers so a streamed partition can free its heavy
     * properties_json after the rows are persisted. NULL on OOM disables the
     * per-partition free (the dump still succeeds). */
    cbm_gbuf_node_t **src = cbm_alloc(CBM_MEM_CLASS_DUMP, cap * sizeof(cbm_gbuf_node_t *));
    int idx = 0;

    for (int i = 0; i < gb->nodes.count; i++) {
        cbm_gbuf_node_t *n = gb->nodes.items[i];
        if (!n->qualified_name || !cbm_ht_get(gb->node_by_qn, n->qualified_name)) {
            continue;
        }

        int64_t final_id = idx + SKIP_ONE; /* 1-based sequential */
        if (n->id < max_temp_id) {
            temp_to_final[n->id] = final_id;
        }

        const char *fp = n->file_path ? n->file_path : "";
        const char *props = n->properties_json ? n->properties_json : "{}";
        dump_nodes[idx] = (CBMDumpNode){
            .id = final_id,
            .project = gb->project,
            .label = n->label,
            .name = n->name,
            .qualified_name = n->qualified_name,
            .file_path = fp,
            .start_line = n->start_line,
            .end_line = n->end_line,
            .properties = props,
        };
        if (src) {
            src[idx] = n;
        }
        idx++;
    }

    *out_count = idx;
    *src_out = src;
    return dump_nodes;
}

/* Build dump-ready edge array with remapped IDs. Returns url_paths and
 * local_names (heap string arrays owned by the caller) via out params. */
static CBMDumpEdge *build_dump_edges(cbm_gbuf_t *gb, const int64_t *temp_to_final,
                                     int64_t max_temp_id, int *out_count, char ***out_url_paths,
                                     char ***out_local_names) {
    /* Count valid edges (both endpoints resolved) */
    int valid_edges = 0;
    for (int i = 0; i < gb->edges.count; i++) {
        cbm_gbuf_edge_t *e = gb->edges.items[i];
        if (remap_id(temp_to_final, max_temp_id, e->source_id) > 0 &&
            remap_id(temp_to_final, max_temp_id, e->target_id) > 0) {
            valid_edges++;
        }
    }

    size_t edge_cap = (size_t)(valid_edges > 0 ? valid_edges : SKIP_ONE);
    CBMDumpEdge *dump_edges = cbm_alloc(CBM_MEM_CLASS_DUMP, edge_cap * sizeof(CBMDumpEdge));
    char **url_paths = cbm_calloc(CBM_MEM_CLASS_DUMP, edge_cap * sizeof(char *));
    char **local_names = cbm_calloc(CBM_MEM_CLASS_DUMP, edge_cap * sizeof(char *));
    int idx = 0;

    for (int i = 0; i < gb->edges.count; i++) {
        cbm_gbuf_edge_t *e = gb->edges.items[i];
        int64_t src = remap_id(temp_to_final, max_temp_id, e->source_id);
        int64_t tgt = remap_id(temp_to_final, max_temp_id, e->target_id);
        if (src == 0 || tgt == 0) {
            continue;
        }

        char *url_path = extract_url_path(e->properties_json);
        url_paths[idx] = url_path;

        /* IMPORTS only — mirrors the local_name_gen CASE in the edges DDL. */
        char *local_name = (e->type && strcmp(e->type, "IMPORTS") == 0)
                               ? extract_local_name(e->properties_json)
                               : NULL;
        local_names[idx] = local_name;

        const char *props = e->properties_json ? e->properties_json : "{}";
        dump_edges[idx] = (CBMDumpEdge){
            .id = idx + SKIP_ONE,
            .project = gb->project,
            .source_id = src,
            .target_id = tgt,
            .type = e->type,
            .properties = props,
            .url_path = url_path ? url_path : "",
            .local_name = local_name ? local_name : "",
        };
        idx++;
    }

    *out_count = idx;
    *out_url_paths = url_paths;
    *out_local_names = local_names;
    return dump_edges;
}

/* Remap vector node IDs through temp_to_final, sort by ID, deduplicate. */
static void remap_sort_dedup_vectors(cbm_gbuf_t *gb, const int64_t *temp_to_final,
                                     int64_t max_temp_id) {
    int remapped = 0;
    int dropped = 0;
    for (int i = 0; i < gb->dump_vector_count; i++) {
        int64_t old_id = gb->dump_vectors[i].node_id;
        int64_t new_id = (old_id > 0 && old_id < max_temp_id) ? temp_to_final[old_id] : 0;
        if (new_id > 0) {
            gb->dump_vectors[remapped] = gb->dump_vectors[i];
            gb->dump_vectors[remapped].node_id = new_id;
            remapped++;
        } else {
            dropped++;
        }
    }
    if (dropped > 0) {
        char r_buf[CBM_SZ_16];
        char d_buf[CBM_SZ_16];
        snprintf(r_buf, sizeof(r_buf), "%d", remapped);
        snprintf(d_buf, sizeof(d_buf), "%d", dropped);
        cbm_log_info("dump.vectors.remap", "remapped", r_buf, "dropped", d_buf);
    }
    gb->dump_vector_count = remapped;

    if (gb->dump_vector_count >= GB_MIN_FOR_DEDUP) {
        qsort(gb->dump_vectors, (size_t)gb->dump_vector_count, sizeof(CBMDumpVector),
              cmp_dump_vectors_by_id);
        int deduped = 0;
        for (int i = 0; i < gb->dump_vector_count; i++) {
            if (i + GB_DEDUP_LOOKAHEAD < gb->dump_vector_count &&
                gb->dump_vectors[i].node_id == gb->dump_vectors[i + GB_DEDUP_LOOKAHEAD].node_id) {
                continue;
            }
            gb->dump_vectors[deduped++] = gb->dump_vectors[i];
        }
        gb->dump_vector_count = deduped;
    }
}

static void log_dump_summary(int node_count, int edge_count) {
    char b1[CBM_SZ_16];
    char b2[CBM_SZ_16];
    snprintf(b1, sizeof(b1), "%d", node_count);
    snprintf(b2, sizeof(b2), "%d", edge_count);
    cbm_log_info("gbuf.dump", "nodes", b1, "edges", b2);
}

static void free_dump_resources(char **url_paths, char **local_names, int edge_count,
                                CBMDumpEdge *dump_edges, CBMDumpNode *dump_nodes,
                                int64_t *temp_to_final) {
    for (int i = 0; i < edge_count; i++) {
        cbm_free(CBM_MEM_CLASS_DUMP, url_paths[i]);
        cbm_free(CBM_MEM_CLASS_DUMP, local_names[i]);
    }
    cbm_free(CBM_MEM_CLASS_DUMP, url_paths);
    cbm_free(CBM_MEM_CLASS_DUMP, local_names);
    cbm_free(CBM_MEM_CLASS_DUMP, dump_edges);
    cbm_free(CBM_MEM_CLASS_DUMP, dump_nodes);
    cbm_free(CBM_MEM_CLASS_DUMP, temp_to_final);
}

static int count_live_nodes(cbm_gbuf_t *gb) {
    int count = 0;
    for (int i = 0; i < gb->nodes.count; i++) {
        cbm_gbuf_node_t *n = gb->nodes.items[i];
        if (n->qualified_name && cbm_ht_get(gb->node_by_qn, n->qualified_name)) {
            count++;
        }
    }
    return count;
}

static void generate_iso_timestamp(char *buf, size_t buf_size) {
    time_t now = time(NULL);
    struct tm tm_buf;
    struct tm *tm_val = cbm_gmtime_r(&now, &tm_buf);
    if (strftime(buf, buf_size, "%Y-%m-%dT%H:%M:%SZ", tm_val) == 0) {
        snprintf(buf, buf_size, "1970-01-01T00:00:00Z");
    }
}

/* Release lookup indexes then remap+sort+dedup vectors for the B-tree writer. */
static void release_and_remap_vectors(cbm_gbuf_t *gb, const int64_t *temp_to_final,
                                      int64_t max_temp_id) {
    CBM_PROF_START(t_vec_remap);
    remap_sort_dedup_vectors(gb, temp_to_final, max_temp_id);
    CBM_PROF_END_N("dump", "5_vector_remap_sort", t_vec_remap, gb->dump_vector_count);
}

int cbm_gbuf_dump_to_sqlite(cbm_gbuf_t *gb, const char *path) {
    if (!gb || !path) {
        return CBM_NOT_FOUND;
    }

    CBM_PROF_START(t_count);
    int live_count = count_live_nodes(gb);
    CBM_PROF_END_N("dump", "1_count_live_nodes", t_count, live_count);

    CBM_PROF_START(t_build_nodes);
    int64_t max_temp_id = gb->next_id;
    int64_t *temp_to_final = cbm_calloc(CBM_MEM_CLASS_DUMP, (size_t)max_temp_id * sizeof(int64_t));
    if (!temp_to_final) {
        return CBM_NOT_FOUND;
    }

    int node_idx = 0;
    cbm_gbuf_node_t **src_nodes = NULL;
    CBMDumpNode *dump_nodes =
        build_dump_nodes(gb, live_count, temp_to_final, max_temp_id, &node_idx, &src_nodes);
    CBM_PROF_END_N("dump", "2_build_dump_nodes", t_build_nodes, node_idx);

    /* Release ALL lookup indexes NOW: nothing between here and finalize reads
     * them (stream-append uses dump_nodes/src_nodes, build_dump_edges uses
     * temp_to_final + gb->edges, the vector remap uses temp_to_final). At
     * kernel scale they are ~3.8 GB (hash buckets + key strings + pointer
     * arrays); releasing them only AFTER edge building made them coexist with
     * every dump-side transient array — the dump-phase RSS peak. */
    CBM_PROF_START(t_release_idx);
    release_gbuf_indexes(gb);
    CBM_PROF_END("dump", "4_release_gbuf_indexes", t_release_idx);

    char indexed_at[CBM_SZ_64];
    generate_iso_timestamp(indexed_at, sizeof(indexed_at));

    /* Stream node rows to the DB in partitions. Under memory pressure, free each
     * partition's heavy properties_json once persisted — the heavy column is
     * write-once and never read again, so this bounds the dump/finalize peak.
     * The DB output is identical whether or not freeing engages, so non-pressure
     * runs (and tests) leave the gbuf intact (the budget>0 guard keeps an
     * uninitialized budget from ever triggering the free). */
    cbm_db_writer_t *w = cbm_writer_open(path);
    if (!w) {
        cbm_free(CBM_MEM_CLASS_DUMP, src_nodes);
        cbm_free(CBM_MEM_CLASS_DUMP, dump_nodes);
        cbm_free(CBM_MEM_CLASS_DUMP, temp_to_final);
        return CBM_NOT_FOUND;
    }

    CBM_PROF_START(t_append);
    enum { DUMP_PARTITION_NODES = 1 << 16 };
    bool free_heavy = false;
    int rc = 0;
    for (int off = 0; off < node_idx; off += DUMP_PARTITION_NODES) {
        int chunk = node_idx - off;
        if (chunk > DUMP_PARTITION_NODES) {
            chunk = DUMP_PARTITION_NODES;
        }
        rc = cbm_writer_append_nodes(w, &dump_nodes[off], chunk);
        if (rc != 0) {
            break;
        }
        free_heavy = free_heavy || (cbm_mem_budget() > 0 && cbm_mem_over_budget());
        if (free_heavy && src_nodes) {
            for (int j = off; j < off + chunk; j++) {
                cbm_free(CBM_MEM_CLASS_GBUF_STRING, src_nodes[j]->properties_json);
                src_nodes[j]->properties_json = NULL;
                dump_nodes[j].properties = NULL;
            }
            cbm_mem_collect();
        }
    }
    CBM_PROF_END_N("dump", "2b_stream_append_nodes", t_append, node_idx);

    int edge_idx = 0;
    char **url_paths = NULL;
    char **local_names = NULL;
    CBMDumpEdge *dump_edges = NULL;
    if (rc == 0) {
        CBM_PROF_START(t_build_edges);
        dump_edges =
            build_dump_edges(gb, temp_to_final, max_temp_id, &edge_idx, &url_paths, &local_names);
        CBM_PROF_END_N("dump", "3_build_dump_edges", t_build_edges, edge_idx);
        release_and_remap_vectors(gb, temp_to_final, max_temp_id);
    }

    /* Finalize: nodes-table interior + edges/vectors/metadata/indexes/sqlite_master.
     * Frees w and closes the file; handles a prior append error cleanly. */
    CBM_PROF_START(t_finalize);
    int frc = cbm_writer_finalize(w, gb->project, gb->root_path, indexed_at, dump_nodes, node_idx,
                                  dump_edges, edge_idx, gb->dump_vectors, gb->dump_vector_count,
                                  gb->dump_token_vecs, gb->dump_token_vec_count);
    CBM_PROF_END_N("dump", "6_write_db_finalize", t_finalize, node_idx + edge_idx);
    if (rc == 0) {
        rc = frc;
    }

    log_dump_summary(node_idx, edge_idx);
    free_dump_resources(url_paths, local_names, edge_idx, dump_edges, dump_nodes, temp_to_final);
    cbm_free(CBM_MEM_CLASS_DUMP, src_nodes);
    return rc;
}

int cbm_gbuf_flush_to_store(cbm_gbuf_t *gb, cbm_store_t *store) {
    if (!gb || !store) {
        return CBM_NOT_FOUND;
    }

    /* Upsert project */
    cbm_store_upsert_project(store, gb->project, gb->root_path);

    /* Begin bulk mode */
    cbm_store_begin_bulk(store);
    cbm_store_drop_indexes(store);
    cbm_store_begin(store);

    /* Delete existing project data */
    cbm_store_delete_edges_by_project(store, gb->project);
    cbm_store_delete_nodes_by_project(store, gb->project);

    /* Build temp_id → real_id map.
     * Temp IDs start at 1 and are sequential, but can have gaps from edge inserts.
     * Use max_id as size. */
    int64_t max_temp_id = gb->next_id;
    int64_t *temp_to_real = cbm_calloc(CBM_MEM_CLASS_DUMP, (size_t)max_temp_id * sizeof(int64_t));

    for (int i = 0; i < gb->nodes.count; i++) {
        cbm_gbuf_node_t *n = gb->nodes.items[i];

        /* Skip if deleted from QN index */
        if (!n->qualified_name || !cbm_ht_get(gb->node_by_qn, n->qualified_name)) {
            continue;
        }

        cbm_node_t sn = {
            .project = gb->project,
            .label = n->label,
            .name = n->name,
            .qualified_name = n->qualified_name,
            .file_path = n->file_path,
            .start_line = n->start_line,
            .end_line = n->end_line,
            .properties_json = n->properties_json,
        };
        int64_t real_id = cbm_store_upsert_node(store, &sn);
        if (real_id > 0 && n->id < max_temp_id) {
            temp_to_real[n->id] = real_id;
        }
    }

    /* Insert all edges with remapped IDs */
    for (int i = 0; i < gb->edges.count; i++) {
        cbm_gbuf_edge_t *e = gb->edges.items[i];
        int64_t real_src = (e->source_id < max_temp_id) ? temp_to_real[e->source_id] : 0;
        int64_t real_tgt = (e->target_id < max_temp_id) ? temp_to_real[e->target_id] : 0;
        if (real_src == 0 || real_tgt == 0) {
            continue;
        }

        cbm_edge_t se = {
            .project = gb->project,
            .source_id = real_src,
            .target_id = real_tgt,
            .type = e->type,
            .properties_json = e->properties_json,
        };
        cbm_store_insert_edge(store, &se);
    }

    cbm_store_commit(store);
    cbm_store_create_indexes(store);
    cbm_store_end_bulk(store);

    cbm_free(CBM_MEM_CLASS_DUMP, temp_to_real);
    return 0;
}

int cbm_gbuf_merge_into_store(cbm_gbuf_t *gb, cbm_store_t *store) {
    if (!gb || !store) {
        return CBM_NOT_FOUND;
    }

    /* Begin bulk mode — no project wipe */
    cbm_store_begin(store);

    /* Build temp_id → real_id map */
    int64_t max_temp_id = gb->next_id;
    int64_t *temp_to_real = cbm_calloc(CBM_MEM_CLASS_DUMP, (size_t)max_temp_id * sizeof(int64_t));

    for (int i = 0; i < gb->nodes.count; i++) {
        cbm_gbuf_node_t *n = gb->nodes.items[i];

        if (!n->qualified_name || !cbm_ht_get(gb->node_by_qn, n->qualified_name)) {
            continue;
        }

        cbm_node_t sn = {
            .project = gb->project,
            .label = n->label,
            .name = n->name,
            .qualified_name = n->qualified_name,
            .file_path = n->file_path,
            .start_line = n->start_line,
            .end_line = n->end_line,
            .properties_json = n->properties_json,
        };
        int64_t real_id = cbm_store_upsert_node(store, &sn);
        if (real_id > 0 && n->id < max_temp_id) {
            temp_to_real[n->id] = real_id;
        }
    }

    for (int i = 0; i < gb->edges.count; i++) {
        cbm_gbuf_edge_t *e = gb->edges.items[i];
        int64_t real_src = (e->source_id < max_temp_id) ? temp_to_real[e->source_id] : 0;
        int64_t real_tgt = (e->target_id < max_temp_id) ? temp_to_real[e->target_id] : 0;
        if (real_src == 0 || real_tgt == 0) {
            continue;
        }

        cbm_edge_t se = {
            .project = gb->project,
            .source_id = real_src,
            .target_id = real_tgt,
            .type = e->type,
            .properties_json = e->properties_json,
        };
        cbm_store_insert_edge(store, &se);
    }

    cbm_store_commit(store);

    cbm_free(CBM_MEM_CLASS_DUMP, temp_to_real);
    return 0;
}
