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
    GB_INVALID_ID = 0,
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
#include "graph_buffer/graph_buffer.h"
#include <yyjson/yyjson.h> // url_path extraction must match json_extract semantics
#include "store/store.h"
#include "sqlite_writer.h"
#include "foundation/hash_table.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/log.h"
#include "foundation/dyn_array.h"
#include "foundation/profile.h"
#include "foundation/mem.h"
#include "foundation/platform.h"
#include <sqlite3.h>

#include <stdatomic.h>
#include <stdarg.h>
#include <stdint.h> // int64_t
#include <stdio.h>
#include <stdlib.h>
#include <string.h> // strdup
#include <time.h>

/* Test-only fault injection for the atomic publish boundary. It lets the suite
 * prove a failed replace leaves the previously published DB untouched. */
static const char cbm_test_env_disabled[] = "0";

static bool cbm_gbuf_test_fail_before_replace_enabled(void) {
    char buf[CBM_SZ_16];
    const char *val =
        cbm_safe_getenv(CBM_TEST_FAIL_GBUF_DUMP_BEFORE_REPLACE, buf, sizeof(buf), NULL);
    return val && val[0] != '\0' && strcmp(val, cbm_test_env_disabled) != 0;
}

static inline void *intptr_to_ptr(intptr_t v) {
    void *p;
    memcpy(&p, &v, sizeof(p));
    return p;
}

/* ── Internal types ──────────────────────────────────────────────── */

/* Edge key for dedup hash table — composite key as string "srcID:tgtID:type" */
#define EDGE_KEY_BUF CBM_SZ_128

/* Per-type or per-key edge list stored in hash tables as values */
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
    CBMHashTable *node_by_id;

    /* Secondary node indexes */
    CBMHashTable *nodes_by_label; /* key: label, value: (node_ptr_array_t*) */
    CBMHashTable *nodes_by_name;  /* key: name, value: (node_ptr_array_t*) */

    /* Edge storage: array of pointers to individually heap-allocated edges */
    CBM_DYN_ARRAY(cbm_gbuf_edge_t *) edges;

    /* Edge dedup index: "srcID:tgtID:type" → cbm_gbuf_edge_t* */
    CBMHashTable *edge_by_key;

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
    return s ? strdup(s) : strdup("{}");
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
    char *copy = strdup(key);
    if (copy) {
        cbm_ht_set(gb->intern_pool, copy, copy); /* key == value == owned copy */
    }
    return copy;
}

static void make_id_key(char *buf, size_t bufsz, int64_t id) {
    snprintf(buf, bufsz, "%lld", (long long)id);
}

static void make_edge_key(char *buf, size_t bufsz, int64_t src, int64_t tgt, const char *type) {
    snprintf(buf, bufsz, "%lld:%lld:%s", (long long)src, (long long)tgt, type);
}

static void make_src_type_key(char *buf, size_t bufsz, int64_t src, const char *type) {
    snprintf(buf, bufsz, "%lld:%s", (long long)src, type);
}

/* Get or create a node_ptr_array_t in a hash table */
static node_ptr_array_t *get_or_create_node_array(CBMHashTable *ht, const char *key) {
    node_ptr_array_t *arr = cbm_ht_get(ht, key);
    if (!arr) {
        arr = calloc(CBM_ALLOC_ONE, sizeof(node_ptr_array_t));
        cbm_ht_set(ht, strdup(key), arr);
    }
    return arr;
}

/* Get or create an edge_ptr_array_t in a hash table */
static edge_ptr_array_t *get_or_create_edge_array(CBMHashTable *ht, const char *key) {
    edge_ptr_array_t *arr = cbm_ht_get(ht, key);
    if (!arr) {
        arr = calloc(CBM_ALLOC_ONE, sizeof(edge_ptr_array_t));
        cbm_ht_set(ht, strdup(key), arr);
    }
    return arr;
}

/* Free a node_ptr_array_t (callback for hash table iteration) */
static void free_node_array(const char *key, void *value, void *ud) {
    (void)ud;
    node_ptr_array_t *arr = value;
    if (arr) {
        cbm_da_free(arr);
        free(arr);
    }
    free((void *)key);
}

/* Free an edge_ptr_array_t (callback) */
static void free_edge_array(const char *key, void *value, void *ud) {
    (void)ud;
    edge_ptr_array_t *arr = value;
    if (arr) {
        cbm_da_free(arr);
        free(arr);
    }
    free((void *)key);
}

/* Free keys only (for node_by_id, edge_by_key) */
static void free_key_only(const char *key, void *value, void *ud) {
    (void)value;
    (void)ud;
    free((void *)key);
}

/* Free a single node's owned strings. label and file_path are interned
 * (pool-owned) — NOT freed here; the pool frees them once in cbm_gbuf_free. */
static void free_node_strings(cbm_gbuf_node_t *n) {
    free(n->name);
    free(n->qualified_name);
    free(n->properties_json);
}

/* Free a single edge's owned strings. type is interned (pool-owned) — NOT
 * freed here; the pool frees it once in cbm_gbuf_free. */
static void free_edge_strings(cbm_gbuf_edge_t *e) {
    free(e->properties_json);
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

    make_edge_key(key, sizeof(key), e->source_id, e->target_id, e->type);
    const char *ekey = cbm_ht_get_key(gb->edge_by_key, key);
    cbm_ht_delete(gb->edge_by_key, key);
    free((void *)ekey);

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
            free(e);
        } else {
            gb->edges.items[write_idx++] = gb->edges.items[i];
        }
    }
    gb->edges.count = write_idx;
}

static int gbuf_ht_set_flag_checked(CBMHashTable *set, const char *key) {
    if (!set || !key) {
        return CBM_NOT_FOUND;
    }
    cbm_ht_set(set, key, intptr_to_ptr(SKIP_ONE));
    return cbm_ht_has(set, key) ? 0 : CBM_NOT_FOUND;
}

static int gbuf_deleted_set_add_node_id(cbm_gbuf_t *gb, CBMHashTable *deleted_set,
                                        const cbm_gbuf_node_t *node) {
    if (!gb || !deleted_set || !node) {
        return CBM_NOT_FOUND;
    }

    char id_buf[CBM_SZ_32];
    make_id_key(id_buf, sizeof(id_buf), node->id);
    const char *stored_key = cbm_ht_get_key(gb->node_by_id, id_buf);
    if (!stored_key) {
        return CBM_NOT_FOUND;
    }
    return gbuf_ht_set_flag_checked(deleted_set, stored_key);
}

/* Register a node in primary (QN, ID) and secondary (label, name) indexes. */
static void register_node_in_indexes(cbm_gbuf_t *gb, cbm_gbuf_node_t *node) {
    cbm_ht_set(gb->node_by_qn, node->qualified_name, node);

    char id_buf[CBM_SZ_32];
    make_id_key(id_buf, sizeof(id_buf), node->id);
    const char *old_key = cbm_ht_get_key(gb->node_by_id, id_buf);
    if (old_key) {
        cbm_ht_set(gb->node_by_id, old_key, node);
    } else {
        cbm_ht_set(gb->node_by_id, strdup(id_buf), node);
    }

    node_ptr_array_t *by_label =
        get_or_create_node_array(gb->nodes_by_label, node->label ? node->label : "");
    cbm_da_push(by_label, (const cbm_gbuf_node_t *)node);

    node_ptr_array_t *by_name =
        get_or_create_node_array(gb->nodes_by_name, node->name ? node->name : "");
    cbm_da_push(by_name, (const cbm_gbuf_node_t *)node);
}

/* Push an edge pointer into a dynamic array (wraps macro to reduce CC contribution). */
static void edge_array_push(edge_ptr_array_t *arr, const cbm_gbuf_edge_t *edge) {
    cbm_da_push(arr, edge);
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
    cbm_ht_foreach(gb->edges_by_source_type, free_edge_array, NULL);
    cbm_ht_free(gb->edges_by_source_type);
    cbm_ht_foreach(gb->edges_by_target_type, free_edge_array, NULL);
    cbm_ht_free(gb->edges_by_target_type);
    cbm_ht_foreach(gb->edges_by_type, free_edge_array, NULL);
    cbm_ht_free(gb->edges_by_type);

    gb->edges_by_source_type = cbm_ht_create(CBM_SZ_256);
    gb->edges_by_target_type = cbm_ht_create(CBM_SZ_256);
    gb->edges_by_type = cbm_ht_create(CBM_SZ_32);

    for (int i = 0; i < gb->edges.count; i++) {
        register_edge_in_indexes(gb, gb->edges.items[i]);
    }
}

static int gbuf_invariant_error(char *err, size_t err_sz, const char *fmt, ...) {
    if (err && err_sz > 0) {
        va_list ap;
        va_start(ap, fmt);
        vsnprintf(err, err_sz, fmt, ap);
        va_end(ap);
    }
    return GB_ERR;
}

static bool gbuf_node_is_live(const cbm_gbuf_t *gb, const cbm_gbuf_node_t *node) {
    if (!gb || !gb->node_by_qn || !node || !node->qualified_name) {
        return false;
    }
    return cbm_ht_get(gb->node_by_qn, node->qualified_name) == node;
}

static bool edge_index_has_id(const edge_ptr_array_t *arr, int64_t edge_id) {
    if (!arr) {
        return false;
    }
    for (int i = 0; i < arr->count; i++) {
        if (arr->items[i] && arr->items[i]->id == edge_id) {
            return true;
        }
    }
    return false;
}

int cbm_gbuf_validate_invariants(const cbm_gbuf_t *gb, char *err, size_t err_sz) {
    if (err && err_sz > 0) {
        err[0] = '\0';
    }
    if (!gb) {
        return gbuf_invariant_error(err, err_sz, "graph buffer is NULL");
    }
    if (!gb->project || !gb->root_path) {
        return gbuf_invariant_error(err, err_sz, "missing project or root_path");
    }
    if (!gb->node_by_qn || !gb->node_by_id || !gb->edge_by_key || !gb->edges_by_source_type ||
        !gb->edges_by_target_type || !gb->edges_by_type) {
        return gbuf_invariant_error(err, err_sz, "lookup indexes are unavailable");
    }
    if (gb->next_id <= GB_INVALID_ID) {
        return gbuf_invariant_error(err, err_sz, "invalid next_id=%lld",
                                    (long long)gb->next_id);
    }

    for (int i = 0; i < gb->nodes.count; i++) {
        const cbm_gbuf_node_t *node = gb->nodes.items[i];
        if (!node) {
            return gbuf_invariant_error(err, err_sz, "node[%d] is NULL", i);
        }
        if (!gbuf_node_is_live(gb, node)) {
            continue;
        }
        if (node->id <= GB_INVALID_ID || node->id >= gb->next_id) {
            return gbuf_invariant_error(err, err_sz, "node id out of range id=%lld next_id=%lld",
                                        (long long)node->id, (long long)gb->next_id);
        }
        char id_buf[CBM_SZ_32];
        make_id_key(id_buf, sizeof(id_buf), node->id);
        if (cbm_ht_get(gb->node_by_id, id_buf) != node) {
            return gbuf_invariant_error(err, err_sz, "node_by_id mismatch id=%lld qn=%s",
                                        (long long)node->id,
                                        node->qualified_name ? node->qualified_name : "");
        }
    }

    for (int i = 0; i < gb->edges.count; i++) {
        const cbm_gbuf_edge_t *edge = gb->edges.items[i];
        if (!edge) {
            return gbuf_invariant_error(err, err_sz, "edge[%d] is NULL", i);
        }
        if (edge->id <= GB_INVALID_ID || edge->source_id <= GB_INVALID_ID ||
            edge->target_id <= GB_INVALID_ID || !edge->type || edge->type[0] == '\0') {
            return gbuf_invariant_error(err, err_sz,
                                        "invalid edge fields edge_id=%lld src=%lld tgt=%lld",
                                        (long long)edge->id, (long long)edge->source_id,
                                        (long long)edge->target_id);
        }
        const cbm_gbuf_node_t *source = cbm_gbuf_find_by_id(gb, edge->source_id);
        const cbm_gbuf_node_t *target = cbm_gbuf_find_by_id(gb, edge->target_id);
        if (!gbuf_node_is_live(gb, source) || !gbuf_node_is_live(gb, target)) {
            return gbuf_invariant_error(err, err_sz,
                                        "edge endpoint missing edge_id=%lld src=%lld tgt=%lld",
                                        (long long)edge->id, (long long)edge->source_id,
                                        (long long)edge->target_id);
        }

        char key[EDGE_KEY_BUF];
        make_edge_key(key, sizeof(key), edge->source_id, edge->target_id, edge->type);
        if (cbm_ht_get(gb->edge_by_key, key) != edge) {
            return gbuf_invariant_error(err, err_sz, "edge_by_key mismatch edge_id=%lld",
                                        (long long)edge->id);
        }
        make_src_type_key(key, sizeof(key), edge->source_id, edge->type);
        if (!edge_index_has_id(cbm_ht_get(gb->edges_by_source_type, key), edge->id)) {
            return gbuf_invariant_error(err, err_sz,
                                        "edges_by_source_type missing edge_id=%lld",
                                        (long long)edge->id);
        }
        make_src_type_key(key, sizeof(key), edge->target_id, edge->type);
        if (!edge_index_has_id(cbm_ht_get(gb->edges_by_target_type, key), edge->id)) {
            return gbuf_invariant_error(err, err_sz,
                                        "edges_by_target_type missing edge_id=%lld",
                                        (long long)edge->id);
        }
        if (!edge_index_has_id(cbm_ht_get(gb->edges_by_type, edge->type), edge->id)) {
            return gbuf_invariant_error(err, err_sz, "edges_by_type missing edge_id=%lld",
                                        (long long)edge->id);
        }
    }
    return 0;
}

/* Release all lookup hash tables (used by dump after building arrays). */
static void release_gbuf_indexes(cbm_gbuf_t *gb) {
    cbm_ht_free(gb->node_by_qn);
    gb->node_by_qn = NULL;
    cbm_ht_foreach(gb->node_by_id, free_key_only, NULL);
    cbm_ht_free(gb->node_by_id);
    gb->node_by_id = NULL;
    cbm_ht_foreach(gb->nodes_by_label, free_node_array, NULL);
    cbm_ht_free(gb->nodes_by_label);
    gb->nodes_by_label = NULL;
    cbm_ht_foreach(gb->nodes_by_name, free_node_array, NULL);
    cbm_ht_free(gb->nodes_by_name);
    gb->nodes_by_name = NULL;
    cbm_ht_foreach(gb->edge_by_key, free_key_only, NULL);
    cbm_ht_free(gb->edge_by_key);
    gb->edge_by_key = NULL;
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
    cbm_gbuf_t *gb = calloc(CBM_ALLOC_ONE, sizeof(cbm_gbuf_t));
    if (!gb) {
        return NULL;
    }

    gb->project = strdup(project ? project : "");
    gb->root_path = strdup(root_path ? root_path : "");
    gb->next_id = SKIP_ONE;
    gb->shared_ids = NULL;

    gb->node_by_qn = cbm_ht_create(CBM_SZ_256);
    gb->node_by_id = cbm_ht_create(CBM_SZ_256);
    gb->nodes_by_label = cbm_ht_create(CBM_SZ_32);
    gb->nodes_by_name = cbm_ht_create(CBM_SZ_256);

    gb->edge_by_key = cbm_ht_create(CBM_SZ_512);
    gb->edges_by_source_type = cbm_ht_create(CBM_SZ_256);
    gb->edges_by_target_type = cbm_ht_create(CBM_SZ_256);
    gb->edges_by_type = cbm_ht_create(CBM_SZ_32);

    gb->intern_pool = cbm_ht_create(CBM_SZ_1K);

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

void cbm_gbuf_free(cbm_gbuf_t *gb) {
    if (!gb) {
        return;
    }

    /* Free each individually-allocated node */
    for (int i = 0; i < gb->nodes.count; i++) {
        cbm_gbuf_node_t *n = gb->nodes.items[i];
        free_node_strings(n);
        free(n);
    }
    cbm_da_free(&gb->nodes);

    /* Free each individually-allocated edge */
    for (int i = 0; i < gb->edges.count; i++) {
        cbm_gbuf_edge_t *e = gb->edges.items[i];
        free_edge_strings(e);
        free(e);
    }
    cbm_da_free(&gb->edges);

    /* Free hash tables — may be NULL if already released by dump_to_sqlite */
    if (gb->node_by_qn) {
        cbm_ht_free(gb->node_by_qn);
    }
    if (gb->node_by_id) {
        cbm_ht_foreach(gb->node_by_id, free_key_only, NULL);
        cbm_ht_free(gb->node_by_id);
    }
    if (gb->nodes_by_label) {
        cbm_ht_foreach(gb->nodes_by_label, free_node_array, NULL);
        cbm_ht_free(gb->nodes_by_label);
    }
    if (gb->nodes_by_name) {
        cbm_ht_foreach(gb->nodes_by_name, free_node_array, NULL);
        cbm_ht_free(gb->nodes_by_name);
    }
    if (gb->edge_by_key) {
        cbm_ht_foreach(gb->edge_by_key, free_key_only, NULL);
        cbm_ht_free(gb->edge_by_key);
    }
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
        free((void *)gb->dump_vectors[i].vector);
    }
    free(gb->dump_vectors);

    /* Free token vector storage */
    for (int i = 0; i < gb->dump_token_vec_count; i++) {
        free((void *)gb->dump_token_vecs[i].token);
        free((void *)gb->dump_token_vecs[i].vector);
    }
    free(gb->dump_token_vecs);

    /* Free interned strings (node label/file_path, edge type) — pool owns one
     * copy each (key == value), freed exactly once via free_key_only. Done after
     * nodes/edges since they borrowed these pointers. */
    if (gb->intern_pool) {
        cbm_ht_foreach(gb->intern_pool, free_key_only, NULL);
        cbm_ht_free(gb->intern_pool);
    }

    free(gb->project);
    free(gb->root_path);
    free(gb);
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
        CBMDumpVector *grown = realloc(gb->dump_vectors, (size_t)new_cap * sizeof(CBMDumpVector));
        if (!grown) {
            return GB_ERR;
        }
        gb->dump_vectors = grown;
        gb->dump_vector_cap = new_cap;
    }
    /* Copy vector data */
    uint8_t *vec_copy = malloc((size_t)vector_len);
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
        CBMDumpTokenVec *grown =
            realloc(gb->dump_token_vecs, (size_t)new_cap * sizeof(CBMDumpTokenVec));
        if (!grown) {
            return GB_ERR;
        }
        gb->dump_token_vecs = grown;
        gb->dump_token_vec_cap = new_cap;
    }
    uint8_t *vec_copy = malloc((size_t)vector_len);
    if (!vec_copy) {
        return GB_ERR;
    }
    memcpy(vec_copy, vector, (size_t)vector_len);

    int idx = gb->dump_token_vec_count;
    gb->dump_token_vecs[idx] = (CBMDumpTokenVec){
        .id = idx + SKIP_ONE, /* 1-based sequential ID */
        .project = gb->project,
        .token = strdup(token),
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

static bool uses_deterministic_source_hint(const char *label) {
    return label && (strcmp(label, "Route") == 0 || strcmp(label, "Section") == 0);
}

static const char *select_upsert_file_path(const cbm_gbuf_node_t *existing, const char *label,
                                           const char *file_path) {
    const char *next = file_path ? file_path : "";
    if (!existing || !uses_deterministic_source_hint(label)) {
        return next;
    }
    const char *cur = existing->file_path ? existing->file_path : "";
    if (cur[0] == '\0') {
        return next;
    }
    if (next[0] == '\0') {
        return cur;
    }
    return strcmp(next, cur) < 0 ? next : cur;
}

static const char *select_upsert_name(const cbm_gbuf_node_t *existing, const char *label,
                                      const char *name) {
    const char *next = name ? name : "";
    if (!existing || !label || strcmp(label, "Route") != 0) {
        return next;
    }
    const char *cur = existing->name ? existing->name : "";
    if (cur[0] == '\0') {
        return next;
    }
    if (next[0] == '\0') {
        return cur;
    }
    return strcmp(next, cur) < 0 ? next : cur;
}

static bool json_props_empty(const char *props) {
    return !props || props[0] == '\0' || strcmp(props, "{}") == 0;
}

static const char *select_upsert_properties_json(const cbm_gbuf_node_t *existing, const char *label,
                                                 const char *properties_json) {
    if (!existing || !label || strcmp(label, "Route") != 0) {
        return properties_json;
    }
    const char *cur = existing->properties_json;
    const char *next = properties_json;
    bool cur_empty = json_props_empty(cur);
    bool next_empty = json_props_empty(next);
    if (next_empty) {
        return cur_empty ? next : cur;
    }
    if (cur_empty) {
        return next;
    }
    size_t cur_len = strlen(cur);
    size_t next_len = strlen(next);
    if (cur_len != next_len) {
        return next_len > cur_len ? next : cur;
    }
    return strcmp(next, cur) < 0 ? next : cur;
}

int64_t cbm_gbuf_upsert_node(cbm_gbuf_t *gb, const char *label, const char *name,
                             const char *qualified_name, const char *file_path, int start_line,
                             int end_line, const char *properties_json) {
    if (!gb || !qualified_name) {
        return 0;
    }

    /* Check if node already exists */
    cbm_gbuf_node_t *existing = cbm_ht_get(gb->node_by_qn, qualified_name);
    if (existing) {
        const char *selected_name = select_upsert_name(existing, label, name);
        const char *selected_file_path = select_upsert_file_path(existing, label, file_path);
        const char *selected_props =
            select_upsert_properties_json(existing, label, properties_json);
        /* Update in-place. name/properties are strdup'd BEFORE freeing old ones
         * (callers may pass existing->name as an argument). label/file_path are
         * interned: gb_intern returns a stable pool pointer (idempotent even when
         * label == existing->label), so the old value is replaced, never freed.
         * Route and Section nodes can intentionally collapse multiple concrete
         * source paths into one QN, so pick display/source hints/properties
         * deterministically where those fields are only representative hints. */
        char *new_name = heap_strdup(selected_name);
        char *new_props = selected_props ? heap_strdup(selected_props) : NULL;
        existing->label = (char *)gb_intern(gb, label);
        free(existing->name);
        existing->name = new_name;
        existing->file_path = (char *)gb_intern(gb, selected_file_path);
        existing->start_line = start_line;
        existing->end_line = end_line;
        if (new_props) {
            free(existing->properties_json);
            existing->properties_json = new_props;
        }
        return existing->id;
    }

    /* Heap-allocate a new node (pointer stays stable across array growth) */
    cbm_gbuf_node_t *node = calloc(CBM_ALLOC_ONE, sizeof(cbm_gbuf_node_t));
    if (!node) {
        return 0;
    }

    int64_t id = alloc_next_id(gb);
    node->id = id;
    node->project = gb->project;
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

const cbm_gbuf_node_t *cbm_gbuf_find_by_qn(const cbm_gbuf_t *gb, const char *qn) {
    if (!gb || !qn) {
        return NULL;
    }
    return cbm_ht_get(gb->node_by_qn, qn);
}

const cbm_gbuf_node_t *cbm_gbuf_find_by_id(const cbm_gbuf_t *gb, int64_t id) {
    if (!gb) {
        return NULL;
    }
    char key[CBM_SZ_32];
    make_id_key(key, sizeof(key), id);
    return cbm_ht_get(gb->node_by_id, key);
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

/* HC-1: DRY helper for name+label+file resolution fallback.
 * Used by pass_calls.c (B2) and pass_normalize.c (B17).
 * Runtime: O(1) hash + O(k) filter where k = name matches (~1-3). */
const cbm_gbuf_node_t *cbm_gbuf_resolve_by_name_in_file(
    const cbm_gbuf_t *gb, const char *qn, const char *file_path,
    const char **label_filter, int label_count)
{
    if (!gb || !qn || !file_path) return NULL;
    const char *dot = strrchr(qn, '.');
    const char *short_name = dot ? dot + 1 : qn;
    if (!short_name[0]) return NULL;

    const cbm_gbuf_node_t **matches = NULL;
    int match_count = 0;
    cbm_gbuf_find_by_name(gb, short_name, &matches, &match_count);

    for (int m = 0; m < match_count; m++) {
        if (!matches[m]->file_path || strcmp(matches[m]->file_path, file_path) != 0)
            continue;
        if (!matches[m]->label) continue;
        for (int l = 0; l < label_count; l++) {
            if (strcmp(matches[m]->label, label_filter[l]) == 0)
                return matches[m];
        }
    }
    return NULL;
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

    CBMHashTable *deleted_set = cbm_ht_create((uint32_t)arr->count);
    if (!deleted_set) {
        return CBM_NOT_FOUND;
    }

    for (int i = 0; i < arr->count; i++) {
        if (gbuf_deleted_set_add_node_id(gb, deleted_set, arr->items[i]) != 0) {
            cbm_ht_free(deleted_set);
            return CBM_NOT_FOUND;
        }
    }

    for (int i = 0; i < arr->count; i++) {
        const cbm_gbuf_node_t *n = arr->items[i];
        char id_buf[CBM_SZ_32];
        make_id_key(id_buf, sizeof(id_buf), n->id);

        /* Remove from primary indexes */
        cbm_ht_delete(gb->node_by_qn, n->qualified_name);
        cbm_ht_delete(gb->node_by_id, id_buf);
    }

    /* Clear the label array */
    cbm_da_clear(arr);

    /* Cascade-delete edges referencing deleted nodes */
    cascade_delete_edges(gb, deleted_set);

    cbm_ht_foreach(deleted_set, free_key_only, NULL);
    cbm_ht_free(deleted_set);
    return 0;
}

int cbm_gbuf_delete_by_file(cbm_gbuf_t *gb, const char *file_path) {
    /* Single-file purge delegates to the batch path (paths=[file_path], count=1)
     * so there is ONE implementation of the node-scan + edge cascade — no
     * duplicated loop (DRY; the batch version does the same work in O(N+E)
     * regardless of path count). */
    if (!gb || !file_path) {
        return CBM_NOT_FOUND;
    }
    return cbm_gbuf_delete_by_paths(gb, &file_path, 1);
}

/* Batch purge: delete every node whose file_path is in `paths`, in a SINGLE
 * pass over gb->nodes + a single edge cascade — O(N+E) total regardless of how
 * many paths are given. Replaces the old loop of cbm_gbuf_delete_by_file calls
 * (one full nodes+edges scan PER file = O(C·(N+E))) in the incremental engine
 * (perf fork-origin #3). Behaviorally identical to calling delete_by_file once
 * per path; just without the repeated full scans. NULL entries in paths are
 * skipped. Keys (paths) are borrowed — not freed by the internal path set. */
int cbm_gbuf_delete_by_paths(cbm_gbuf_t *gb, const char *const *paths, int count) {
    if (!gb) {
        return CBM_NOT_FOUND;
    }
    if (count <= 0 || !paths) {
        return 0;
    }

    CBMHashTable *path_set = cbm_ht_create((uint32_t)count);
    if (!path_set) {
        return CBM_NOT_FOUND;
    }

    for (int i = 0; i < count; i++) {
        if (paths[i] && gbuf_ht_set_flag_checked(path_set, paths[i]) != 0) {
            cbm_ht_free(path_set);
            return CBM_NOT_FOUND;
        }
    }

    CBMHashTable *deleted_set = cbm_ht_create(CBM_SZ_64);
    if (!deleted_set) {
        cbm_ht_free(path_set);
        return CBM_NOT_FOUND;
    }

    int deleted_count = 0;
    int scanned = 0;

    for (int i = 0; i < gb->nodes.count; i++) {
        cbm_gbuf_node_t *n = gb->nodes.items[i];
        scanned++;
        if (!n->file_path || !cbm_ht_get(path_set, n->file_path)) {
            continue;
        }
        if (!n->qualified_name || !cbm_ht_get(gb->node_by_qn, n->qualified_name)) {
            continue;
        }

        if (gbuf_deleted_set_add_node_id(gb, deleted_set, n) != 0) {
            cbm_ht_free(path_set);
            cbm_ht_free(deleted_set);
            return CBM_NOT_FOUND;
        }
        deleted_count++;
    }

    if (deleted_count == 0) {
        cbm_ht_free(path_set);
        cbm_ht_free(deleted_set);
        return 0;
    }

    for (int i = 0; i < gb->nodes.count; i++) {
        cbm_gbuf_node_t *n = gb->nodes.items[i];
        if (!n->qualified_name) {
            continue;
        }

        char id_buf[CBM_SZ_32];
        make_id_key(id_buf, sizeof(id_buf), n->id);
        if (!cbm_ht_get(deleted_set, id_buf)) {
            continue;
        }

        remove_node_from_ptr_array(cbm_ht_get(gb->nodes_by_label, n->label), n->id);
        remove_node_from_ptr_array(cbm_ht_get(gb->nodes_by_name, n->name), n->id);

        cbm_ht_delete(gb->node_by_qn, n->qualified_name);
        cbm_ht_delete(gb->node_by_id, id_buf);

        free(n->qualified_name);
        n->qualified_name = NULL;
    }

    cbm_ht_free(path_set); /* keys borrowed from caller — not freed here */

    cascade_delete_edges(gb, deleted_set);

    cbm_ht_foreach(deleted_set, free_key_only, NULL);
    cbm_ht_free(deleted_set);
    {
        char s_buf[CBM_SZ_16];
        char d_buf[CBM_SZ_16];
        char p_buf[CBM_SZ_16];
        snprintf(s_buf, sizeof(s_buf), "%d", scanned);
        snprintf(d_buf, sizeof(d_buf), "%d", deleted_count);
        snprintf(p_buf, sizeof(p_buf), "%d", count);
        cbm_log_info("gbuf.delete_by_paths", "paths", p_buf, "scanned", s_buf, "deleted", d_buf);
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

    int64_t *old_to_new = calloc((size_t)(max_old_id + SKIP_ONE), sizeof(int64_t));
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
        free(old_to_new);
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
        free(old_to_new);
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

    free(old_to_new);
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

int64_t cbm_gbuf_insert_edge(cbm_gbuf_t *gb, int64_t source_id, int64_t target_id, const char *type,
                             const char *properties_json) {
    if (!gb || !type) {
        return 0;
    }

    /* Check for dedup */
    char key[EDGE_KEY_BUF];
    make_edge_key(key, sizeof(key), source_id, target_id, type);

    cbm_gbuf_edge_t *existing = cbm_ht_get(gb->edge_by_key, key);
    if (existing) {
        /* Merge properties (just replace for now) */
        if (properties_json && strcmp(properties_json, "{}") != 0) {
            free(existing->properties_json);
            existing->properties_json = heap_strdup(properties_json);
        }
        return existing->id;
    }

    /* Heap-allocate a new edge (pointer stays stable) */
    cbm_gbuf_edge_t *edge = calloc(CBM_ALLOC_ONE, sizeof(cbm_gbuf_edge_t));
    if (!edge) {
        return 0;
    }

    int64_t id = alloc_next_id(gb);
    edge->id = id;
    edge->project = gb->project;
    edge->source_id = source_id;
    edge->target_id = target_id;
    edge->type = (char *)gb_intern(gb, type);
    edge->properties_json = heap_strdup(properties_json);

    /* Store pointer in array */
    cbm_da_push(&gb->edges, edge);

    /* Dedup index */
    cbm_ht_set(gb->edge_by_key, strdup(key), edge);

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

static int delete_edges_by_type_filtered(cbm_gbuf_t *gb, const char *type,
                                         const char *prop_substr) {
    if (!gb || !type) {
        return CBM_NOT_FOUND;
    }

    /* Remove edges of the given type from array and dedup index */
    int deleted = 0;
    int write_idx = 0;
    for (int i = 0; i < gb->edges.count; i++) {
        cbm_gbuf_edge_t *e = gb->edges.items[i];
        int type_matches = (strcmp(e->type, type) == 0);
        int props_match =
            (!prop_substr || (e->properties_json && strstr(e->properties_json, prop_substr)));
        if (type_matches && props_match) {
            char key[EDGE_KEY_BUF];
            make_edge_key(key, sizeof(key), e->source_id, e->target_id, e->type);
            const char *ekey = cbm_ht_get_key(gb->edge_by_key, key);
            cbm_ht_delete(gb->edge_by_key, key);
            free((void *)ekey);
            free_edge_strings(e);
            free(e);
            deleted++;
        } else {
            gb->edges.items[write_idx++] = gb->edges.items[i];
        }
    }
    gb->edges.count = write_idx;

    /* Rebuild edge secondary indexes */
    rebuild_edge_secondary_indexes(gb);

    return deleted;
}

int cbm_gbuf_delete_edges_by_type(cbm_gbuf_t *gb, const char *type) {
    int rc = delete_edges_by_type_filtered(gb, type, NULL);
    return rc < 0 ? rc : 0;
}

int cbm_gbuf_delete_edges_by_type_matching_props(cbm_gbuf_t *gb, const char *type,
                                                 const char *prop_substr) {
    if (!prop_substr) {
        return CBM_NOT_FOUND;
    }
    return delete_edges_by_type_filtered(gb, type, prop_substr);
}

/* ── Merge ───────────────────────────────────────────────────────── */

/* Free remap hash table entries (key = heap string, value = heap int64_t*) */
static void free_remap_entry(const char *key, void *val, void *ud) {
    (void)ud;
    free((void *)key);
    free(val);
}

/* Handle QN collision: update dst node fields, record remap if IDs differ.
 * Representative source hints are chosen deterministically for labels that can
 * collapse multiple paths into one QN; worker merge order is intentionally not
 * part of the graph contract. label/file_path are re-interned into dst's pool
 * (sn's pointers belong to src). */
static void merge_update_existing(cbm_gbuf_t *dst, cbm_gbuf_node_t *existing,
                                  const cbm_gbuf_node_t *sn, CBMHashTable **remap) {
    const char *selected_name = select_upsert_name(existing, sn->label, sn->name);
    const char *selected_file_path = select_upsert_file_path(existing, sn->label, sn->file_path);
    const char *selected_props =
        select_upsert_properties_json(existing, sn->label, sn->properties_json);
    char *new_name = heap_strdup(selected_name);
    char *new_props = selected_props ? heap_strdup(selected_props) : NULL;
    existing->label = (char *)gb_intern(dst, sn->label);
    free(existing->name);
    existing->name = new_name;
    existing->file_path = (char *)gb_intern(dst, selected_file_path);
    existing->start_line = sn->start_line;
    existing->end_line = sn->end_line;
    if (new_props) {
        free(existing->properties_json);
        existing->properties_json = new_props;
    }

    if (sn->id != existing->id) {
        if (!*remap) {
            *remap = cbm_ht_create(CBM_SZ_32);
        }
        char key[CBM_SZ_32];
        make_id_key(key, sizeof(key), sn->id);
        int64_t *val = malloc(sizeof(int64_t));
        *val = existing->id;
        cbm_ht_set(*remap, strdup(key), val);
    }
}

/* Copy a non-colliding src node into dst with its original ID. */
static void merge_copy_new_node(cbm_gbuf_t *dst, const cbm_gbuf_node_t *sn) {
    cbm_gbuf_node_t *node = calloc(CBM_ALLOC_ONE, sizeof(cbm_gbuf_node_t));
    if (!node) {
        return;
    }

    node->id = sn->id;
    node->project = dst->project;
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

/* Extract url_path value from a properties JSON string.
 * Returns heap-allocated string or NULL. Caller must free.
 * Parses real JSON: the dump writer feeds this value into idx_edges_url_path,
 * whose backing column is GENERATED AS json_extract(properties,'$.url_path').
 * Naive byte slicing returned the ESCAPED text (and cut at embedded \\")
 * while json_extract yields the unescaped value — the mismatch left rows
 * "missing from index idx_edges_url_path" under PRAGMA integrity_check. */
static char *extract_url_path(const char *props) {
    if (!props || !strstr(props, "\"url_path\"")) {
        return NULL;
    }
    yyjson_doc *doc = yyjson_read(props, strlen(props), 0);
    if (!doc) {
        return NULL;
    }
    char *out = NULL;
    yyjson_val *v = yyjson_obj_get(yyjson_doc_get_root(doc), "url_path");
    if (v && yyjson_is_str(v)) {
        const char *sv = yyjson_get_str(v);
        out = cbm_strndup(sv, strlen(sv));
    }
    yyjson_doc_free(doc);
    return out;
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
    CBMDumpNode *dump_nodes = malloc(cap * sizeof(CBMDumpNode));
    /* Parallel gbuf-node pointers so a streamed partition can free its heavy
     * properties_json after the rows are persisted. NULL on OOM disables the
     * per-partition free (the dump still succeeds). */
    cbm_gbuf_node_t **src = malloc(cap * sizeof(cbm_gbuf_node_t *));
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

/* Build dump-ready edge array with remapped IDs. Returns url_paths via out param. */
static CBMDumpEdge *build_dump_edges(cbm_gbuf_t *gb, const int64_t *temp_to_final,
                                     int64_t max_temp_id, int *out_count, char ***out_url_paths) {
    /* Count valid edges (both endpoints resolved) */
    int valid_edges = 0;
    for (int i = 0; i < gb->edges.count; i++) {
        cbm_gbuf_edge_t *e = gb->edges.items[i];
        if (remap_id(temp_to_final, max_temp_id, e->source_id) > 0 &&
            remap_id(temp_to_final, max_temp_id, e->target_id) > 0) {
            valid_edges++;
        }
    }

    CBMDumpEdge *dump_edges =
        malloc((size_t)(valid_edges > 0 ? valid_edges : SKIP_ONE) * sizeof(CBMDumpEdge));
    char **url_paths = calloc((size_t)(valid_edges > 0 ? valid_edges : SKIP_ONE), sizeof(char *));
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

        const char *props = e->properties_json ? e->properties_json : "{}";
        dump_edges[idx] = (CBMDumpEdge){
            .id = idx + SKIP_ONE,
            .project = gb->project,
            .source_id = src,
            .target_id = tgt,
            .type = e->type,
            .properties = props,
            .url_path = url_path ? url_path : "",
        };
        idx++;
    }

    *out_count = idx;
    *out_url_paths = url_paths;
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

static void free_dump_resources(char **url_paths, int edge_count, CBMDumpEdge *dump_edges,
                                CBMDumpNode *dump_nodes, int64_t *temp_to_final) {
    for (int i = 0; i < edge_count; i++) {
        free(url_paths[i]);
    }
    free(url_paths);
    free(dump_edges);
    free(dump_nodes);
    free(temp_to_final);
}

static int count_live_nodes(cbm_gbuf_t *gb) {
    int count = 0;
    for (int i = 0; i < gb->nodes.count; i++) {
        cbm_gbuf_node_t *n = gb->nodes.items[i];
        if (gbuf_node_is_live(gb, n)) {
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
    CBM_PROF_START(t_release_idx);
    release_gbuf_indexes(gb);
    CBM_PROF_END("dump", "4_release_gbuf_indexes", t_release_idx);

    CBM_PROF_START(t_vec_remap);
    remap_sort_dedup_vectors(gb, temp_to_final, max_temp_id);
    CBM_PROF_END_N("dump", "5_vector_remap_sort", t_vec_remap, gb->dump_vector_count);
}

int cbm_gbuf_dump_to_sqlite(cbm_gbuf_t *gb, const char *path) {
    if (!gb || !path) {
        return CBM_NOT_FOUND;
    }
    char invariant_err[CBM_SZ_512];
    if (cbm_gbuf_validate_invariants(gb, invariant_err, sizeof(invariant_err)) != 0) {
        cbm_log_error("gbuf.dump.invalid_graph", "error", invariant_err);
        return GB_ERR;
    }
    char tmp_path[CBM_SZ_1K];
    int tmp_len = snprintf(tmp_path, sizeof(tmp_path), "%s.tmp.XXXXXX", path);
    if (tmp_len <= 0 || (size_t)tmp_len >= sizeof(tmp_path)) {
        return CBM_NOT_FOUND;
    }
    char wal_path[CBM_SZ_1K];
    char shm_path[CBM_SZ_1K];
    int wal_len = snprintf(wal_path, sizeof(wal_path), "%s-wal", path);
    int shm_len = snprintf(shm_path, sizeof(shm_path), "%s-shm", path);
    if (wal_len <= 0 || (size_t)wal_len >= sizeof(wal_path) || shm_len <= 0 ||
        (size_t)shm_len >= sizeof(shm_path)) {
        return CBM_NOT_FOUND;
    }
    CBM_PROF_START(t_count);
    int live_count = count_live_nodes(gb);
    CBM_PROF_END_N("dump", "1_count_live_nodes", t_count, live_count);

    CBM_PROF_START(t_build_nodes);
    int64_t max_temp_id = gb->next_id;
    int64_t *temp_to_final = calloc((size_t)max_temp_id, sizeof(int64_t));
    if (!temp_to_final) {
        return CBM_NOT_FOUND;
    }

    int node_idx = 0;
    cbm_gbuf_node_t **src_nodes = NULL;
    CBMDumpNode *dump_nodes =
        build_dump_nodes(gb, live_count, temp_to_final, max_temp_id, &node_idx, &src_nodes);
    CBM_PROF_END_N("dump", "2_build_dump_nodes", t_build_nodes, node_idx);

    char indexed_at[CBM_SZ_64];
    generate_iso_timestamp(indexed_at, sizeof(indexed_at));

    int tmp_fd = cbm_mkstemp_s(tmp_path, sizeof(tmp_path));
    if (tmp_fd < 0) {
        free(src_nodes);
        free(dump_nodes);
        free(temp_to_final);
        return CBM_NOT_FOUND;
    }
    cbm_close_fd(tmp_fd);

    /* Stream node rows to the unique temp DB in partitions. Under memory pressure, free each
     * partition's heavy properties_json once persisted — the heavy column is
     * write-once and never read again, so this bounds the dump/finalize peak.
     * The DB output is identical whether or not freeing engages, so non-pressure
     * runs (and tests) leave the gbuf intact (the budget>0 guard keeps an
     * uninitialized budget from ever triggering the free). */
    cbm_db_writer_t *w = cbm_writer_open(tmp_path);
    if (!w) {
        cbm_unlink(tmp_path);
        free(src_nodes);
        free(dump_nodes);
        free(temp_to_final);
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
                free(src_nodes[j]->properties_json);
                src_nodes[j]->properties_json = NULL;
                dump_nodes[j].properties = NULL;
            }
            cbm_mem_collect();
        }
    }
    CBM_PROF_END_N("dump", "2b_stream_append_nodes", t_append, node_idx);

    int edge_idx = 0;
    char **url_paths = NULL;
    CBMDumpEdge *dump_edges = NULL;
    if (rc == 0) {
        CBM_PROF_START(t_build_edges);
        dump_edges = build_dump_edges(gb, temp_to_final, max_temp_id, &edge_idx, &url_paths);
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

    /* Post-write integrity verification (B1 mitigation): verify the temp DB
     * before the atomic rename. A corrupt or unopenable temp file is deleted;
     * the previously published DB, if any, remains in place for readers.
     *
     * Use the _full variant so a path-only defect (root_path the check considers
     * non-absolute, e.g. a relative repo_path like ".") is RETAINED, not deleted
     * — the node/edge data is intact and queries key off project name, not
     * root_path. Deleting on path-only would drop valid DBs whenever a relative
     * path is indexed (caught by self-indexing the repo with repo_path=".").
     * Consistent with #557 resolve_store. */
    if (rc == 0) {
        cbm_store_t *verify = cbm_store_open_path((const char *)tmp_path);
        if (verify) {
            bool path_only = false;
            bool intact = cbm_store_check_integrity_full(verify, &path_only);
            cbm_store_close(verify);
            if (!intact && !path_only) { /* genuine structural corruption only */
                char nodes_str[CBM_SZ_16];
                char edges_str[CBM_SZ_16];
                snprintf(nodes_str, sizeof(nodes_str), "%d", node_idx);
                snprintf(edges_str, sizeof(edges_str), "%d", edge_idx);
                cbm_log_error("dump.verify_corrupt", "path", tmp_path, "action",
                              "deleting corrupt temp db; re-index required", "nodes", nodes_str,
                              "edges", edges_str);
                cbm_unlink(tmp_path);
                rc = GB_ERR;
            }
        } else {
            cbm_log_error("dump.verify_open_failed", "path", tmp_path, "action",
                          "deleting unverified temp db");
            cbm_unlink(tmp_path);
            rc = GB_ERR;
        }
    }
    if (rc == 0) {
        if (cbm_gbuf_test_fail_before_replace_enabled()) {
            cbm_log_error("dump.test_fail_before_replace", "tmp", tmp_path, "path", path);
            cbm_unlink(tmp_path);
            rc = GB_ERR;
        }
    }
    if (rc == 0) {
        cbm_unlink(wal_path);
        cbm_unlink(shm_path);
        if (cbm_replace_file(tmp_path, path) != 0) {
            cbm_log_error("dump.rename_failed", "tmp", tmp_path, "path", path);
            cbm_unlink(tmp_path);
            rc = GB_ERR;
        }
    } else {
        cbm_unlink(tmp_path);
    }

    log_dump_summary(node_idx, edge_idx);
    free_dump_resources(url_paths, edge_idx, dump_edges, dump_nodes, temp_to_final);
    free(src_nodes);
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
    int64_t *temp_to_real = calloc(max_temp_id, sizeof(int64_t));

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

    free(temp_to_real);
    return 0;
}

int cbm_gbuf_merge_into_store(cbm_gbuf_t *gb, cbm_store_t *store) {
    if (!gb || !store) {
        return CBM_NOT_FOUND;
    }

    /* Begin bulk mode — no project wipe (unlike flush_to_store) */
    cbm_store_begin(store);

    /* Build temp_id → real_id map */
    int64_t max_temp_id = gb->next_id;
    int64_t *temp_to_real = calloc(max_temp_id, sizeof(int64_t));

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

    free(temp_to_real);
    return 0;
}
