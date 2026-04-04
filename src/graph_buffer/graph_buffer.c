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
    GB_COL_2 = 2,
    GB_COL_3 = 3,
    GB_COL_4 = 4,
    GB_COL_5 = 5,
    GB_COL_6 = 6,
    GB_COL_7 = 7,
    GB_URL_PATH_PREFIX = 12, /* strlen(""url_path":"") */
};
#include "graph_buffer/graph_buffer.h"
#include "store/store.h"
#include "sqlite_writer.h"
#include "foundation/hash_table.h"
#include "foundation/compat.h"
#include "foundation/log.h"
#include "foundation/dyn_array.h"
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
};

/* ── Helpers ─────────────────────────────────────────────────────── */

static char *heap_strdup(const char *s) {
    return s ? strdup(s) : strdup("{}");
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

/* Free a single node's owned strings */
static void free_node_strings(cbm_gbuf_node_t *n) {
    free(n->label);
    free(n->name);
    free(n->qualified_name);
    free(n->file_path);
    free(n->properties_json);
}

/* Free a single edge's owned strings */
static void free_edge_strings(cbm_gbuf_edge_t *e) {
    free(e->type);
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

    free(gb->project);
    free(gb->root_path);
    free(gb);
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
        /* Update in-place. Strdup new values BEFORE freeing old ones,
         * because callers may pass existing->label etc. as arguments. */
        char *new_label = heap_strdup(label);
        char *new_name = heap_strdup(name);
        char *new_file = heap_strdup(file_path);
        char *new_props = properties_json ? heap_strdup(properties_json) : NULL;
        free(existing->label);
        existing->label = new_label;
        free(existing->name);
        existing->name = new_name;
        free(existing->file_path);
        existing->file_path = new_file;
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
    node->label = heap_strdup(label);
    node->name = heap_strdup(name);
    node->qualified_name = heap_strdup(qualified_name);
    node->file_path = heap_strdup(file_path);
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
        cbm_ht_set(deleted_set, strdup(id_buf), intptr_to_ptr(SKIP_ONE));

        /* Remove from primary indexes */
        cbm_ht_delete(gb->node_by_qn, n->qualified_name);
        const char *stored_key = cbm_ht_get_key(gb->node_by_id, id_buf);
        cbm_ht_delete(gb->node_by_id, id_buf);
        free((void *)stored_key);
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
        cbm_ht_set(deleted_set, strdup(id_buf), intptr_to_ptr(SKIP_ONE));

        /* Remove from secondary indexes */
        remove_node_from_ptr_array(cbm_ht_get(gb->nodes_by_label, n->label), n->id);
        remove_node_from_ptr_array(cbm_ht_get(gb->nodes_by_name, n->name), n->id);

        /* Remove from primary indexes */
        cbm_ht_delete(gb->node_by_qn, n->qualified_name);
        const char *stored_key = cbm_ht_get_key(gb->node_by_id, id_buf);
        cbm_ht_delete(gb->node_by_id, id_buf);
        free((void *)stored_key);

        /* NULL out QN so dump's liveness check (cbm_ht_get by QN) fails
         * even if a new node with the same QN is inserted later via merge. */
        free(n->qualified_name);
        n->qualified_name = NULL;
        deleted_count++;
    }

    if (deleted_count == 0) {
        cbm_ht_free(deleted_set);
        return 0;
    }

    /* Cascade-delete edges referencing deleted nodes */
    cascade_delete_edges(gb, deleted_set);

    cbm_ht_foreach(deleted_set, free_key_only, NULL);
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
    edge->type = heap_strdup(type);
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
            make_edge_key(key, sizeof(key), e->source_id, e->target_id, e->type);
            const char *ekey = cbm_ht_get_key(gb->edge_by_key, key);
            cbm_ht_delete(gb->edge_by_key, key);
            free((void *)ekey);
            free_edge_strings(e);
            free(e);
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
    free((void *)key);
    free(val);
}

/* Handle QN collision: update dst node fields (src wins), record remap if IDs differ. */
static void merge_update_existing(cbm_gbuf_node_t *existing, const cbm_gbuf_node_t *sn,
                                  CBMHashTable **remap) {
    free(existing->label);
    existing->label = heap_strdup(sn->label);
    free(existing->name);
    existing->name = heap_strdup(sn->name);
    free(existing->file_path);
    existing->file_path = heap_strdup(sn->file_path);
    existing->start_line = sn->start_line;
    existing->end_line = sn->end_line;
    if (sn->properties_json) {
        free(existing->properties_json);
        existing->properties_json = heap_strdup(sn->properties_json);
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
    node->label = heap_strdup(sn->label);
    node->name = heap_strdup(sn->name);
    node->qualified_name = heap_strdup(sn->qualified_name);
    node->file_path = heap_strdup(sn->file_path);
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
            merge_update_existing(existing, sn, &remap);
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
 * Returns heap-allocated string or NULL. Caller must free. */
static char *extract_url_path(const char *props) {
    if (!props) {
        return NULL;
    }
    const char *key = strstr(props, "\"url_path\":\"");
    if (!key) {
        return NULL;
    }
    key += GB_URL_PATH_PREFIX; /* strlen("\"url_path\":\"") */
    const char *end = strchr(key, '"');
    if (!end || end <= key) {
        return NULL;
    }
    return cbm_strndup(key, (size_t)(end - key));
}

/* Remap a temp edge ID to its final sequential ID, or 0 if out of range. */
static int64_t remap_id(const int64_t *temp_to_final, int64_t max_temp_id, int64_t temp_id) {
    return (temp_id < max_temp_id) ? temp_to_final[temp_id] : 0;
}

/* Build dump-ready node array with sequential IDs. Populates temp_to_final mapping. */
static CBMDumpNode *build_dump_nodes(cbm_gbuf_t *gb, int live_count, int64_t *temp_to_final,
                                     int64_t max_temp_id, int *out_count) {
    CBMDumpNode *dump_nodes =
        malloc((size_t)(live_count > 0 ? live_count : SKIP_ONE) * sizeof(CBMDumpNode));
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
        idx++;
    }

    *out_count = idx;
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

int cbm_gbuf_dump_to_sqlite(cbm_gbuf_t *gb, const char *path) {
    if (!gb || !path) {
        return CBM_NOT_FOUND;
    }

    /* Count live nodes (not deleted from QN index) */
    int live_count = 0;
    for (int i = 0; i < gb->nodes.count; i++) {
        cbm_gbuf_node_t *n = gb->nodes.items[i];
        if (n->qualified_name && cbm_ht_get(gb->node_by_qn, n->qualified_name)) {
            live_count++;
        }
    }

    /* Build temp→final ID mapping */
    int64_t max_temp_id = gb->next_id;
    int64_t *temp_to_final = calloc((size_t)max_temp_id, sizeof(int64_t));
    if (!temp_to_final) {
        return CBM_NOT_FOUND;
    }

    int node_idx = 0;
    CBMDumpNode *dump_nodes =
        build_dump_nodes(gb, live_count, temp_to_final, max_temp_id, &node_idx);

    int edge_idx = 0;
    char **url_paths = NULL;
    CBMDumpEdge *dump_edges =
        build_dump_edges(gb, temp_to_final, max_temp_id, &edge_idx, &url_paths);

    /* Generate ISO 8601 timestamp */
    time_t now = time(NULL);
    struct tm tm_buf;
    struct tm *tm_val = cbm_gmtime_r(&now, &tm_buf);
    char indexed_at[CBM_SZ_64];
    if (strftime(indexed_at, sizeof(indexed_at), "%Y-%m-%dT%H:%M:%SZ", tm_val) == 0) {
        snprintf(indexed_at, sizeof(indexed_at), "1970-01-01T00:00:00Z");
    }

    /* Release lookup tables — no longer needed now that dump arrays are built.
     * Frees hash tables (~200-400MB on large codebases) while keeping the
     * raw node/edge data alive for cbm_write_db() to read via dump arrays. */
    release_gbuf_indexes(gb);

    /* Write directly to final path — no .tmp + rename.
     * Callers must delete the old .db before calling this (reindex)
     * or ensure no file exists (first index). */
    int rc = cbm_write_db(path, gb->project, gb->root_path, indexed_at, dump_nodes, node_idx,
                          dump_edges, edge_idx);

    {
        char b1[CBM_SZ_16];
        char b2[CBM_SZ_16];
        snprintf(b1, sizeof(b1), "%d", node_idx);
        snprintf(b2, sizeof(b2), "%d", edge_idx);
        cbm_log_info("gbuf.dump", "nodes", b1, "edges", b2);
    }

    /* Cleanup */
    for (int i = 0; i < edge_idx; i++) {
        free(url_paths[i]);
    }
    free(url_paths);
    free(dump_edges);
    free(dump_nodes);
    free(temp_to_final);

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

    /* Begin bulk mode — no project wipe */
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
