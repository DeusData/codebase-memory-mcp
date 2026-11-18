/*
 * neo4j_store.c — Answer a store's graph reads from Neo4j.
 *
 * Every query is scoped to one mirrored graph: {db: <project db>, gen: <the
 * CbmProject's published generation>}, read once when the store is attached
 * so all reads of one request see one consistent generation.
 */
#include "neo4j/neo4j_store.h"
#include "neo4j/neo4j_client.h"
#include "neo4j/neo4j_sync.h"

#include "foundation/log.h"
#include "foundation/limits.h"
#include "foundation/platform.h"

#include <ctype.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define N4S_GEN_MAX 128
/* Server-side limit for user (native / fallback) queries: below the HTTP
 * client's 120 s I/O timeout, so Neo4j stops the query itself and the reply
 * says so, instead of the client giving up while the server keeps working. */
#define N4S_NATIVE_MAX_EXEC_MS 60000
/* Server-side limit for the store's own reads (batched node/edge reads, scan
 * pages, counts): the largest measured on an 817k-node project took 6.2 s,
 * so a read still running after a minute is hung, and its error should not
 * wait out the 120 s I/O timeout. */
#define N4S_READ_MAX_EXEC_MS 60000
#define N4S_ID_CHUNK 5000

/* Node columns, in cbm_node_t order. */
#define N4S_NODE_COLS(v)                                                                       \
    v ".id, " v ".project, " v ".label, " v ".name, " v ".qualified_name, " v ".file_path, " v \
      ".start_line, " v ".end_line, " v ".properties_json"
#define N4S_NODE_NCOLS 9
/* The same columns under their own names, for a query that also returns
 * edge columns over the same variables (a RETURN may not repeat a name). */
#define N4S_FAR_COLS(v)                                                                           \
    v ".id AS f_id, " v ".project AS f_project, " v ".label AS f_label, " v ".name AS f_name, " v \
      ".qualified_name AS f_qn, " v ".file_path AS f_file, " v ".start_line AS f_start, " v       \
      ".end_line AS f_end, " v ".properties_json AS f_props"

/* Edges prefetched for a set of nodes in one round trip (one per direction).
 * types == NULL holds every type; otherwise only the listed ones. */
typedef struct {
    bool valid;
    char **types;
    int type_count;
    int64_t *ids; /* sorted, unique: the nodes whose edges are all here */
    int id_count;
    cbm_edge_t *edges; /* sorted by (key node, edge id); key = target if by_target */
    int edge_count;
} n4s_edge_cache_t;

/* Nodes read alongside edges (the far end of each edge), sorted by id. */
typedef struct {
    cbm_node_t *nodes;
    int count;
} n4s_node_cache_t;

typedef struct {
    cbm_neo4j_t *c;
    char db[CBM_NEO4J_URI_MAX];
    int64_t gen;
    char err[CBM_NEO4J_ERR_MAX];
    uint64_t queries; /* round trips issued (per-request backend report) */
    /* A graph walk asks for one node's edges, then each neighbour, one round
     * trip each: hundreds of thousands of them on a busy node. Edge reads
     * therefore also return the far-end nodes, and a caller about to expand
     * many nodes prefetches all their edges at once (cbm_store_prefetch_edges).
     * Both are read-through caches of the same immutable generation. */
    n4s_edge_cache_t edge_cache[2];  /* [0] by source, [1] by target */
    n4s_node_cache_t chunk_nodes[2]; /* far ends of the last prefetch, per direction */
    n4s_node_cache_t last_nodes;     /* far ends of the last single-node edge read */
    /* Wall-clock ms after which a multi-page read gives up between pages
     * (CBM_STORE_SCAN_LIMIT); 0 = none. Set by the Cypher engine. */
    uint64_t read_deadline_ms;
} n4s_ctx_t;

/* ── Query plumbing ─────────────────────────────────────────────── */

typedef struct {
    cbm_neo4j_batch_t b;
    yyjson_mut_val *params;
} n4s_q_t;

static int q_begin(n4s_ctx_t *x, n4s_q_t *q, const char *cypher) {
    if (cbm_neo4j_batch_init(&q->b) != 0) {
        return -1;
    }
    q->params = cbm_neo4j_batch_add(&q->b, cypher);
    yyjson_mut_obj_add_strcpy(q->b.doc, q->params, "db", x->db);
    yyjson_mut_obj_add_int(q->b.doc, q->params, "gen", x->gen);
    return 0;
}

static void q_str(n4s_q_t *q, const char *key, const char *val) {
    if (val) {
        yyjson_mut_obj_add_strcpy(q->b.doc, q->params, key, val);
    } else {
        yyjson_mut_obj_add_null(q->b.doc, q->params, key);
    }
}

static void q_int(n4s_q_t *q, const char *key, int64_t val) {
    yyjson_mut_obj_add_int(q->b.doc, q->params, key, val);
}

static void q_ids(n4s_q_t *q, const char *key, const int64_t *ids, int n) {
    yyjson_mut_val *arr = yyjson_mut_arr(q->b.doc);
    for (int i = 0; i < n; i++) {
        yyjson_mut_arr_add_int(q->b.doc, arr, ids[i]);
    }
    yyjson_mut_obj_add_val(q->b.doc, q->params, key, arr);
}

/* Edge types: an empty list means CALLS, as in the SQLite BFS. */
static void q_types(n4s_q_t *q, const char *key, const char **types, int n) {
    yyjson_mut_val *arr = yyjson_mut_arr(q->b.doc);
    if (n <= 0) {
        yyjson_mut_arr_add_str(q->b.doc, arr, "CALLS");
    }
    for (int i = 0; i < n; i++) {
        yyjson_mut_arr_add_strcpy(q->b.doc, arr, types[i]);
    }
    yyjson_mut_obj_add_val(q->b.doc, q->params, key, arr);
}

/* Run and keep the result; on failure record the error in the ctx. */
static int q_run(n4s_ctx_t *x, n4s_q_t *q, cbm_neo4j_result_t *r) {
    x->queries++;
    int rc = cbm_neo4j_batch_run(x->c, &q->b, r);
    cbm_neo4j_batch_free(&q->b);
    if (rc != 0) {
        (void)snprintf(x->err, sizeof(x->err), "%s", cbm_neo4j_error(x->c));
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

static void node_from_row(yyjson_val *entry, size_t off, cbm_node_t *n) {
    memset(n, 0, sizeof(*n));
    n->id = cbm_neo4j_val_int(cbm_neo4j_row_col(entry, off + 0), 0);
    n->project = cbm_neo4j_val_strdup(cbm_neo4j_row_col(entry, off + 1), "");
    n->label = cbm_neo4j_val_strdup(cbm_neo4j_row_col(entry, off + 2), "");
    n->name = cbm_neo4j_val_strdup(cbm_neo4j_row_col(entry, off + 3), "");
    n->qualified_name = cbm_neo4j_val_strdup(cbm_neo4j_row_col(entry, off + 4), "");
    n->file_path = cbm_neo4j_val_strdup(cbm_neo4j_row_col(entry, off + 5), "");
    n->start_line = (int)cbm_neo4j_val_int(cbm_neo4j_row_col(entry, off + 6), 0);
    n->end_line = (int)cbm_neo4j_val_int(cbm_neo4j_row_col(entry, off + 7), 0);
    n->properties_json = cbm_neo4j_val_strdup(cbm_neo4j_row_col(entry, off + 8), "{}");
}

static void node_free(cbm_node_t *n) {
    free((void *)n->project);
    free((void *)n->label);
    free((void *)n->name);
    free((void *)n->qualified_name);
    free((void *)n->file_path);
    free((void *)n->properties_json);
    memset(n, 0, sizeof(*n));
}

static int collect_nodes(n4s_ctx_t *x, n4s_q_t *q, cbm_node_t **out, int *count) {
    *out = NULL;
    *count = 0;
    cbm_neo4j_result_t r;
    if (q_run(x, q, &r) != CBM_STORE_OK) {
        return CBM_STORE_ERR;
    }
    yyjson_val *data = cbm_neo4j_result_data(&r, 0);
    size_t n = yyjson_arr_size(data);
    if (n > 0) {
        cbm_node_t *arr = calloc(n, sizeof(cbm_node_t));
        if (!arr) {
            cbm_neo4j_result_free(&r);
            return CBM_STORE_ERR;
        }
        yyjson_arr_iter it = yyjson_arr_iter_with(data);
        for (size_t i = 0; i < n; i++) {
            node_from_row(yyjson_arr_iter_next(&it), 0, &arr[i]);
        }
        *out = arr;
        *count = (int)n;
    }
    cbm_neo4j_result_free(&r);
    return CBM_STORE_OK;
}

/* ── Ops ────────────────────────────────────────────────────────── */

static void caches_free(n4s_ctx_t *x);
static const cbm_node_t *node_cache_find(const n4s_node_cache_t *c, int64_t id);
static int node_copy(const cbm_node_t *src, cbm_node_t *dst);

static void op_close(void *ctx) {
    n4s_ctx_t *x = ctx;
    if (x) {
        caches_free(x);
        cbm_neo4j_close(x->c);
        free(x);
    }
}

static const char *op_error(void *ctx) {
    return ((n4s_ctx_t *)ctx)->err;
}

static int one_node(n4s_ctx_t *x, n4s_q_t *q, cbm_node_t *out) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (collect_nodes(x, q, &nodes, &count) != CBM_STORE_OK) {
        return CBM_STORE_ERR;
    }
    if (count == 0) {
        return CBM_STORE_NOT_FOUND;
    }
    *out = nodes[0];
    for (int i = 1; i < count; i++) {
        node_free(&nodes[i]);
    }
    free(nodes);
    return CBM_STORE_OK;
}

static int op_find_node_by_id(void *ctx, int64_t id, cbm_node_t *out) {
    n4s_ctx_t *x = ctx;
    const cbm_node_t *hit = node_cache_find(&x->chunk_nodes[0], id);
    if (!hit) {
        hit = node_cache_find(&x->chunk_nodes[1], id);
    }
    if (!hit) {
        hit = node_cache_find(&x->last_nodes, id);
    }
    if (hit) {
        return node_copy(hit, out);
    }
    n4s_q_t q;
    if (q_begin(x, &q,
                "MATCH (n:CbmNode {db: $db, gen: $gen, id: $id}) RETURN " N4S_NODE_COLS(
                    "n") " LIMIT 1") != 0) {
        return CBM_STORE_ERR;
    }
    q_int(&q, "id", id);
    return one_node(x, &q, out);
}

static int op_find_node_by_qn(void *ctx, const char *project, const char *qn, cbm_node_t *out) {
    n4s_ctx_t *x = ctx;
    n4s_q_t q;
    if (q_begin(x, &q,
                "MATCH (n:CbmNode {db: $db, gen: $gen, qualified_name: $qn}) "
                "WHERE $project IS NULL OR n.project = $project RETURN " N4S_NODE_COLS(
                    "n") " ORDER BY n.id LIMIT 1") != 0) {
        return CBM_STORE_ERR;
    }
    q_str(&q, "qn", qn);
    q_str(&q, "project", project);
    return one_node(x, &q, out);
}

static int op_find_nodes(void *ctx, cbm_remote_node_query_t kind, const char *project,
                         const char *arg, cbm_node_t **out, int *count) {
    n4s_ctx_t *x = ctx;
    const char *cypher = NULL;
    switch (kind) {
    case CBM_REMOTE_NODES_ALL:
        cypher = "MATCH (n:CbmNode {db: $db, gen: $gen}) "
                 "WHERE $project IS NULL OR n.project = $project RETURN " N4S_NODE_COLS(
                     "n") " ORDER BY n.file_path, n.id";
        break;
    case CBM_REMOTE_NODES_BY_NAME:
        cypher = "MATCH (n:CbmNode {db: $db, gen: $gen, name: $arg}) "
                 "WHERE $project IS NULL OR n.project = $project RETURN " N4S_NODE_COLS(
                     "n") " ORDER BY n.id";
        break;
    case CBM_REMOTE_NODES_BY_LABEL:
        cypher = "MATCH (n:CbmNode {db: $db, gen: $gen, label: $arg}) "
                 "WHERE $project IS NULL OR n.project = $project RETURN " N4S_NODE_COLS(
                     "n") " ORDER BY n.id";
        break;
    case CBM_REMOTE_NODES_BY_FILE:
        cypher = "MATCH (n:CbmNode {db: $db, gen: $gen, file_path: $arg}) "
                 "WHERE $project IS NULL OR n.project = $project RETURN " N4S_NODE_COLS(
                     "n") " ORDER BY n.id";
        break;
    /* Candidate supersets: the caller re-applies the exact predicate. */
    case CBM_REMOTE_NODES_QN_ENDS_CI:
        cypher = "MATCH (n:CbmNode {db: $db, gen: $gen}) "
                 "WHERE ($project IS NULL OR n.project = $project) AND "
                 "(n.qualified_name = $arg OR "
                 "toLower(n.qualified_name) ENDS WITH toLower('.' + $arg)) RETURN " N4S_NODE_COLS(
                     "n") " ORDER BY n.id";
        break;
    case CBM_REMOTE_NODES_QN_CONTAINS_CI:
        cypher = "MATCH (n:CbmNode {db: $db, gen: $gen}) "
                 "WHERE ($project IS NULL OR n.project = $project) AND "
                 "toLower(n.qualified_name) CONTAINS toLower($arg) RETURN " N4S_NODE_COLS(
                     "n") " ORDER BY n.id";
        break;
    default:
        return CBM_STORE_ERR;
    }
    n4s_q_t q;
    if (q_begin(x, &q, cypher) != 0) {
        return CBM_STORE_ERR;
    }
    q_str(&q, "project", project);
    q_str(&q, "arg", arg ? arg : "");
    return collect_nodes(x, &q, out, count);
}

/* ── Read-through caches (see n4s_ctx_t) ───────────────────────── */

static char *n4s_dup(const char *s) {
    size_t n = strlen(s ? s : "");
    char *d = malloc(n + 1);
    if (d) {
        memcpy(d, s ? s : "", n + 1);
    }
    return d;
}

static int node_copy(const cbm_node_t *src, cbm_node_t *dst) {
    memset(dst, 0, sizeof(*dst));
    dst->id = src->id;
    dst->start_line = src->start_line;
    dst->end_line = src->end_line;
    dst->project = n4s_dup(src->project);
    dst->label = n4s_dup(src->label);
    dst->name = n4s_dup(src->name);
    dst->qualified_name = n4s_dup(src->qualified_name);
    dst->file_path = n4s_dup(src->file_path);
    dst->properties_json = n4s_dup(src->properties_json ? src->properties_json : "{}");
    if (!dst->project || !dst->label || !dst->name || !dst->qualified_name || !dst->file_path ||
        !dst->properties_json) {
        node_free(dst);
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

static void edge_fields_free(cbm_edge_t *e) {
    free((void *)e->project);
    free((void *)e->type);
    free((void *)e->properties_json);
    memset(e, 0, sizeof(*e));
}

static int edge_copy(const cbm_edge_t *src, cbm_edge_t *dst) {
    *dst = *src;
    dst->project = n4s_dup(src->project);
    dst->type = n4s_dup(src->type);
    dst->properties_json = n4s_dup(src->properties_json ? src->properties_json : "{}");
    if (!dst->project || !dst->type || !dst->properties_json) {
        edge_fields_free(dst);
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

static void node_cache_free(n4s_node_cache_t *c) {
    for (int i = 0; i < c->count; i++) {
        node_free(&c->nodes[i]);
    }
    free(c->nodes);
    c->nodes = NULL;
    c->count = 0;
}

/* Take ownership of nodes[0..n): sort by id and drop duplicates. */
static void node_cache_set(n4s_node_cache_t *c, cbm_node_t *nodes, int n) {
    node_cache_free(c);
    if (n <= 0) {
        free(nodes);
        return;
    }
    qsort(nodes, (size_t)n, sizeof(cbm_node_t), cbm_store_node_cmp_id);
    int w = 0;
    for (int i = 0; i < n; i++) {
        if (w > 0 && nodes[w - 1].id == nodes[i].id) {
            node_free(&nodes[i]);
            continue;
        }
        if (w != i) {
            nodes[w] = nodes[i];
        }
        w++;
    }
    c->nodes = nodes;
    c->count = w;
}

static const cbm_node_t *node_cache_find(const n4s_node_cache_t *c, int64_t id) {
    cbm_node_t key = {.id = id};
    return c->count > 0 ? bsearch(&key, c->nodes, (size_t)c->count, sizeof(cbm_node_t),
                                  cbm_store_node_cmp_id)
                        : NULL;
}

static void edge_cache_free(n4s_edge_cache_t *c) {
    for (int i = 0; i < c->type_count; i++) {
        free(c->types[i]);
    }
    free(c->types);
    free(c->ids);
    for (int i = 0; i < c->edge_count; i++) {
        edge_fields_free(&c->edges[i]);
    }
    free(c->edges);
    memset(c, 0, sizeof(*c));
}

static void caches_free(n4s_ctx_t *x) {
    edge_cache_free(&x->edge_cache[0]);
    edge_cache_free(&x->edge_cache[1]);
    node_cache_free(&x->chunk_nodes[0]);
    node_cache_free(&x->chunk_nodes[1]);
    node_cache_free(&x->last_nodes);
}

static int cmp_i64_asc(const void *a, const void *b) {
    int64_t x = *(const int64_t *)a;
    int64_t y = *(const int64_t *)b;
    return (x > y) - (x < y);
}

static int cmp_edge_by_source(const void *a, const void *b) {
    const cbm_edge_t *x = a;
    const cbm_edge_t *y = b;
    if (x->source_id != y->source_id) {
        return (x->source_id > y->source_id) - (x->source_id < y->source_id);
    }
    return (x->id > y->id) - (x->id < y->id);
}

static int cmp_edge_by_target(const void *a, const void *b) {
    const cbm_edge_t *x = a;
    const cbm_edge_t *y = b;
    if (x->target_id != y->target_id) {
        return (x->target_id > y->target_id) - (x->target_id < y->target_id);
    }
    return (x->id > y->id) - (x->id < y->id);
}

static bool ids_contain(const int64_t *ids, int n, int64_t id) {
    return n > 0 && bsearch(&id, ids, (size_t)n, sizeof(int64_t), cmp_i64_asc) != NULL;
}

static bool cache_has_type(const n4s_edge_cache_t *c, const char *type) {
    if (!c->types) {
        return true; /* every type was fetched */
    }
    if (!type) {
        return false; /* asked for all types, only some are here */
    }
    for (int i = 0; i < c->type_count; i++) {
        if (strcmp(c->types[i], type) == 0) {
            return true;
        }
    }
    return false;
}

/* A cached answer to find_edges(node_id, by_target, type), in r.id order.
 * Returns CBM_STORE_NOT_FOUND when the cache cannot answer it. */
static int edge_cache_lookup(n4s_ctx_t *x, int64_t node_id, bool by_target, const char *type,
                             cbm_edge_t **out, int *count) {
    const n4s_edge_cache_t *c = &x->edge_cache[by_target ? 1 : 0];
    if (!c->valid || !cache_has_type(c, type) || !ids_contain(c->ids, c->id_count, node_id)) {
        return CBM_STORE_NOT_FOUND;
    }
    /* first edge whose key is node_id */
    int lo = 0;
    int hi = c->edge_count;
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        int64_t key = by_target ? c->edges[mid].target_id : c->edges[mid].source_id;
        if (key < node_id) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    int n = 0;
    for (int i = lo; i < c->edge_count; i++) {
        int64_t key = by_target ? c->edges[i].target_id : c->edges[i].source_id;
        if (key != node_id) {
            break;
        }
        if (!type || strcmp(c->edges[i].type, type) == 0) {
            n++;
        }
    }
    *out = NULL;
    *count = 0;
    if (n == 0) {
        return CBM_STORE_OK;
    }
    cbm_edge_t *arr = calloc((size_t)n, sizeof(cbm_edge_t));
    if (!arr) {
        return CBM_STORE_ERR;
    }
    int w = 0;
    for (int i = lo; i < c->edge_count && w < n; i++) {
        int64_t key = by_target ? c->edges[i].target_id : c->edges[i].source_id;
        if (key != node_id) {
            break;
        }
        if (type && strcmp(c->edges[i].type, type) != 0) {
            continue;
        }
        if (edge_copy(&c->edges[i], &arr[w]) != CBM_STORE_OK) {
            for (int k = 0; k < w; k++) {
                edge_fields_free(&arr[k]);
            }
            free(arr);
            return CBM_STORE_ERR;
        }
        w++;
    }
    *out = arr;
    *count = w;
    return CBM_STORE_OK;
}

/* Edge rows: r.id, project, source, target, type, properties_json, then (when
 * far_ends) the N4S_NODE_COLS of the node at the other end of the edge. */
static int edges_from_data(yyjson_val *data, cbm_edge_t **out, int *count, cbm_node_t **far,
                           int *far_count) {
    *out = NULL;
    *count = 0;
    if (far) {
        *far = NULL;
        *far_count = 0;
    }
    size_t n = yyjson_arr_size(data);
    if (n > INT_MAX) {
        return CBM_STORE_ERR;
    }
    if (n > 0) {
        cbm_edge_t *arr = calloc(n, sizeof(cbm_edge_t));
        cbm_node_t *nodes = far ? calloc(n, sizeof(cbm_node_t)) : NULL;
        if (!arr || (far && !nodes)) {
            free(arr);
            free(nodes);
            return CBM_STORE_ERR;
        }
        yyjson_arr_iter it = yyjson_arr_iter_with(data);
        for (size_t i = 0; i < n; i++) {
            yyjson_val *e = yyjson_arr_iter_next(&it);
            arr[i].id = cbm_neo4j_val_int(cbm_neo4j_row_col(e, 0), 0);
            arr[i].project = cbm_neo4j_val_strdup(cbm_neo4j_row_col(e, 1), "");
            arr[i].source_id = cbm_neo4j_val_int(cbm_neo4j_row_col(e, 2), 0);
            arr[i].target_id = cbm_neo4j_val_int(cbm_neo4j_row_col(e, 3), 0);
            arr[i].type = cbm_neo4j_val_strdup(cbm_neo4j_row_col(e, 4), "");
            arr[i].properties_json = cbm_neo4j_val_strdup(cbm_neo4j_row_col(e, 5), "{}");
            if (nodes) {
                node_from_row(e, 6, &nodes[i]);
            }
        }
        *out = arr;
        *count = (int)n;
        if (far) {
            *far = nodes;
            *far_count = (int)n;
        }
    }
    return CBM_STORE_OK;
}

static int collect_edges_ex(n4s_ctx_t *x, n4s_q_t *q, cbm_edge_t **out, int *count,
                            cbm_node_t **far, int *far_count) {
    *out = NULL;
    *count = 0;
    if (far) {
        *far = NULL;
        *far_count = 0;
    }
    cbm_neo4j_result_t r;
    if (q_run(x, q, &r) != CBM_STORE_OK) {
        return CBM_STORE_ERR;
    }
    int rc = edges_from_data(cbm_neo4j_result_data(&r, 0), out, count, far, far_count);
    cbm_neo4j_result_free(&r);
    return rc;
}

static int collect_edges(n4s_ctx_t *x, n4s_q_t *q, cbm_edge_t **out, int *count) {
    return collect_edges_ex(x, q, out, count, NULL, NULL);
}

#define N4S_EDGE_RET \
    " RETURN r.id, a.project, a.id, b.id, coalesce(r.type, type(r)), r.properties_json"

/* Edges one prefetch request may pull: more would be one large,
 * uninterruptible HTTP response. */
#define N4S_PREFETCH_EDGE_CAP 50000
/* A node with more edges than this (in the prefetched direction, any type)
 * is a hub: it is left out of a prefetch and read on its own, in pages. */
#define N4S_HUB_DEGREE 5000
/* Rows per page of one node's edges. */
#define N4S_EDGE_PAGE 50000

/* Tests shrink these to exercise hubs and paging on a small graph. */
static _Thread_local int g_n4s_prefetch_cap = N4S_PREFETCH_EDGE_CAP;
static _Thread_local int g_n4s_hub_degree = N4S_HUB_DEGREE;
static _Thread_local int g_n4s_edge_page = N4S_EDGE_PAGE;

#ifdef CBM_ENABLE_TEST_SEAMS
void cbm_neo4j_test_set_edge_limits(int prefetch_cap, int hub_degree, int edge_page) {
    g_n4s_prefetch_cap = prefetch_cap > 0 ? prefetch_cap : N4S_PREFETCH_EDGE_CAP;
    g_n4s_hub_degree = hub_degree > 0 ? hub_degree : N4S_HUB_DEGREE;
    g_n4s_edge_page = edge_page > 0 ? edge_page : N4S_EDGE_PAGE;
}
#endif

static bool read_deadline_passed(const n4s_ctx_t *x) {
    return x->read_deadline_ms != 0 && cbm_now_ms() >= x->read_deadline_ms;
}

/* Append src[0..n) to the growing (edges, far) pair arrays, taking ownership. */
static int edges_append(cbm_edge_t **edges, cbm_node_t **far, int *count, int *cap,
                        cbm_edge_t *src_e, cbm_node_t *src_n, int n) {
    if (n <= 0) {
        return CBM_STORE_OK;
    }
    if (*count > INT_MAX - n) {
        return CBM_STORE_ERR;
    }
    if (*count + n > *cap) {
        int next = *cap > 0 ? *cap : 1024;
        while (next < *count + n) {
            next = next > INT_MAX / 2 ? INT_MAX : next * 2;
        }
        cbm_edge_t *ge = realloc(*edges, (size_t)next * sizeof(cbm_edge_t));
        if (!ge) {
            return CBM_STORE_ERR;
        }
        *edges = ge;
        cbm_node_t *gn = realloc(*far, (size_t)next * sizeof(cbm_node_t));
        if (!gn) {
            return CBM_STORE_ERR;
        }
        *far = gn;
        *cap = next;
    }
    memcpy(*edges + *count, src_e, (size_t)n * sizeof(cbm_edge_t));
    memcpy(*far + *count, src_n, (size_t)n * sizeof(cbm_node_t));
    *count += n;
    return CBM_STORE_OK;
}

/* One node's edges in r.id order, g_n4s_edge_page rows per round trip (a
 * hub's 200k callers were one 6 s response). A normal node is one page. The
 * read deadline is checked between pages: past it the rows read are dropped
 * and CBM_STORE_SCAN_LIMIT returned, so the caller's budget holds. */
static int op_find_edges(void *ctx, int64_t node_id, bool by_target, const char *type,
                         cbm_edge_t **out, int *count) {
    n4s_ctx_t *x = ctx;
    int cached = edge_cache_lookup(x, node_id, by_target, type, out, count);
    if (cached != CBM_STORE_NOT_FOUND) {
        return cached;
    }
    *out = NULL;
    *count = 0;
    /* The far ends come back too: a caller walking these edges looks every
     * one of them up next (find_node_by_id answers from last_nodes). */
    const char *cypher =
        by_target ? "MATCH (a:CbmNode)-[r]->(b:CbmNode {db: $db, gen: $gen, id: $id}) "
                    "WHERE a.db = $db AND a.gen = $gen AND ($type IS NULL OR r.type = $type) "
                    "AND ($after IS NULL OR r.id > $after)" N4S_EDGE_RET
                    ", " N4S_FAR_COLS("a") " ORDER BY r.id LIMIT $lim"
                  : "MATCH (a:CbmNode {db: $db, gen: $gen, id: $id})-[r]->(b:CbmNode) "
                    "WHERE b.db = $db AND b.gen = $gen AND ($type IS NULL OR r.type = $type) "
                    "AND ($after IS NULL OR r.id > $after)" N4S_EDGE_RET
                    ", " N4S_FAR_COLS("b") " ORDER BY r.id LIMIT $lim";
    const int page = g_n4s_edge_page;
    cbm_edge_t *all_e = NULL;
    cbm_node_t *all_n = NULL;
    int all = 0;
    int cap = 0;
    bool first = true;
    int64_t after = 0;
    int rc = CBM_STORE_OK;
    for (;;) {
        n4s_q_t q;
        if (q_begin(x, &q, cypher) != 0) {
            rc = CBM_STORE_ERR;
            break;
        }
        q_int(&q, "id", node_id);
        q_str(&q, "type", type);
        if (first) {
            yyjson_mut_obj_add_null(q.b.doc, q.params, "after");
        } else {
            q_int(&q, "after", after);
        }
        q_int(&q, "lim", (int64_t)page + 1); /* one probe row: is there another page? */
        cbm_edge_t *pe = NULL;
        cbm_node_t *pn = NULL;
        int pc = 0;
        int pf = 0;
        rc = collect_edges_ex(x, &q, &pe, &pc, &pn, &pf);
        if (rc != CBM_STORE_OK) {
            break;
        }
        bool more = pc > page;
        int take = more ? page : pc;
        if (more) {
            after = pe[page - 1].id;
            for (int i = page; i < pc; i++) { /* the probe row is read again next page */
                edge_fields_free(&pe[i]);
                node_free(&pn[i]);
            }
        }
        rc = edges_append(&all_e, &all_n, &all, &cap, pe, pn, take);
        if (rc != CBM_STORE_OK) {
            for (int i = 0; i < take; i++) {
                edge_fields_free(&pe[i]);
                node_free(&pn[i]);
            }
        }
        free(pe);
        free(pn);
        if (rc != CBM_STORE_OK || !more) {
            break;
        }
        first = false;
        if (read_deadline_passed(x)) {
            rc = CBM_STORE_SCAN_LIMIT;
            break;
        }
    }
    if (rc != CBM_STORE_OK) {
        cbm_store_free_edges(all_e, all);
        cbm_store_free_nodes(all_n, all);
        if (rc == CBM_STORE_SCAN_LIMIT) {
            (void)snprintf(x->err, sizeof(x->err), "%s",
                           "edge read stopped at the query's time limit");
        }
        return rc;
    }
    node_cache_set(&x->last_nodes, all_n, all);
    *out = all_e;
    *count = all;
    return CBM_STORE_OK;
}

#define N4S_PREFETCH_EDGES_TARGET                                                               \
    "MATCH (b:CbmNode {db: $db, gen: $gen}) WHERE b.id IN $ids AND COUNT { (b)<--() } <= $hub " \
    "MATCH (a:CbmNode)-[r]->(b) "                                                               \
    "WHERE a.db = $db AND a.gen = $gen AND ($types IS NULL OR r.type IN $types)" N4S_EDGE_RET   \
    ", " N4S_FAR_COLS("a") " LIMIT $cap"
#define N4S_PREFETCH_EDGES_SOURCE                                                               \
    "MATCH (a:CbmNode {db: $db, gen: $gen}) WHERE a.id IN $ids AND COUNT { (a)-->() } <= $hub " \
    "MATCH (a)-[r]->(b:CbmNode) "                                                               \
    "WHERE b.db = $db AND b.gen = $gen AND ($types IS NULL OR r.type IN $types)" N4S_EDGE_RET   \
    ", " N4S_FAR_COLS("b") " LIMIT $cap"

/* Parameters of one prefetch edge statement. */
static void prefetch_edge_params(n4s_q_t *q, const int64_t *ids, int n, const char *const *types,
                                 int type_count) {
    q_ids(q, "ids", ids, n);
    q_int(q, "cap", (int64_t)g_n4s_prefetch_cap + 1);
    q_int(q, "hub", g_n4s_hub_degree);
    if (types && type_count > 0) {
        q_types(q, "types", (const char **)types, type_count);
    } else {
        yyjson_mut_obj_add_null(q->b.doc, q->params, "types");
    }
}

typedef struct {
    int64_t id;
    int64_t degree;
} n4s_degree_t;

static int cmp_degree_by_id(const void *a, const void *b) {
    int64_t x = ((const n4s_degree_t *)a)->id;
    int64_t y = ((const n4s_degree_t *)b)->id;
    return (x > y) - (x < y);
}

/* Fetch every edge of ids[0..n) in one direction (types NULL: all types)
 * plus their far-end nodes, replacing that direction's edge cache and the
 * chunk node cache. A hint only: a failure leaves the caches empty and reads
 * then go to Neo4j one by one, so it never changes an answer.
 *
 * One request carries two statements: every node's degree (Neo4j reads it
 * from its degree store) and the edges of the nodes that are not hubs. Hubs
 * (more than g_n4s_hub_degree edges) stay out of the cache, so their reads go
 * to op_find_edges, which pages them under the read deadline -- the previous
 * version gave up on the whole chunk (a false "time limit") after
 * downloading 50,001 rows. If the non-hub edges still exceed the cap, the
 * nodes are packed by degree into requests that each fit. */
static int op_prefetch_edges(void *ctx, const int64_t *ids, int n, bool by_target,
                             const char *const *types, int type_count) {
    n4s_ctx_t *x = ctx;
    n4s_edge_cache_t *c = &x->edge_cache[by_target ? 1 : 0];
    edge_cache_free(c);
    node_cache_free(&x->chunk_nodes[by_target ? 1 : 0]);
    if (n <= 0) {
        return CBM_STORE_OK;
    }
    int64_t *sorted = malloc((size_t)n * sizeof(int64_t));
    if (!sorted) {
        return CBM_STORE_ERR;
    }
    memcpy(sorted, ids, (size_t)n * sizeof(int64_t));
    qsort(sorted, (size_t)n, sizeof(int64_t), cmp_i64_asc);
    int u = 0;
    for (int i = 0; i < n; i++) {
        if (u == 0 || sorted[u - 1] != sorted[i]) {
            sorted[u++] = sorted[i];
        }
    }
    const char *edges_cypher = by_target ? N4S_PREFETCH_EDGES_TARGET : N4S_PREFETCH_EDGES_SOURCE;
    n4s_q_t q;
    if (q_begin(x, &q,
                by_target ? "MATCH (b:CbmNode {db: $db, gen: $gen}) WHERE b.id IN $ids "
                            "RETURN b.id, COUNT { (b)<--() }"
                          : "MATCH (a:CbmNode {db: $db, gen: $gen}) WHERE a.id IN $ids "
                            "RETURN a.id, COUNT { (a)-->() }") != 0) {
        free(sorted);
        return CBM_STORE_ERR;
    }
    q_ids(&q, "ids", sorted, u);
    q.params = cbm_neo4j_batch_add(&q.b, edges_cypher);
    yyjson_mut_obj_add_strcpy(q.b.doc, q.params, "db", x->db);
    yyjson_mut_obj_add_int(q.b.doc, q.params, "gen", x->gen);
    prefetch_edge_params(&q, sorted, u, types, type_count);
    cbm_neo4j_result_t res;
    if (q_run(x, &q, &res) != CBM_STORE_OK) {
        free(sorted);
        return CBM_STORE_ERR;
    }
    /* Degrees: hubs leave the cached id set (their reads are paged). */
    yyjson_val *deg_rows = cbm_neo4j_result_data(&res, 0);
    size_t nd = yyjson_arr_size(deg_rows);
    n4s_degree_t *deg = nd > 0 ? calloc(nd, sizeof(*deg)) : NULL;
    if (nd > 0 && !deg) {
        cbm_neo4j_result_free(&res);
        free(sorted);
        return CBM_STORE_ERR;
    }
    yyjson_arr_iter dit = yyjson_arr_iter_with(deg_rows);
    for (size_t i = 0; i < nd; i++) {
        yyjson_val *e = yyjson_arr_iter_next(&dit);
        deg[i].id = cbm_neo4j_val_int(cbm_neo4j_row_col(e, 0), 0);
        deg[i].degree = cbm_neo4j_val_int(cbm_neo4j_row_col(e, 1), 0);
    }
    if (nd > 1) {
        qsort(deg, nd, sizeof(*deg), cmp_degree_by_id);
    }
    int keep = 0; /* non-hub ids, still sorted; an id without a node has degree 0 */
    for (int i = 0; i < u; i++) {
        n4s_degree_t key = {sorted[i], 0};
        const n4s_degree_t *d =
            nd > 0 ? bsearch(&key, deg, nd, sizeof(*deg), cmp_degree_by_id) : NULL;
        if (d && d->degree > g_n4s_hub_degree) {
            continue;
        }
        sorted[keep++] = sorted[i];
    }
    u = keep;
    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    cbm_node_t *far = NULL;
    int far_count = 0;
    int erc =
        edges_from_data(cbm_neo4j_result_data(&res, 1), &edges, &edge_count, &far, &far_count);
    cbm_neo4j_result_free(&res);
    if (erc != CBM_STORE_OK) {
        free(deg);
        free(sorted);
        return CBM_STORE_ERR;
    }
    if (edge_count > g_n4s_prefetch_cap) {
        /* Too many for one response: pack the non-hub nodes by degree into
         * requests that each stay under the cap. */
        cbm_store_free_edges(edges, edge_count);
        cbm_store_free_nodes(far, far_count);
        edges = NULL;
        far = NULL;
        edge_count = 0;
        int cap_n = 0;
        int start = 0;
        int rc = CBM_STORE_OK;
        while (start < u && rc == CBM_STORE_OK) {
            int64_t sum = 0;
            int end = start;
            while (end < u) {
                n4s_degree_t key = {sorted[end], 0};
                const n4s_degree_t *d =
                    nd > 0 ? bsearch(&key, deg, nd, sizeof(*deg), cmp_degree_by_id) : NULL;
                int64_t dg = d ? d->degree : 0;
                if (end > start && sum + dg > g_n4s_prefetch_cap) {
                    break;
                }
                sum += dg;
                end++;
            }
            n4s_q_t sq;
            if (q_begin(x, &sq, edges_cypher) != 0) {
                rc = CBM_STORE_ERR;
                break;
            }
            prefetch_edge_params(&sq, sorted + start, end - start, types, type_count);
            cbm_edge_t *pe = NULL;
            cbm_node_t *pn = NULL;
            int pc = 0;
            int pf = 0;
            rc = collect_edges_ex(x, &sq, &pe, &pc, &pn, &pf);
            if (rc == CBM_STORE_OK && pc > g_n4s_prefetch_cap) {
                rc = CBM_STORE_ERR; /* degrees under-counted: give the hint up */
            }
            if (rc == CBM_STORE_OK) {
                rc = edges_append(&edges, &far, &edge_count, &cap_n, pe, pn, pc);
            }
            if (rc != CBM_STORE_OK) {
                cbm_store_free_edges(pe, pc);
                cbm_store_free_nodes(pn, pf);
            } else {
                free(pe);
                free(pn);
            }
            start = end;
        }
        far_count = edge_count;
        if (rc != CBM_STORE_OK) {
            cbm_store_free_edges(edges, edge_count);
            cbm_store_free_nodes(far, far_count);
            free(deg);
            free(sorted);
            return CBM_STORE_ERR;
        }
    }
    free(deg);
    if (edge_count > 1) {
        qsort(edges, (size_t)edge_count, sizeof(cbm_edge_t),
              by_target ? cmp_edge_by_target : cmp_edge_by_source);
    }
    if (types && type_count > 0) {
        c->types = calloc((size_t)type_count, sizeof(char *));
        if (!c->types) {
            cbm_store_free_edges(edges, edge_count);
            cbm_store_free_nodes(far, far_count);
            free(sorted);
            return CBM_STORE_ERR;
        }
        c->type_count = type_count;
        for (int i = 0; i < type_count; i++) {
            c->types[i] = n4s_dup(types[i]);
            if (!c->types[i]) {
                edge_cache_free(c);
                cbm_store_free_edges(edges, edge_count);
                cbm_store_free_nodes(far, far_count);
                free(sorted);
                return CBM_STORE_ERR;
            }
        }
    }
    c->ids = sorted;
    c->id_count = u;
    c->edges = edges;
    c->edge_count = edge_count;
    c->valid = true;
    node_cache_set(&x->chunk_nodes[by_target ? 1 : 0], far, far_count);
    return CBM_STORE_OK;
}

static void op_cache_clear(void *ctx) {
    caches_free((n4s_ctx_t *)ctx);
}

/* Append the node rows of r (columns from off) to *arr. */
static int append_node_rows(cbm_neo4j_result_t *r, size_t off, cbm_node_t **arr, int *n, int *cap) {
    yyjson_val *data = cbm_neo4j_result_data(r, 0);
    size_t rows = yyjson_arr_size(data);
    if (rows == 0) {
        return CBM_STORE_OK;
    }
    if ((size_t)*n + rows > (size_t)*cap) {
        size_t next = (size_t)*n + rows;
        if (next < (size_t)*cap * 2) {
            next = (size_t)*cap * 2;
        }
        if (next > INT_MAX) {
            return CBM_STORE_ERR;
        }
        cbm_node_t *grown = realloc(*arr, next * sizeof(cbm_node_t));
        if (!grown) {
            return CBM_STORE_ERR;
        }
        *arr = grown;
        *cap = (int)next;
    }
    size_t idx;
    size_t max;
    yyjson_val *e;
    yyjson_arr_foreach(data, idx, max, e) {
        node_from_row(e, off, &(*arr)[(*n)++]);
    }
    return CBM_STORE_OK;
}

/* Nodes for ids[0..n), sorted by id; ids with no node are skipped. */
static int op_find_nodes_by_ids(void *ctx, const int64_t *ids, int n, cbm_node_t **out,
                                int *count) {
    n4s_ctx_t *x = ctx;
    *out = NULL;
    *count = 0;
    cbm_node_t *arr = NULL;
    int got = 0;
    int cap = 0;
    for (int base = 0; base < n; base += N4S_ID_CHUNK) {
        int m = n - base < N4S_ID_CHUNK ? n - base : N4S_ID_CHUNK;
        n4s_q_t q;
        if (q_begin(x, &q,
                    "MATCH (n:CbmNode {db: $db, gen: $gen}) WHERE n.id IN $ids "
                    "RETURN " N4S_NODE_COLS("n")) != 0) {
            cbm_store_free_nodes(arr, got);
            return CBM_STORE_ERR;
        }
        q_ids(&q, "ids", ids + base, m);
        cbm_neo4j_result_t r;
        if (q_run(x, &q, &r) != CBM_STORE_OK) {
            cbm_store_free_nodes(arr, got);
            return CBM_STORE_ERR;
        }
        int rc = append_node_rows(&r, 0, &arr, &got, &cap);
        cbm_neo4j_result_free(&r);
        if (rc != CBM_STORE_OK) {
            cbm_store_free_nodes(arr, got);
            return CBM_STORE_ERR;
        }
    }
    if (got > 1) {
        qsort(arr, (size_t)got, sizeof(cbm_node_t), cbm_store_node_cmp_id);
    }
    *out = arr;
    *count = got;
    return CBM_STORE_OK;
}

#define N4S_FILE_CHUNK 2000

/* Nodes of files[0..n) (project-filtered like find_nodes BY_FILE), grouped by
 * file in input order and by id within a file. */
static int op_find_nodes_by_files(void *ctx, const char *project, const char *const *files, int n,
                                  cbm_node_t **out, int *count) {
    n4s_ctx_t *x = ctx;
    *out = NULL;
    *count = 0;
    cbm_node_t *arr = NULL;
    int got = 0;
    int cap = 0;
    for (int base = 0; base < n; base += N4S_FILE_CHUNK) {
        int m = n - base < N4S_FILE_CHUNK ? n - base : N4S_FILE_CHUNK;
        n4s_q_t q;
        if (q_begin(x, &q,
                    "UNWIND range(0, size($files) - 1) AS i "
                    "MATCH (n:CbmNode {db: $db, gen: $gen, file_path: $files[i]}) "
                    "WHERE $project IS NULL OR n.project = $project "
                    "RETURN i, " N4S_NODE_COLS("n") " ORDER BY i, n.id") != 0) {
            cbm_store_free_nodes(arr, got);
            return CBM_STORE_ERR;
        }
        yyjson_mut_val *list = yyjson_mut_arr(q.b.doc);
        for (int i = 0; i < m; i++) {
            yyjson_mut_arr_add_strcpy(q.b.doc, list, files[base + i] ? files[base + i] : "");
        }
        yyjson_mut_obj_add_val(q.b.doc, q.params, "files", list);
        q_str(&q, "project", project);
        cbm_neo4j_result_t r;
        if (q_run(x, &q, &r) != CBM_STORE_OK) {
            cbm_store_free_nodes(arr, got);
            return CBM_STORE_ERR;
        }
        int rc = append_node_rows(&r, 1, &arr, &got, &cap);
        cbm_neo4j_result_free(&r);
        if (rc != CBM_STORE_OK) {
            cbm_store_free_nodes(arr, got);
            return CBM_STORE_ERR;
        }
    }
    *out = arr;
    *count = got;
    return CBM_STORE_OK;
}

/* One page of a node scan in the order of find_nodes ALL (file_path, id) or
 * BY_LABEL (id), continuing after (after_file, after_id) when keyed. */
static int op_scan_nodes(void *ctx, const char *project, const char *label, bool keyed,
                         const char *after_file, int64_t after_id, int max, cbm_node_t **out,
                         int *count) {
    n4s_ctx_t *x = ctx;
    /* A full scan seeks (db, gen, file_path) and orders by the whole index
     * key (db and gen are fixed by the seek, so the order is (file_path,
     * id)): the planner then reads the index in order and sorts only rows
     * of equal file_path (PartialTop), so a page reads about its own rows.
     * ORDER BY n.file_path, n.id alone made it read and sort every remaining
     * row of the project on every page. A label scan seeks (db, gen, label)
     * and sorts that label's remaining rows by id: a rare label costs its
     * own size, not a walk of the whole graph in id order. */
#define N4S_SCAN_LABEL_WHERE                                                       \
    "WHERE n.db = $db AND n.gen = $gen AND n.id > $after_id AND n.label = $label " \
    "AND ($project IS NULL OR n.project = $project) "                              \
    "RETURN " N4S_NODE_COLS("n") " ORDER BY n.id LIMIT $max"
#define N4S_SCAN_ALL_WHERE                                                   \
    "WHERE n.db = $db AND n.gen = $gen AND n.file_path >= $after_file "      \
    "AND ($keyed = false OR n.file_path > $after_file OR n.id > $after_id) " \
    "AND ($project IS NULL OR n.project = $project) "                        \
    "RETURN " N4S_NODE_COLS("n") " ORDER BY n.db, n.gen, n.file_path, n.id LIMIT $max"
    /* With the hint, a page that the planner cannot serve from the index
     * (still populating, dropped by hand) is an error, not a slow scan:
     * retry such a page once without it. */
    static const char *const hinted[2] = {
        "MATCH (n:CbmNode) USING INDEX n:CbmNode(db, gen, file_path) " N4S_SCAN_ALL_WHERE,
        "MATCH (n:CbmNode) USING INDEX n:CbmNode(db, gen, label) " N4S_SCAN_LABEL_WHERE};
    static const char *const plain[2] = {"MATCH (n:CbmNode) " N4S_SCAN_ALL_WHERE,
                                         "MATCH (n:CbmNode) " N4S_SCAN_LABEL_WHERE};
    int rc = CBM_STORE_ERR;
    for (int attempt = 0; attempt < 2 && rc != CBM_STORE_OK; attempt++) {
        n4s_q_t q;
        if (q_begin(x, &q, (attempt == 0 ? hinted : plain)[label ? 1 : 0]) != 0) {
            return CBM_STORE_ERR;
        }
        q_str(&q, "project", project);
        q_str(&q, "label", label);
        yyjson_mut_obj_add_bool(q.b.doc, q.params, "keyed", keyed);
        q_str(&q, "after_file", after_file ? after_file : "");
        q_int(&q, "after_id", keyed ? after_id : INT64_MIN);
        q_int(&q, "max", max);
        rc = collect_nodes(x, &q, out, count);
    }
    return rc;
#undef N4S_SCAN_LABEL_WHERE
#undef N4S_SCAN_ALL_WHERE
}

static int op_find_edges_by_type(void *ctx, const char *project, const char *type, cbm_edge_t **out,
                                 int *count) {
    n4s_ctx_t *x = ctx;
    n4s_q_t q;
    if (q_begin(x, &q,
                "MATCH (a:CbmNode {db: $db, gen: $gen})-[r]->(b:CbmNode) "
                "WHERE r.type = $type AND ($project IS NULL OR a.project = $project)" N4S_EDGE_RET
                " ORDER BY r.id") != 0) {
        return CBM_STORE_ERR;
    }
    q_str(&q, "type", type ? type : "");
    q_str(&q, "project", project);
    return collect_edges(x, &q, out, count);
}

static int op_count(void *ctx, cbm_remote_count_t kind, const char *project, const char *arg) {
    n4s_ctx_t *x = ctx;
    const char *cypher = NULL;
    switch (kind) {
    case CBM_REMOTE_COUNT_NODES:
        cypher = "MATCH (n:CbmNode {db: $db, gen: $gen}) "
                 "WHERE $project IS NULL OR n.project = $project RETURN count(n)";
        break;
    case CBM_REMOTE_COUNT_EDGES:
        cypher = "MATCH (a:CbmNode {db: $db, gen: $gen})-[r]->() "
                 "WHERE $project IS NULL OR a.project = $project RETURN count(r)";
        break;
    case CBM_REMOTE_COUNT_NODES_BY_LABEL:
        /* seeks the (db, gen, label) index */
        cypher = "MATCH (n:CbmNode {db: $db, gen: $gen, label: $arg}) "
                 "WHERE $project IS NULL OR n.project = $project RETURN count(n)";
        break;
    case CBM_REMOTE_COUNT_EDGES_BY_TYPE:
        cypher = "MATCH (a:CbmNode {db: $db, gen: $gen})-[r]->() "
                 "WHERE r.type = $arg AND ($project IS NULL OR a.project = $project) "
                 "RETURN count(r)";
        break;
    default:
        return CBM_STORE_ERR;
    }
    n4s_q_t q;
    if (q_begin(x, &q, cypher) != 0) {
        return CBM_STORE_ERR;
    }
    q_str(&q, "project", project);
    q_str(&q, "arg", arg ? arg : "");
    cbm_neo4j_result_t r;
    if (q_run(x, &q, &r) != CBM_STORE_OK) {
        return CBM_STORE_ERR;
    }
    int64_t n = cbm_neo4j_val_int(
        cbm_neo4j_row_col(yyjson_arr_get_first(cbm_neo4j_result_data(&r, 0)), 0), 0);
    cbm_neo4j_result_free(&r);
    return n > INT_MAX ? INT_MAX : (int)n;
}

static int op_degrees(void *ctx, const int64_t *ids, int id_count, const char *type, int *in,
                      int *out) {
    n4s_ctx_t *x = ctx;
    const char *t = (type && type[0]) ? type : NULL;
    for (int base = 0; base < id_count; base += N4S_ID_CHUNK) {
        int n = id_count - base < N4S_ID_CHUNK ? id_count - base : N4S_ID_CHUNK;
        n4s_q_t q;
        if (q_begin(x, &q,
                    "UNWIND range(0, size($ids) - 1) AS i "
                    "MATCH (n:CbmNode {db: $db, gen: $gen, id: $ids[i]}) "
                    "RETURN i, size([(n)<-[r]-() WHERE $type IS NULL OR r.type = $type | 1]), "
                    "size([(n)-[r]->() WHERE $type IS NULL OR r.type = $type | 1])") != 0) {
            return CBM_STORE_ERR;
        }
        q_ids(&q, "ids", ids + base, n);
        q_str(&q, "type", t);
        cbm_neo4j_result_t r;
        if (q_run(x, &q, &r) != CBM_STORE_OK) {
            return CBM_STORE_ERR;
        }
        yyjson_val *data = cbm_neo4j_result_data(&r, 0);
        size_t idx;
        size_t max;
        yyjson_val *e;
        yyjson_arr_foreach(data, idx, max, e) {
            int64_t i = cbm_neo4j_val_int(cbm_neo4j_row_col(e, 0), -1);
            if (i >= 0 && i < n) {
                in[base + i] = (int)cbm_neo4j_val_int(cbm_neo4j_row_col(e, 1), 0);
                out[base + i] = (int)cbm_neo4j_val_int(cbm_neo4j_row_col(e, 2), 0);
            }
        }
        cbm_neo4j_result_free(&r);
    }
    return CBM_STORE_OK;
}

/* ── Traversal ──────────────────────────────────────────────────── */

typedef struct {
    int64_t *v;
    int n;
    int cap;
} n4s_ids_t;

static int ids_push(n4s_ids_t *a, int64_t id) {
    if (a->n >= a->cap) {
        int cap = a->cap ? a->cap * 2 : 64;
        int64_t *v = realloc(a->v, (size_t)cap * sizeof(int64_t));
        if (!v) {
            return -1;
        }
        a->v = v;
        a->cap = cap;
    }
    a->v[a->n++] = id;
    return 0;
}

static int cmp_i64(const void *a, const void *b) {
    int64_t x = *(const int64_t *)a;
    int64_t y = *(const int64_t *)b;
    return (x > y) - (x < y);
}

/* Open-addressing set of node ids. */
typedef struct {
    int64_t *slots;
    unsigned char *used;
    size_t cap;
    size_t n;
} n4s_set_t;

static size_t set_hash(int64_t id, size_t cap) {
    uint64_t h = (uint64_t)id * 0x9E3779B97F4A7C15ULL;
    return (size_t)(h >> 17) & (cap - 1);
}

static int set_add(n4s_set_t *s, int64_t id); /* fwd */

static int set_grow(n4s_set_t *s) {
    size_t cap = s->cap ? s->cap * 2 : 256;
    n4s_set_t ns = {calloc(cap, sizeof(int64_t)), calloc(cap, 1), cap, 0};
    if (!ns.slots || !ns.used) {
        free(ns.slots);
        free(ns.used);
        return -1;
    }
    for (size_t i = 0; i < s->cap; i++) {
        if (s->used[i]) {
            (void)set_add(&ns, s->slots[i]);
        }
    }
    free(s->slots);
    free(s->used);
    *s = ns;
    return 0;
}

static bool set_has(const n4s_set_t *s, int64_t id) {
    if (!s->cap) {
        return false;
    }
    for (size_t i = set_hash(id, s->cap);; i = (i + 1) & (s->cap - 1)) {
        if (!s->used[i]) {
            return false;
        }
        if (s->slots[i] == id) {
            return true;
        }
    }
}

/* 1 when added, 0 when already present, -1 on OOM. */
static int set_add(n4s_set_t *s, int64_t id) {
    if ((s->n + 1) * 2 > s->cap && set_grow(s) != 0) {
        return -1;
    }
    for (size_t i = set_hash(id, s->cap);; i = (i + 1) & (s->cap - 1)) {
        if (!s->used[i]) {
            s->used[i] = 1;
            s->slots[i] = id;
            s->n++;
            return 1;
        }
        if (s->slots[i] == id) {
            return 0;
        }
    }
}

static void set_free(n4s_set_t *s) {
    free(s->slots);
    free(s->used);
    memset(s, 0, sizeof(*s));
}

/* Next BFS layer: distinct neighbour ids of `frontier` over the edge types. */
static int bfs_expand(n4s_ctx_t *x, const n4s_ids_t *frontier, bool inbound, const char **types,
                      int type_count, n4s_ids_t *next) {
    const char *cypher =
        inbound ? "UNWIND $ids AS fid MATCH (a:CbmNode {db: $db, gen: $gen, id: fid})<-[r]-(b) "
                  "WHERE r.type IN $types AND b.db = $db AND b.gen = $gen RETURN DISTINCT b.id"
                : "UNWIND $ids AS fid MATCH (a:CbmNode {db: $db, gen: $gen, id: fid})-[r]->(b) "
                  "WHERE r.type IN $types AND b.db = $db AND b.gen = $gen RETURN DISTINCT b.id";
    for (int base = 0; base < frontier->n; base += N4S_ID_CHUNK) {
        int n = frontier->n - base < N4S_ID_CHUNK ? frontier->n - base : N4S_ID_CHUNK;
        n4s_q_t q;
        if (q_begin(x, &q, cypher) != 0) {
            return CBM_STORE_ERR;
        }
        q_ids(&q, "ids", frontier->v + base, n);
        q_types(&q, "types", types, type_count);
        cbm_neo4j_result_t r;
        if (q_run(x, &q, &r) != CBM_STORE_OK) {
            return CBM_STORE_ERR;
        }
        size_t idx;
        size_t max;
        yyjson_val *e;
        yyjson_arr_foreach(cbm_neo4j_result_data(&r, 0), idx, max, e) {
            if (ids_push(next, cbm_neo4j_val_int(cbm_neo4j_row_col(e, 0), 0)) != 0) {
                cbm_neo4j_result_free(&r);
                return CBM_STORE_ERR;
            }
        }
        cbm_neo4j_result_free(&r);
    }
    return CBM_STORE_OK;
}

/* Fetch node rows for ids and place them in visited[] in the given order. */
static int fill_hops(n4s_ctx_t *x, const int64_t *ids, const int *hops, int n, cbm_node_hop_t **out,
                     int *out_count) {
    *out = NULL;
    *out_count = 0;
    cbm_node_hop_t *visited = calloc(n > 0 ? (size_t)n : 1, sizeof(cbm_node_hop_t));
    if (!visited) {
        return CBM_STORE_ERR;
    }
    int written = 0;
    for (int base = 0; base < n; base += N4S_ID_CHUNK) {
        int m = n - base < N4S_ID_CHUNK ? n - base : N4S_ID_CHUNK;
        n4s_q_t q;
        if (q_begin(x, &q,
                    "UNWIND range(0, size($ids) - 1) AS i "
                    "MATCH (n:CbmNode {db: $db, gen: $gen, id: $ids[i]}) "
                    "RETURN i, " N4S_NODE_COLS("n") " ORDER BY i") != 0) {
            free(visited);
            return CBM_STORE_ERR;
        }
        q_ids(&q, "ids", ids + base, m);
        cbm_neo4j_result_t r;
        if (q_run(x, &q, &r) != CBM_STORE_OK) {
            for (int k = 0; k < written; k++) {
                node_free(&visited[k].node);
            }
            free(visited);
            return CBM_STORE_ERR;
        }
        size_t idx;
        size_t max;
        yyjson_val *e;
        yyjson_arr_foreach(cbm_neo4j_result_data(&r, 0), idx, max, e) {
            int64_t i = cbm_neo4j_val_int(cbm_neo4j_row_col(e, 0), -1);
            if (i < 0 || i >= m) {
                continue;
            }
            /* An id with no node row is skipped, as the SQL JOIN skips it. */
            node_from_row(e, 1, &visited[written].node);
            visited[written].hop = hops[base + i];
            written++;
        }
        cbm_neo4j_result_free(&r);
    }
    *out = visited;
    *out_count = written;
    return CBM_STORE_OK;
}

static int collect_bfs_edges(n4s_ctx_t *x, int64_t root_id, const cbm_node_hop_t *visited, int n,
                             bool inbound, const char **types, int type_count, int max_edges,
                             cbm_traverse_result_t *out) {
    (void)inbound;
    n4s_ids_t ids = {0};
    if (ids_push(&ids, root_id) != 0) {
        return CBM_STORE_ERR;
    }
    for (int i = 0; i < n; i++) {
        if (ids_push(&ids, visited[i].node.id) != 0) {
            free(ids.v);
            return CBM_STORE_ERR;
        }
    }
    char cypher[1024];
    char limit[48] = "";
    if (max_edges > 0) {
        (void)snprintf(limit, sizeof(limit), " LIMIT %d",
                       max_edges < INT_MAX ? max_edges + 1 : max_edges);
    }
    (void)snprintf(cypher, sizeof(cypher),
                   "MATCH (a:CbmNode {db: $db, gen: $gen})-[r]->(b:CbmNode {db: $db, gen: $gen}) "
                   "WHERE a.id IN $ids AND b.id IN $ids AND r.type IN $types "
                   "RETURN a.name, b.name, r.type, a.id, b.id, r.properties_json "
                   "ORDER BY a.id, b.id, r.type%s",
                   limit);
    n4s_q_t q;
    if (q_begin(x, &q, cypher) != 0) {
        free(ids.v);
        return CBM_STORE_ERR;
    }
    q_ids(&q, "ids", ids.v, ids.n);
    free(ids.v);
    q_types(&q, "types", types, type_count);
    cbm_neo4j_result_t r;
    if (q_run(x, &q, &r) != CBM_STORE_OK) {
        return CBM_STORE_ERR;
    }
    yyjson_val *data = cbm_neo4j_result_data(&r, 0);
    int en = (int)yyjson_arr_size(data);
    if (max_edges > 0 && en > max_edges) {
        en = max_edges;
        out->edges_truncated = true;
    }
    if (en > 0) {
        out->edges = calloc((size_t)en, sizeof(cbm_edge_info_t));
        if (!out->edges) {
            cbm_neo4j_result_free(&r);
            return CBM_STORE_ERR;
        }
        yyjson_arr_iter it = yyjson_arr_iter_with(data);
        for (int i = 0; i < en; i++) {
            yyjson_val *e = yyjson_arr_iter_next(&it);
            cbm_edge_info_t *ei = &out->edges[i];
            ei->from_name = cbm_neo4j_val_strdup(cbm_neo4j_row_col(e, 0), "");
            ei->to_name = cbm_neo4j_val_strdup(cbm_neo4j_row_col(e, 1), "");
            ei->type = cbm_neo4j_val_strdup(cbm_neo4j_row_col(e, 2), "");
            ei->confidence = 1.0;
            ei->source_id = cbm_neo4j_val_int(cbm_neo4j_row_col(e, 3), 0);
            ei->target_id = cbm_neo4j_val_int(cbm_neo4j_row_col(e, 4), 0);
            ei->properties_json = cbm_neo4j_val_strdup(cbm_neo4j_row_col(e, 5), "{}");
        }
        out->edge_count = en;
    }
    cbm_neo4j_result_free(&r);
    return CBM_STORE_OK;
}

/* Relationship-unique (trail) expansion: Neo4j's own variable-length match
 * semantics, distinct (node, hop) pairs in (hop, id) order. */
static int bfs_trail_ids(n4s_ctx_t *x, int64_t start, bool inbound, const char **types,
                         int type_count, int max_depth, int probe, n4s_ids_t *ids,
                         n4s_ids_t *hops) {
    char cypher[1024];
    (void)snprintf(cypher, sizeof(cypher),
                   "MATCH p = (s:CbmNode {db: $db, gen: $gen, id: $id})%s[rs*1..%d]%s(n:CbmNode) "
                   "WHERE all(x IN rs WHERE x.type IN $types) "
                   "RETURN DISTINCT n.id AS nid, length(p) AS hop ORDER BY hop, nid LIMIT %d",
                   inbound ? "<-" : "-", max_depth, inbound ? "-" : "->", probe);
    n4s_q_t q;
    if (q_begin(x, &q, cypher) != 0) {
        return CBM_STORE_ERR;
    }
    q_int(&q, "id", start);
    q_types(&q, "types", types, type_count);
    cbm_neo4j_result_t r;
    if (q_run(x, &q, &r) != CBM_STORE_OK) {
        return CBM_STORE_ERR;
    }
    size_t idx;
    size_t max;
    yyjson_val *e;
    yyjson_arr_foreach(cbm_neo4j_result_data(&r, 0), idx, max, e) {
        if (ids_push(ids, cbm_neo4j_val_int(cbm_neo4j_row_col(e, 0), 0)) != 0 ||
            ids_push(hops, cbm_neo4j_val_int(cbm_neo4j_row_col(e, 1), 0)) != 0) {
            cbm_neo4j_result_free(&r);
            return CBM_STORE_ERR;
        }
    }
    cbm_neo4j_result_free(&r);
    return CBM_STORE_OK;
}

/* Shortest-path layers, client side: each node once at its minimal hop,
 * layers in ascending id order, stopping once probe rows are known. Seeds
 * are excluded in multi mode; a single root reached again by a cycle is
 * reported at that hop, exactly like the SQL CTE (which filters hop > 0
 * before taking MIN(hop)). */
static int bfs_shortest_ids(n4s_ctx_t *x, const int64_t *seeds, int seed_count, bool multi,
                            bool inbound, const char **types, int type_count, int max_depth,
                            int probe, n4s_ids_t *ids, n4s_ids_t *hops) {
    n4s_set_t reached = {0};
    n4s_set_t seedset = {0};
    n4s_ids_t frontier = {0};
    int rc = CBM_STORE_OK;
    for (int i = 0; i < seed_count; i++) {
        if (set_add(&seedset, seeds[i]) == 1 && ids_push(&frontier, seeds[i]) != 0) {
            rc = CBM_STORE_ERR;
            goto done;
        }
    }
    for (int hop = 1; hop <= max_depth && frontier.n > 0 && ids->n < probe; hop++) {
        n4s_ids_t next = {0};
        rc = bfs_expand(x, &frontier, inbound, types, type_count, &next);
        if (rc != CBM_STORE_OK) {
            free(next.v);
            goto done;
        }
        qsort(next.v, (size_t)next.n, sizeof(int64_t), cmp_i64);
        frontier.n = 0;
        for (int i = 0; i < next.n; i++) {
            int64_t id = next.v[i];
            if (i > 0 && next.v[i - 1] == id) {
                continue;
            }
            bool is_seed = set_has(&seedset, id);
            if (multi && is_seed) {
                continue;
            }
            int added = set_add(&reached, id);
            if (added < 0) {
                rc = CBM_STORE_ERR;
                break;
            }
            if (added == 0) {
                continue;
            }
            if (ids->n < probe && (ids_push(ids, id) != 0 || ids_push(hops, hop) != 0)) {
                rc = CBM_STORE_ERR;
                break;
            }
            /* A seed re-reached is already expanded from hop 0. */
            if (!is_seed && ids_push(&frontier, id) != 0) {
                rc = CBM_STORE_ERR;
                break;
            }
        }
        free(next.v);
        if (rc != CBM_STORE_OK) {
            goto done;
        }
    }
done:
    free(frontier.v);
    set_free(&reached);
    set_free(&seedset);
    return rc;
}

static int op_bfs(void *ctx, const int64_t *seeds, int seed_count, bool multi,
                  const char *direction, const char **edge_types, int edge_type_count,
                  int max_depth, int max_results, bool trail, int max_edges,
                  cbm_traverse_result_t *out, bool *truncated) {
    n4s_ctx_t *x = ctx;
    memset(out, 0, sizeof(*out));
    if (truncated) {
        *truncated = false;
    }
    if (max_results < 0) {
        max_results = 0;
    }
    bool inbound = direction && strcmp(direction, "inbound") == 0;
    if (!multi) {
        int rc = op_find_node_by_id(x, seeds[0], &out->root);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }
    int probe = max_results < INT_MAX ? max_results + 1 : max_results;
    n4s_ids_t ids = {0};
    n4s_ids_t hops = {0};
    int rc = CBM_STORE_OK;
    if (max_depth > 0 && probe > 0) {
        rc = (trail && !multi) ? bfs_trail_ids(x, seeds[0], inbound, edge_types, edge_type_count,
                                               max_depth, probe, &ids, &hops)
                               : bfs_shortest_ids(x, seeds, seed_count, multi, inbound, edge_types,
                                                  edge_type_count, max_depth, probe, &ids, &hops);
    }
    if (rc == CBM_STORE_OK && ids.n > 0) {
        int *hop_ints = malloc((size_t)ids.n * sizeof(int));
        if (!hop_ints) {
            rc = CBM_STORE_ERR;
        } else {
            for (int i = 0; i < ids.n; i++) {
                hop_ints[i] = (int)hops.v[i];
            }
            rc = fill_hops(x, ids.v, hop_ints, ids.n, &out->visited, &out->visited_count);
            free(hop_ints);
        }
    }
    free(ids.v);
    free(hops.v);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    if (out->visited_count > max_results) {
        /* The probe row fired: drop it and report the ceiling. */
        node_free(&out->visited[max_results].node);
        out->visited_count = max_results;
        out->truncated = true;
        if (truncated) {
            *truncated = true;
        }
    }
    if (!multi && out->visited_count > 0 && max_edges != 0) {
        rc = collect_bfs_edges(x, seeds[0], out->visited, out->visited_count, inbound, edge_types,
                               edge_type_count, max_edges, out);
    }
    return rc;
}

/* ── Search, names, files, schema ───────────────────────────────── */

static void q_str_list(n4s_q_t *q, const char *key, const char *const *items) {
    yyjson_mut_val *arr = yyjson_mut_arr(q->b.doc);
    for (int i = 0; items && items[i]; i++) {
        yyjson_mut_arr_add_strcpy(q->b.doc, arr, items[i]);
    }
    yyjson_mut_obj_add_val(q->b.doc, q->params, key, arr);
}

static void q_opt_str(n4s_q_t *q, const char *key, const char *val) {
    q_str(q, key, (val && val[0]) ? val : NULL);
}

/* The edge types search_graph's in/out degree counts (store.c search). */
static const char *const k_search_degree_types[] = {"CALLS",    "USAGE",      "CALL_REFERENCE",
                                                    "INHERITS", "IMPLEMENTS", NULL};

/* Everything after MATCH (n): the filters of cbm_store_search's WHERE, then
 * the degree columns its degree filter and rows need. */
static const char k_search_where[] =
    "MATCH (n:CbmNode {db: $db, gen: $gen}) "
    "WHERE ($project IS NULL OR n.project = $project) "
    "AND ($label IS NULL OR n.label = $label) "
    "AND NOT n.label IN $exclude_labels "
    "AND all(h IN $name_hints WHERE toLower(n.name) CONTAINS toLower(h)) "
    "AND all(h IN $qn_hints WHERE toLower(n.qualified_name) CONTAINS toLower(h)) "
    "AND all(h IN $file_hints WHERE toLower(coalesce(n.file_path, '')) CONTAINS toLower(h)) "
    "AND ($kind IS NULL OR n.kind = $kind) "
    "AND ($ns IS NULL OR (size(split(coalesce(n.obj_key, ''), '/')) >= 4 "
    "     AND toLower(split(n.obj_key, '/')[2]) = toLower($ns))) "
    "AND ($rel IS NULL OR size([(n)-[r]-() WHERE r.type = $rel | 1]) > 0) "
    "AND (NOT $exclude_entry_points OR NOT ("
    "     size([(n)<-[c]-() WHERE c.type = 'CALLS' | 1]) = 0 AND "
    "     size([(n)-[c]->() WHERE c.type = 'CALLS' | 1]) > 0)) "
    "WITH n, size([(n)<-[r]-() WHERE r.type IN $degree_types | 1]) AS in_deg, "
    "     size([(n)-[r]->() WHERE r.type IN $degree_types | 1]) AS out_deg "
    "WHERE ($min_degree < 0 OR in_deg + out_deg >= $min_degree) "
    "AND ($max_degree < 0 OR in_deg + out_deg <= $max_degree) ";

static void q_search_params(n4s_q_t *q, const cbm_remote_search_t *s) {
    q_opt_str(q, "project", s->project);
    q_opt_str(q, "label", s->label);
    q_str_list(q, "exclude_labels", s->exclude_labels);
    q_str_list(q, "name_hints", s->name_contains_ci);
    q_str_list(q, "qn_hints", s->qn_contains_ci);
    q_str_list(q, "file_hints", s->file_contains_ci);
    q_opt_str(q, "kind", s->kind);
    q_opt_str(q, "ns", s->namespace_name);
    q_opt_str(q, "rel", s->relationship);
    yyjson_mut_obj_add_bool(q->b.doc, q->params, "exclude_entry_points", s->exclude_entry_points);
    q_str_list(q, "degree_types", k_search_degree_types);
    q_int(q, "min_degree", s->min_degree);
    q_int(q, "max_degree", s->max_degree);
}

static int op_search(void *ctx, const cbm_remote_search_t *s, cbm_search_result_t **out, int *count,
                     int *total) {
    n4s_ctx_t *x = ctx;
    *out = NULL;
    *count = 0;
    *total = 0;
    bool paged = s->limit >= 0;
    char rows_cypher[sizeof(k_search_where) + 256];
    (void)snprintf(rows_cypher, sizeof(rows_cypher),
                   "%sRETURN " N4S_NODE_COLS("n") ", in_deg, out_deg ORDER BY n.name, n.id%s",
                   k_search_where, paged ? " SKIP $offset LIMIT $limit" : "");
    cbm_neo4j_batch_t b;
    if (cbm_neo4j_batch_init(&b) != 0) {
        return CBM_STORE_ERR;
    }
    n4s_q_t q = {.b = b};
    q.params = cbm_neo4j_batch_add(&q.b, rows_cypher);
    yyjson_mut_obj_add_strcpy(q.b.doc, q.params, "db", x->db);
    yyjson_mut_obj_add_int(q.b.doc, q.params, "gen", x->gen);
    q_search_params(&q, s);
    if (paged) {
        q_int(&q, "offset", s->offset);
        q_int(&q, "limit", s->limit);
        /* Same filters, counted, in the same transaction. */
        char count_cypher[sizeof(k_search_where) + 32];
        (void)snprintf(count_cypher, sizeof(count_cypher), "%sRETURN count(n)", k_search_where);
        yyjson_mut_val *saved = q.params;
        q.params = cbm_neo4j_batch_add(&q.b, count_cypher);
        yyjson_mut_obj_add_strcpy(q.b.doc, q.params, "db", x->db);
        yyjson_mut_obj_add_int(q.b.doc, q.params, "gen", x->gen);
        q_search_params(&q, s);
        q.params = saved;
    }
    cbm_neo4j_result_t r;
    if (q_run(x, &q, &r) != CBM_STORE_OK) {
        return CBM_STORE_ERR;
    }
    yyjson_val *data = cbm_neo4j_result_data(&r, 0);
    size_t n = yyjson_arr_size(data);
    cbm_search_result_t *rows = n ? calloc(n, sizeof(cbm_search_result_t)) : NULL;
    if (n && !rows) {
        cbm_neo4j_result_free(&r);
        return CBM_STORE_ERR;
    }
    yyjson_arr_iter it = yyjson_arr_iter_with(data);
    for (size_t i = 0; i < n; i++) {
        yyjson_val *e = yyjson_arr_iter_next(&it);
        node_from_row(e, 0, &rows[i].node);
        rows[i].in_degree = (int)cbm_neo4j_val_int(cbm_neo4j_row_col(e, N4S_NODE_NCOLS), 0);
        rows[i].out_degree = (int)cbm_neo4j_val_int(cbm_neo4j_row_col(e, N4S_NODE_NCOLS + 1), 0);
    }
    *out = rows;
    *count = (int)n;
    *total = paged
                 ? (int)cbm_neo4j_val_int(
                       cbm_neo4j_row_col(yyjson_arr_get_first(cbm_neo4j_result_data(&r, 1)), 0), 0)
                 : (int)n;
    cbm_neo4j_result_free(&r);
    return CBM_STORE_OK;
}

/* Column 0 of statement `stmt`, as a malloc'd string array.
 *
 * Every loop over result rows walks them with an iterator: rows are objects,
 * and yyjson_arr_get(data, i) on an array of objects steps through all i
 * earlier rows, so an indexed loop is quadratic: reading 272,571 rows (the
 * File list of a 4.5M-node graph) took 156 s in a benchmark, 3 ms with the
 * iterator, and search_code's scope step took 86 s on that graph. */
static int collect_strings(cbm_neo4j_result_t *r, size_t stmt, char ***out, int *count) {
    yyjson_val *data = cbm_neo4j_result_data(r, stmt);
    size_t n = yyjson_arr_size(data);
    *out = NULL;
    *count = 0;
    if (n == 0) {
        return CBM_STORE_OK;
    }
    char **arr = calloc(n, sizeof(char *));
    if (!arr) {
        return CBM_STORE_ERR;
    }
    yyjson_arr_iter it = yyjson_arr_iter_with(data);
    for (size_t i = 0; i < n; i++) {
        arr[i] = cbm_neo4j_val_strdup(cbm_neo4j_row_col(yyjson_arr_iter_next(&it), 0), "");
    }
    *out = arr;
    *count = (int)n;
    return CBM_STORE_OK;
}

static int op_neighbor_names(void *ctx, int64_t node_id, int limit, char ***callers,
                             int *caller_count, char ***callees, int *callee_count) {
    n4s_ctx_t *x = ctx;
    static const char *const directions[] = {
        "MATCH (t:CbmNode {db: $db, gen: $gen, id: $id})<-[r]-(n:CbmNode) "
        "WHERE r.type IN ['CALLS', 'HTTP_CALLS', 'ASYNC_CALLS'] "
        "RETURN DISTINCT n.name AS name ORDER BY name LIMIT $limit",
        "MATCH (t:CbmNode {db: $db, gen: $gen, id: $id})-[r]->(n:CbmNode) "
        "WHERE r.type IN ['CALLS', 'HTTP_CALLS', 'ASYNC_CALLS'] "
        "RETURN DISTINCT n.name AS name ORDER BY name LIMIT $limit"};
    cbm_neo4j_batch_t b;
    if (cbm_neo4j_batch_init(&b) != 0) {
        return CBM_STORE_ERR;
    }
    for (int i = 0; i < 2; i++) {
        yyjson_mut_val *p = cbm_neo4j_batch_add(&b, directions[i]);
        yyjson_mut_obj_add_strcpy(b.doc, p, "db", x->db);
        yyjson_mut_obj_add_int(b.doc, p, "gen", x->gen);
        yyjson_mut_obj_add_int(b.doc, p, "id", node_id);
        yyjson_mut_obj_add_int(b.doc, p, "limit", limit > 0 ? limit : 0);
    }
    n4s_q_t q = {.b = b};
    cbm_neo4j_result_t r;
    if (q_run(x, &q, &r) != CBM_STORE_OK) {
        return CBM_STORE_ERR;
    }
    int rc = collect_strings(&r, 0, callers, caller_count);
    if (rc == CBM_STORE_OK) {
        rc = collect_strings(&r, 1, callees, callee_count);
    }
    cbm_neo4j_result_free(&r);
    return rc;
}

/* The canonical File paths, through the (db, gen, label) index. The path test
 * stays after the WITH: in the MATCH, "file_path IS NOT NULL" leads the
 * planner to the (db, gen, file_path) index, which holds every node of the
 * project, and the label filter then reads them all. (path <> '' is also
 * false for a null path.) */
static const char k_list_file_nodes[] = "MATCH (n:CbmNode {db: $db, gen: $gen, label: 'File'}) "
                                        "WHERE n.project = $project "
                                        "WITH n.file_path AS path WHERE path <> '' "
                                        "RETURN DISTINCT path ORDER BY path";

#ifdef CBM_ENABLE_TEST_SEAMS
const char *cbm_neo4j_test_list_file_nodes_cypher(void) {
    return k_list_file_nodes;
}
#endif

/* The full cbm_store_list_files rule, for a graph without File paths. Its OR
 * keeps Neo4j off the label index, so it reads every node of the project. */
static const char k_list_node_paths[] =
    "OPTIONAL MATCH (f:CbmNode {db: $db, gen: $gen, label: 'File'}) "
    "WHERE f.project = $project "
    "WITH count(f) > 0 AS has_files "
    "MATCH (n:CbmNode {db: $db, gen: $gen}) "
    "WHERE n.project = $project AND n.label <> 'Folder' "
    "AND n.file_path IS NOT NULL AND n.file_path <> '' "
    "AND (n.label = 'File' OR NOT has_files) "
    "RETURN DISTINCT n.file_path AS path ORDER BY path";

static int list_files_query(n4s_ctx_t *x, const char *cypher, const char *project, char ***out,
                            int *count) {
    n4s_q_t q;
    if (q_begin(x, &q, cypher) != 0) {
        return CBM_STORE_ERR;
    }
    q_str(&q, "project", project);
    cbm_neo4j_result_t r;
    if (q_run(x, &q, &r) != CBM_STORE_OK) {
        return CBM_STORE_ERR;
    }
    int rc = collect_strings(&r, 0, out, count);
    cbm_neo4j_result_free(&r);
    return rc;
}

static int op_list_files(void *ctx, const char *project, char ***out, int *count) {
    n4s_ctx_t *x = ctx;
    /* cbm_store_list_files: canonical File nodes, or -- for a graph without
     * any -- every non-Folder node path. Asking for File nodes first reads only
     * those (reading every node of a 4.5M-node graph took over two minutes,
     * and search_code waits on this list); the full rule runs only when that
     * finds nothing, and then answers exactly as before. */
    int rc = list_files_query(x, k_list_file_nodes, project, out, count);
    if (rc != CBM_STORE_OK || *count > 0) {
        return rc;
    }
    free(*out);
    *out = NULL;
    return list_files_query(x, k_list_node_paths, project, out, count);
}

/* Property keys the sync stores itself; never reported as graph properties. */
static const char *const k_internal_node_keys[] = {
    "db",         "gen",      "project",         "id",
    "label",      "name",     "qualified_name",  "file_path",
    "start_line", "end_line", "properties_json", "prop_keys",
    NULL};
static const char *const k_internal_edge_keys[] = {"id", "type", "properties_json", "prop_keys",
                                                   NULL};

/* base columns + up to 50 JSON keys, exactly like schema_discover_props. */
enum { N4S_SCHEMA_MAX_KEYS = 50 };

static char **schema_props(const char *const *base, int base_count, yyjson_val *keys, int *count) {
    size_t nkeys = yyjson_arr_size(keys);
    if (nkeys > N4S_SCHEMA_MAX_KEYS) {
        nkeys = N4S_SCHEMA_MAX_KEYS;
    }
    char **props = calloc((size_t)base_count + nkeys + 1, sizeof(char *));
    int n = 0;
    if (!props) {
        *count = 0;
        return NULL;
    }
    for (int i = 0; i < base_count; i++) {
        props[n++] = cbm_neo4j_val_strdup(NULL, base[i]);
    }
    for (size_t i = 0; i < nkeys; i++) {
        props[n++] = cbm_neo4j_val_strdup(yyjson_arr_get(keys, i), "");
    }
    *count = n;
    return props;
}

static int op_schema(void *ctx, const char *project, bool with_props, cbm_schema_info_t *out) {
    n4s_ctx_t *x = ctx;
    static const char *const node_base[] = {"name", "qualified_name", "file_path", "start_line",
                                            "end_line"};
    static const char *const edge_base[] = {"source_id", "target_id"};
    /* prop_keys (every top-level properties key, stored by the sync) is the
     * exact json_each key set; graphs synced before it existed fall back to
     * the flattened keys. */
    static const char *const statements[] = {
        "MATCH (n:CbmNode {db: $db, gen: $gen}) WHERE n.project = $project "
        "RETURN n.label AS l, count(*) AS c ORDER BY c DESC, l",
        "MATCH (a:CbmNode {db: $db, gen: $gen})-[r]->() WHERE a.project = $project "
        "RETURN r.type AS t, count(*) AS c ORDER BY c DESC, t",
        "MATCH (n:CbmNode {db: $db, gen: $gen}) WHERE n.project = $project "
        "UNWIND coalesce(n.prop_keys, [k IN keys(n) WHERE NOT k IN $node_internal]) AS k "
        "WITH n.label AS l, k ORDER BY l, k "
        "RETURN l, collect(DISTINCT k)",
        "MATCH (a:CbmNode {db: $db, gen: $gen})-[r]->() WHERE a.project = $project "
        "UNWIND coalesce(r.prop_keys, [k IN keys(r) WHERE NOT k IN $edge_internal]) AS k "
        "WITH r.type AS t, k ORDER BY t, k "
        "RETURN t, collect(DISTINCT k)"};
    int nstmt = with_props ? 4 : 2;
    cbm_neo4j_batch_t b;
    if (cbm_neo4j_batch_init(&b) != 0) {
        return CBM_STORE_ERR;
    }
    for (int i = 0; i < nstmt; i++) {
        yyjson_mut_val *p = cbm_neo4j_batch_add(&b, statements[i]);
        yyjson_mut_obj_add_strcpy(b.doc, p, "db", x->db);
        yyjson_mut_obj_add_int(b.doc, p, "gen", x->gen);
        yyjson_mut_obj_add_strcpy(b.doc, p, "project", project ? project : "");
        if (i == 2) {
            yyjson_mut_val *arr = yyjson_mut_arr(b.doc);
            for (int k = 0; k_internal_node_keys[k]; k++) {
                yyjson_mut_arr_add_str(b.doc, arr, k_internal_node_keys[k]);
            }
            yyjson_mut_obj_add_val(b.doc, p, "node_internal", arr);
        } else if (i == 3) {
            yyjson_mut_val *arr = yyjson_mut_arr(b.doc);
            for (int k = 0; k_internal_edge_keys[k]; k++) {
                yyjson_mut_arr_add_str(b.doc, arr, k_internal_edge_keys[k]);
            }
            yyjson_mut_obj_add_val(b.doc, p, "edge_internal", arr);
        }
    }
    n4s_q_t q = {.b = b};
    cbm_neo4j_result_t r;
    if (q_run(x, &q, &r) != CBM_STORE_OK) {
        return CBM_STORE_ERR;
    }
    yyjson_val *labels = cbm_neo4j_result_data(&r, 0);
    yyjson_val *types = cbm_neo4j_result_data(&r, 1);
    size_t nl = yyjson_arr_size(labels);
    size_t nt = yyjson_arr_size(types);
    out->node_labels = nl ? calloc(nl, sizeof(cbm_label_count_t)) : NULL;
    out->edge_types = nt ? calloc(nt, sizeof(cbm_type_count_t)) : NULL;
    if ((nl && !out->node_labels) || (nt && !out->edge_types)) {
        cbm_neo4j_result_free(&r);
        return CBM_STORE_ERR;
    }
    yyjson_arr_iter lit = yyjson_arr_iter_with(labels);
    for (size_t i = 0; i < nl; i++) {
        yyjson_val *e = yyjson_arr_iter_next(&lit);
        out->node_labels[i].label = cbm_neo4j_val_strdup(cbm_neo4j_row_col(e, 0), "");
        out->node_labels[i].count = (int)cbm_neo4j_val_int(cbm_neo4j_row_col(e, 1), 0);
    }
    out->node_label_count = (int)nl;
    yyjson_arr_iter tit = yyjson_arr_iter_with(types);
    for (size_t i = 0; i < nt; i++) {
        yyjson_val *e = yyjson_arr_iter_next(&tit);
        out->edge_types[i].type = cbm_neo4j_val_strdup(cbm_neo4j_row_col(e, 0), "");
        out->edge_types[i].count = (int)cbm_neo4j_val_int(cbm_neo4j_row_col(e, 1), 0);
    }
    out->edge_type_count = (int)nt;
    if (with_props) {
        yyjson_val *label_keys = cbm_neo4j_result_data(&r, 2);
        yyjson_val *type_keys = cbm_neo4j_result_data(&r, 3);
        for (size_t i = 0; i < nl; i++) {
            yyjson_val *keys = NULL;
            size_t idx;
            size_t max;
            yyjson_val *e;
            yyjson_arr_foreach(label_keys, idx, max, e) {
                const char *l = yyjson_get_str(cbm_neo4j_row_col(e, 0));
                if (l && strcmp(l, out->node_labels[i].label) == 0) {
                    keys = cbm_neo4j_row_col(e, 1);
                    break;
                }
            }
            out->node_labels[i].properties =
                schema_props(node_base, 5, keys, &out->node_labels[i].property_count);
        }
        for (size_t i = 0; i < nt; i++) {
            yyjson_val *keys = NULL;
            size_t idx;
            size_t max;
            yyjson_val *e;
            yyjson_arr_foreach(type_keys, idx, max, e) {
                const char *t = yyjson_get_str(cbm_neo4j_row_col(e, 0));
                if (t && strcmp(t, out->edge_types[i].type) == 0) {
                    keys = cbm_neo4j_row_col(e, 1);
                    break;
                }
            }
            out->edge_types[i].properties =
                schema_props(edge_base, 2, keys, &out->edge_types[i].property_count);
        }
    }
    cbm_neo4j_result_free(&r);
    return CBM_STORE_OK;
}

static uint64_t op_queries(void *ctx) {
    return ((n4s_ctx_t *)ctx)->queries;
}

static void op_set_read_deadline(void *ctx, uint64_t deadline_ms) {
    ((n4s_ctx_t *)ctx)->read_deadline_ms = deadline_ms;
}

static const cbm_store_remote_ops_t k_neo4j_ops = {
    .close = op_close,
    .error = op_error,
    .find_node_by_id = op_find_node_by_id,
    .find_node_by_qn = op_find_node_by_qn,
    .find_nodes = op_find_nodes,
    .find_edges = op_find_edges,
    .find_edges_by_type = op_find_edges_by_type,
    .count = op_count,
    .degrees = op_degrees,
    .bfs = op_bfs,
    .search = op_search,
    .neighbor_names = op_neighbor_names,
    .list_files = op_list_files,
    .schema = op_schema,
    .queries = op_queries,
    .prefetch_edges = op_prefetch_edges,
    .cache_clear = op_cache_clear,
    .find_nodes_by_ids = op_find_nodes_by_ids,
    .find_nodes_by_files = op_find_nodes_by_files,
    .scan_nodes = op_scan_nodes,
    .set_read_deadline = op_set_read_deadline,
};

/* ── Attach ─────────────────────────────────────────────────────── */

/* Connect to the project's Neo4j graph and read its published generation. */
static n4s_ctx_t *ctx_open(const char *db_path, const char *project, char *err, size_t err_sz,
                           int *status) {
    cbm_neo4j_config_t cfg;
    char synced[N4S_GEN_MAX];
    char marker_db[4096];
    if (cbm_neo4j_sidecar_locate(db_path, project, marker_db, sizeof(marker_db)) != 0) {
        *status = CBM_NEO4J_ATTACH_NONE;
        return NULL;
    }
    int rc = cbm_neo4j_sidecar_read(marker_db, &cfg, synced, sizeof(synced), err, err_sz);
    if (rc == 1) {
        *status = CBM_NEO4J_ATTACH_NONE;
        return NULL;
    }
    *status = CBM_NEO4J_ATTACH_FALLBACK;
    if (rc != 0) {
        return NULL;
    }
    if (!cfg.password[0]) {
        /* Don't spend a failed login (Neo4j rate-limits them) on a request
         * that cannot authenticate. */
        (void)snprintf(err, err_sz,
                       "no Neo4j password for %s: set NEO4J_PASSWORD in the MCP server's "
                       "environment, or re-run index_repository with --graph-password",
                       cfg.uri);
        return NULL;
    }
    n4s_ctx_t *x = calloc(1, sizeof(*x));
    if (!x) {
        cbm_neo4j_config_wipe(&cfg);
        return NULL;
    }
    x->c = cbm_neo4j_connect(&cfg, err, err_sz);
    cbm_neo4j_config_wipe(&cfg);
    if (!x->c) {
        free(x);
        return NULL;
    }
    /* Every read of the store runs in Neo4j's READ access mode: the server
     * refuses any write sent on this connection. */
    if (cbm_neo4j_set_read_only(x->c, N4S_READ_MAX_EXEC_MS) != 0) {
        cbm_neo4j_close(x->c);
        free(x);
        return NULL;
    }
    (void)snprintf(x->db, sizeof(x->db), "%s", project);
    cbm_neo4j_batch_t b;
    cbm_neo4j_result_t r;
    if (cbm_neo4j_batch_init(&b) != 0) {
        op_close(x);
        return NULL;
    }
    yyjson_mut_val *params =
        cbm_neo4j_batch_add(&b, "MATCH (p:CbmProject {name: $db}) RETURN p.gen, p.generation");
    yyjson_mut_obj_add_strcpy(b.doc, params, "db", project);
    int run_rc = cbm_neo4j_batch_run(x->c, &b, &r);
    cbm_neo4j_batch_free(&b);
    if (run_rc != 0) {
        (void)snprintf(err, err_sz, "%s", cbm_neo4j_error(x->c));
        op_close(x);
        return NULL;
    }
    yyjson_val *row = yyjson_arr_get_first(cbm_neo4j_result_data(&r, 0));
    if (!row) {
        cbm_neo4j_result_free(&r);
        (void)snprintf(err, err_sz, "project '%s' has not been synced to Neo4j yet", project);
        op_close(x);
        return NULL;
    }
    x->gen = cbm_neo4j_val_int(cbm_neo4j_row_col(row, 0), 0);
    char *remote_generation = cbm_neo4j_val_strdup(cbm_neo4j_row_col(row, 1), "");
    cbm_neo4j_result_free(&r);

    /* The local SQLite generation must be the one Neo4j mirrors; otherwise
     * ids from the two would not agree. */
    char local[N4S_GEN_MAX] = "";
    cbm_store_t *probe = cbm_store_open_path_query(db_path);
    if (probe) {
        (void)cbm_store_generation(probe, local, sizeof(local));
        cbm_store_close(probe);
    }
    bool same = remote_generation && local[0] && strcmp(remote_generation, local) == 0;
    free(remote_generation);
    if (!same) {
        (void)snprintf(err, err_sz,
                       "Neo4j holds an older generation of '%s' than the local index (last sync "
                       "failed?); re-run index_repository with graph_db to resync",
                       project);
        op_close(x);
        return NULL;
    }
    *status = CBM_NEO4J_ATTACHED;
    return x;
}

cbm_neo4j_attach_t cbm_neo4j_store_attach(cbm_store_t *s, const char *db_path, const char *project,
                                          char *err, size_t err_sz) {
    if (err && err_sz) {
        err[0] = '\0';
    }
    if (!s || !db_path || !project) {
        return CBM_NEO4J_ATTACH_NONE;
    }
    int status = CBM_NEO4J_ATTACH_NONE;
    n4s_ctx_t *x = ctx_open(db_path, project, err, err_sz, &status);
    if (!x) {
        if (status == CBM_NEO4J_ATTACH_FALLBACK) {
            cbm_log_warn("neo4j.attach", "project", project, "result", "unavailable", "err",
                         err ? err : "");
        }
        return (cbm_neo4j_attach_t)status;
    }
    cbm_store_attach_remote(s, &k_neo4j_ops, x);
    return CBM_NEO4J_ATTACHED;
}

/* ── Native Cypher ──────────────────────────────────────────────── */

static void native_fail(cbm_cypher_result_t *out, const char *msg) {
    free(out->error);
    out->error = n4s_dup(msg ? msg : "Neo4j query failed");
}

/* query_graph is a read tool: refuse Cypher that could write. Keywords are
 * matched as whole words outside string literals, comments and backticks. */
static const char *native_write_keyword(const char *q) {
    /* The server enforces READ access mode on these transactions
     * (cbm_neo4j_set_read_only); this keyword check only gives an early,
     * clear refusal. It lexes like Neo4j: block and line comments, string
     * escapes, backtick identifiers (no escapes; `` is a literal backtick). */
    static const char *const kw[] = {"CREATE", "MERGE",   "DELETE", "DETACH", "SET",    "REMOVE",
                                     "DROP",   "FOREACH", "LOAD",   "CALL",   "INSERT", "USE",
                                     "ALTER",  "GRANT",   "DENY",   "REVOKE", "RENAME", NULL};
    for (const char *p = q; *p;) {
        if (*p == '\'' || *p == '"') {
            char quote = *p++;
            while (*p && *p != quote) {
                p += (*p == '\\' && p[1]) ? 2 : 1;
            }
            p += *p ? 1 : 0;
            continue;
        }
        if (*p == '`') {
            p++;
            while (*p && *p != '`') {
                p++;
            }
            p += *p ? 1 : 0;
            continue;
        }
        if (p[0] == '/' && p[1] == '*') {
            p += 2;
            while (*p && !(p[0] == '*' && p[1] == '/')) {
                p++;
            }
            p += *p ? 2 : 0;
            continue;
        }
        if (p[0] == '/' && p[1] == '/') {
            while (*p && *p != '\n') {
                p++;
            }
            continue;
        }
        if (isalpha((unsigned char)*p) || *p == '_') {
            const char *w = p;
            while (isalnum((unsigned char)*p) || *p == '_') {
                p++;
            }
            size_t len = (size_t)(p - w);
            for (int i = 0; kw[i]; i++) {
                if (strlen(kw[i]) == len) {
                    size_t k = 0;
                    while (k < len && toupper((unsigned char)w[k]) == kw[i][k]) {
                        k++;
                    }
                    if (k == len) {
                        return kw[i];
                    }
                }
            }
            continue;
        }
        p++;
    }
    return NULL;
}

/* Statement 0 of r as a query_graph result, at most max_rows rows (0: the
 * legacy 100000). Returns 0, or -1 with out->error set. */
static int native_rows(cbm_neo4j_result_t *r, int max_rows, bool engine_compat,
                       cbm_cypher_result_t *out) {
    yyjson_val *res0 = yyjson_arr_get_first(r->results);
    yyjson_val *cols = yyjson_obj_get(res0, "columns");
    yyjson_val *data = yyjson_obj_get(res0, "data");
    int ncols = (int)yyjson_arr_size(cols);
    int nrows = (int)yyjson_arr_size(data);
    int cap = max_rows > 0 ? max_rows : 100000;
    if (nrows > cap) {
        nrows = cap;
        out->truncated = true;
    }
    out->columns = calloc(ncols > 0 ? (size_t)ncols : 1, sizeof(char *));
    out->rows = calloc(nrows > 0 ? (size_t)nrows : 1, sizeof(char **));
    if (!out->columns || !out->rows) {
        native_fail(out, "out of memory");
        return -1;
    }
    out->col_count = ncols; /* only now may cbm_cypher_result_free walk columns */
    for (int j = 0; j < ncols; j++) {
        out->columns[j] = cbm_neo4j_val_strdup(yyjson_arr_get(cols, (size_t)j), "");
    }
    yyjson_arr_iter it = yyjson_arr_iter_with(data);
    for (int i = 0; i < nrows; i++) {
        yyjson_val *e = yyjson_arr_iter_next(&it);
        const char **row = calloc(ncols > 0 ? (size_t)ncols : 1, sizeof(char *));
        if (!row) {
            /* A partial result must not read as complete. */
            native_fail(out, "out of memory");
            return -1;
        }
        for (int j = 0; j < ncols; j++) {
            yyjson_val *v = cbm_neo4j_row_col(e, (size_t)j);
            /* The engine renders a missing value as ""; native:true keeps
             * Neo4j's null. */
            row[j] = cbm_neo4j_val_strdup(v, yyjson_is_null(v) && !engine_compat ? "null" : "");
        }
        out->rows[i] = row;
        out->row_count = i + 1;
    }
    return 0;
}

/* ── Engine dialect → scoped Neo4j Cypher ──────────────────────── */

/* True when q (outside string literals) holds syntax the engine's lexer and
 * Neo4j's read differently, so a token-level rewrite of what the engine
 * saw would not be what Neo4j runs: comments (the engine drops `--` to the
 * end of the line, where Neo4j sees an undirected relationship), and
 * backtick identifiers. Sets *what. */
static bool scope_unsafe_syntax(const char *q, const char **what) {
    for (const char *p = q; *p;) {
        if (*p == '\'' || *p == '"') {
            char quote = *p++;
            while (*p && *p != quote) {
                p += (*p == '\\' && p[1]) ? 2 : 1;
            }
            p += *p ? 1 : 0;
            continue;
        }
        if (p[0] == '-' && p[1] == '-') {
            *what = "'--' (a comment to the engine, a relationship to Neo4j)";
            return true;
        }
        if ((p[0] == '/' && p[1] == '/') || (p[0] == '/' && p[1] == '*')) {
            *what = "a comment";
            return true;
        }
        if (*p == '`') {
            *what = "a backtick identifier";
            return true;
        }
        p++;
    }
    return false;
}

/* Does tokens[i] ('(') open a node pattern `( [var] (:L(|L)*)* [{...}] )`?
 * On success *brace is the '{' index (-1 if none) and *close the ')'. */
static bool node_pattern_shape(const cbm_token_t *t, int count, int i, int *brace, int *close) {
    int j = i + 1;
    if (j < count && t[j].type == TOK_IDENT) {
        j++;
    }
    while (j < count && t[j].type == TOK_COLON) {
        j++;
        if (j >= count || t[j].type != TOK_IDENT) {
            return false;
        }
        j++;
        while (j + 1 < count && t[j].type == TOK_PIPE && t[j + 1].type == TOK_IDENT) {
            j += 2;
        }
    }
    *brace = -1;
    if (j < count && t[j].type == TOK_LBRACE) {
        *brace = j;
        int depth = 0;
        for (; j < count; j++) {
            if (t[j].type == TOK_LBRACE) {
                depth++;
            } else if (t[j].type == TOK_RBRACE && --depth == 0) {
                break;
            }
        }
        if (j >= count) {
            return false;
        }
        j++;
    }
    if (j >= count || t[j].type != TOK_RPAREN) {
        return false;
    }
    *close = j;
    return true;
}

static bool tok_is_word(const cbm_token_t *t, const char *word) {
    if (t->type != TOK_IDENT || !t->text) {
        return false;
    }
    size_t n = strlen(word);
    if (strlen(t->text) != n) {
        return false;
    }
    for (size_t k = 0; k < n; k++) {
        if (tolower((unsigned char)t->text[k]) != word[k]) {
            return false;
        }
    }
    return true;
}

typedef struct {
    int pos;          /* byte offset in the original query */
    int seq;          /* order among insertions at the same offset */
    const char *text; /* inserted there */
    int skip;         /* original bytes it replaces (0: a pure insertion) */
} n4s_insert_t;

/* "6", "-3", "1.5": a string literal Neo4j and the engine compare differently
 * (the engine compares text; Neo4j a string with a number never matches). */
static bool numeric_text(const char *s) {
    if (!s || !*s) {
        return false;
    }
    if (*s == '-') {
        s++;
    }
    int digits = 0;
    while (isdigit((unsigned char)*s)) {
        s++;
        digits++;
    }
    if (*s == '.') {
        s++;
        int frac = 0;
        while (isdigit((unsigned char)*s)) {
            s++;
            frac++;
        }
        if (frac == 0) {
            return false;
        }
    }
    return digits > 0 && *s == '\0';
}

/* A variable-length hop range at the '*' of t[i], rewritten to what the
 * engine runs: `*N` is 1..N there (exactly N in Neo4j), and every range is
 * capped at cbm_cypher_max_depth() (Neo4j would walk an unbounded `*`).
 * Appends to ins (numbers from nums[]); false with why[] when the range
 * cannot be matched (its lower bound is past the cap). */
static bool scope_hop_range(const cbm_token_t *t, int count, int i, int cap, n4s_insert_t *ins,
                            int *nins, char (*nums)[24], char *why, size_t why_sz) {
    int j = i + 1;
    const cbm_token_t *lo = j < count && t[j].type == TOK_NUMBER ? &t[j] : NULL;
    if (lo) {
        j++;
    }
    const cbm_token_t *dots = j < count && t[j].type == TOK_DOTDOT ? &t[j] : NULL;
    if (dots) {
        j++;
    }
    const cbm_token_t *hi = dots && j < count && t[j].type == TOK_NUMBER ? &t[j] : NULL;
    long lo_v = lo ? strtol(lo->text, NULL, 10) : 1;
    long hi_v = hi ? strtol(hi->text, NULL, 10) : 0;
    n4s_insert_t *e = &ins[*nins];
    char *num = nums[*nins];
    memset(e, 0, sizeof(*e));
    e->seq = *nins;
    if (!lo && !dots) { /* * -> *1..cap */
        (void)snprintf(num, 24, "1..%d", cap);
        e->pos = t[i].pos + 1;
    } else if (lo && !dots) { /* *N -> *1..N (N capped) */
        if (lo_v > cap) {
            (void)snprintf(num, 24, "1..%d", cap);
            e->skip = (int)strlen(lo->text);
        } else {
            (void)snprintf(num, 24, "1..");
        }
        e->pos = lo->pos;
    } else {
        if (lo && lo_v > cap) {
            if (why) {
                (void)snprintf(why, why_sz,
                               "its hop range starts beyond the engine's depth limit (%d)", cap);
            }
            return false;
        }
        if (hi) { /* *N..M / *..M: M capped */
            if (hi_v <= cap) {
                return true;
            }
            (void)snprintf(num, 24, "%d", cap);
            e->pos = hi->pos;
            e->skip = (int)strlen(hi->text);
        } else { /* *N.. / *.. -> upper bound cap */
            (void)snprintf(num, 24, "%d", cap);
            e->pos = dots->pos + 2;
        }
    }
    e->text = num;
    (*nins)++;
    return true;
}

static int cmp_insert(const void *a, const void *b) {
    const n4s_insert_t *x = a;
    const n4s_insert_t *y = b;
    if (x->pos != y->pos) {
        return x->pos < y->pos ? -1 : 1;
    }
    return x->seq < y->seq ? -1 : (x->seq > y->seq);
}

char *cbm_neo4j_scope_query(const char *query, char *why, size_t why_sz) {
    if (why && why_sz) {
        why[0] = '\0';
    }
    if (!query) {
        return NULL;
    }
    const char *unsafe = NULL;
    if (scope_unsafe_syntax(query, &unsafe)) {
        if (why) {
            (void)snprintf(why, why_sz, "it contains %s", unsafe);
        }
        return NULL;
    }
    cbm_lex_result_t lx = {0};
    if (cbm_lex(query, &lx) < 0 || lx.error) {
        if (why) {
            (void)snprintf(why, why_sz, "query does not tokenize");
        }
        cbm_lex_free(&lx);
        return NULL;
    }
    const cbm_token_t *t = lx.tokens;
    int count = lx.count;
    /* up to three insertions per node pattern (label, its closing ')',
     * scope) and one per hop range */
    n4s_insert_t *ins = calloc((size_t)count * 4 + 1, sizeof(n4s_insert_t));
    char (*nums)[24] = calloc((size_t)count * 4 + 1, sizeof(*nums));
    if (!ins || !nums) {
        free(ins);
        free(nums);
        cbm_lex_free(&lx);
        return NULL;
    }
    int nins = 0;
    bool in_match = false; /* inside a MATCH clause's pattern list */
    bool in_rel = false;   /* inside a relationship's [ ] */
    const int depth_cap = cbm_cypher_max_depth();
    bool ok = true;
    for (int i = 0; i < count && ok; i++) {
        cbm_token_type_t ty = t[i].type;
        if (ty == TOK_LBRACKET) {
            in_rel = i > 0 && t[i - 1].type == TOK_DASH; /* -[ ... ] or <-[ ... ] */
        } else if (ty == TOK_RBRACKET) {
            in_rel = false;
        }
        if (ty == TOK_STRING && numeric_text(t[i].text)) {
            if (why) {
                (void)snprintf(why, why_sz,
                               "it compares with a numeric string ('%.20s'): the engine compares "
                               "text, Neo4j typed values",
                               t[i].text);
            }
            ok = false;
            break;
        }
        if (ty == TOK_STAR && in_rel) {
            if (!scope_hop_range(t, count, i, depth_cap, ins, &nins, nums, why, why_sz)) {
                ok = false;
                break;
            }
            continue;
        }
        if (ty == TOK_MATCH) {
            in_match = true;
        } else if (ty == TOK_WHERE || ty == TOK_RETURN || ty == TOK_WITH || ty == TOK_UNWIND ||
                   ty == TOK_ORDER || ty == TOK_UNION) {
            in_match = false;
        }
        /* =~ is POSIX ERE in the engine and Java regex in Neo4j: the same
         * pattern can match different strings ([[:digit:]] matches nothing). */
        if (ty == TOK_EQTILDE) {
            if (why) {
                (void)snprintf(why, why_sz,
                               "regular expressions (=~) differ between the engine "
                               "and Neo4j");
            }
            ok = false;
            break;
        }
        /* n.in_degree / n.out_degree are computed by the engine, not stored. */
        if (ty == TOK_DOT && i + 1 < count &&
            (tok_is_word(&t[i + 1], "in_degree") || tok_is_word(&t[i + 1], "out_degree"))) {
            if (why) {
                (void)snprintf(why, why_sz, "it reads the engine-only property %s", t[i + 1].text);
            }
            ok = false;
            break;
        }
        /* {db: ...} / {gen: ...} written by the query would sit next to the
         * injected scope keys (Neo4j accepts a duplicate key silently). */
        if (ty == TOK_LBRACE || ty == TOK_COMMA) {
            if (i + 2 < count && t[i + 2].type == TOK_COLON &&
                (tok_is_word(&t[i + 1], "db") || tok_is_word(&t[i + 1], "gen") ||
                 tok_is_word(&t[i + 1], "project"))) {
                if (why) {
                    (void)snprintf(why, why_sz, "it sets the reserved property %s", t[i + 1].text);
                }
                ok = false;
                break;
            }
        }
        if (ty != TOK_LPAREN) {
            continue;
        }
        int brace = -1;
        int close = -1;
        if (!node_pattern_shape(t, count, i, &brace, &close)) {
            continue;
        }
        cbm_token_type_t prev = i > 0 ? t[i - 1].type : TOK_EOF;
        /* count(n) - 1, toLower(x) ...: a call's parentheses, not a node. */
        if (prev == TOK_IDENT || prev == TOK_COUNT || prev == TOK_SUM || prev == TOK_AVG ||
            prev == TOK_MIN_KW || prev == TOK_MAX_KW || prev == TOK_COLLECT ||
            prev == TOK_TOLOWER || prev == TOK_TOUPPER || prev == TOK_TOSTRING ||
            prev == TOK_EXISTS) {
            continue;
        }
        cbm_token_type_t next = close + 1 < count ? t[close + 1].type : TOK_EOF;
        bool pattern_context = prev == TOK_MATCH || prev == TOK_DASH || prev == TOK_GT ||
                               prev == TOK_LT || (prev == TOK_COMMA && in_match) ||
                               (prev == TOK_EQ && in_match) || next == TOK_DASH || next == TOK_LT;
        if (!pattern_context) {
            continue;
        }
        /* Every graph node carries the CbmNode label, and Neo4j's indexes
         * are per label: without it each pattern scans every node of every
         * project in the database. */
        int j = i + 1;
        if (t[j].type == TOK_IDENT) {
            j++;
        }
        int first_colon = t[j].type == TOK_COLON ? j : -1;
        int colons = 0;
        bool alternation = false;
        while (j < close && t[j].type == TOK_COLON) {
            colons++;
            j += 2; /* ':' Label */
            while (j + 1 < close && t[j].type == TOK_PIPE) {
                alternation = true;
                j += 2;
            }
        }
        int label_end = j; /* the '{' or ')' after the labels */
        if (first_colon < 0) {
            ins[nins].pos = t[label_end].pos;
            ins[nins].seq = nins;
            ins[nins].text = brace >= 0 ? ":CbmNode " : ":CbmNode";
            nins++;
        } else if (!alternation) {
            ins[nins].pos = t[first_colon].pos; /* (n:CbmNode:Function) */
            ins[nins].seq = nins;
            ins[nins].text = ":CbmNode";
            nins++;
        } else if (colons == 1) {
            ins[nins].pos = t[first_colon].pos + 1; /* (n:CbmNode&(File|Folder)) */
            ins[nins].seq = nins;
            ins[nins].text = "CbmNode&(";
            nins++;
            ins[nins].pos = t[label_end].pos;
            ins[nins].seq = nins;
            ins[nins].text = ")";
            nins++;
        } else {
            if (why) {
                (void)snprintf(why, why_sz, "it mixes label alternation with several labels");
            }
            ok = false;
            break;
        }
        if (brace >= 0) {
            bool empty = t[brace + 1].type == TOK_RBRACE;
            ins[nins].pos = t[brace].pos + 1;
            ins[nins].text = empty ? "db: $db, gen: $gen, project: $project"
                                   : "db: $db, gen: $gen, project: $project, ";
        } else {
            ins[nins].pos = t[close].pos;
            ins[nins].text = " {db: $db, gen: $gen, project: $project}";
        }
        ins[nins].seq = nins;
        nins++;
    }
    cbm_lex_free(&lx);
    bool scoped_any = false;
    for (int k = 0; k < nins && !scoped_any; k++) {
        scoped_any = strstr(ins[k].text, "$db") != NULL;
    }
    if (!ok || !scoped_any) {
        if (ok && why) {
            (void)snprintf(why, why_sz, "no node pattern to scope");
        }
        free(ins);
        free(nums);
        return NULL;
    }
    qsort(ins, (size_t)nins, sizeof(n4s_insert_t), cmp_insert);
    size_t qlen = strlen(query);
    size_t extra = 0;
    for (int k = 0; k < nins; k++) {
        extra += strlen(ins[k].text);
    }
    char *outq = malloc(qlen + extra + 1);
    if (!outq) {
        free(ins);
        free(nums);
        return NULL;
    }
    size_t w = 0;
    size_t from = 0;
    for (int k = 0; k < nins; k++) {
        size_t at = (size_t)ins[k].pos;
        if (at > qlen || at < from || at + (size_t)ins[k].skip > qlen) {
            free(ins);
            free(nums);
            free(outq);
            if (why) {
                (void)snprintf(why, why_sz, "token positions are inconsistent");
            }
            return NULL;
        }
        memcpy(outq + w, query + from, at - from);
        w += at - from;
        size_t tl = strlen(ins[k].text);
        memcpy(outq + w, ins[k].text, tl);
        w += tl;
        from = at + (size_t)ins[k].skip;
    }
    memcpy(outq + w, query + from, qlen - from);
    w += qlen - from;
    outq[w] = '\0';
    free(ins);
    free(nums);
    return outq;
}

int cbm_neo4j_native_query_ex(const char *db_path, const char *project, const char *query,
                              int max_rows, bool engine_compat, cbm_cypher_result_t *out) {
    memset(out, 0, sizeof(*out));
    const char *kw = native_write_keyword(query);
    if (kw) {
        char msg[160];
        (void)snprintf(msg, sizeof(msg), "native query_graph is read-only: '%s' is not allowed",
                       kw);
        native_fail(out, msg);
        return -1;
    }
    char err[CBM_NEO4J_ERR_MAX] = "";
    int status = 0;
    n4s_ctx_t *x = ctx_open(db_path, project, err, sizeof(err), &status);
    if (!x) {
        native_fail(out, status == CBM_NEO4J_ATTACH_NONE
                             ? "project is not Neo4j-backed (index it with graph_db first)"
                             : err);
        return -1;
    }
    /* Neo4j itself refuses writes and stops the query after the limit. */
    if (cbm_neo4j_set_read_only(x->c, N4S_NATIVE_MAX_EXEC_MS) != 0) {
        op_close(x);
        native_fail(out, "out of memory");
        return -1;
    }
    n4s_q_t q;
    if (q_begin(x, &q, query) != 0) {
        op_close(x);
        native_fail(out, "out of memory");
        return -1;
    }
    q_str(&q, "project", project);
    cbm_neo4j_result_t r;
    if (q_run(x, &q, &r) != CBM_STORE_OK) {
        native_fail(out, x->err);
        op_close(x);
        return -1;
    }
    int rc = native_rows(&r, max_rows, engine_compat, out);
    cbm_neo4j_result_free(&r);
    op_close(x);
    return rc;
}

int cbm_neo4j_native_query(const char *db_path, const char *project, const char *query,
                           int max_rows, cbm_cypher_result_t *out) {
    return cbm_neo4j_native_query_ex(db_path, project, query, max_rows, false, out);
}

#ifdef CBM_ENABLE_TEST_SEAMS
/* Tests: a raw Neo4j response body, checked like cbm_neo4j_batch_run checks
 * one, read through the same row conversion the store uses. */
static int test_result_from_body(const char *body, cbm_neo4j_result_t *r) {
    r->doc = yyjson_read(body, strlen(body), 0);
    r->results = yyjson_obj_get(yyjson_doc_get_root(r->doc), "results");
    if (!r->doc || cbm_neo4j_result_shape_error(r->results, 1) != NULL) {
        cbm_neo4j_result_free(r);
        return -1;
    }
    return 0;
}

int cbm_neo4j_test_native_rows(const char *body, int max_rows, cbm_cypher_result_t *out) {
    memset(out, 0, sizeof(*out));
    cbm_neo4j_result_t r;
    if (test_result_from_body(body, &r) != 0) {
        native_fail(out, "unparseable test body");
        return -1;
    }
    int rc = native_rows(&r, max_rows, false, out);
    cbm_neo4j_result_free(&r);
    return rc;
}

int cbm_neo4j_test_column_strings(const char *body, char ***out, int *count) {
    cbm_neo4j_result_t r;
    if (test_result_from_body(body, &r) != 0) {
        return CBM_STORE_ERR;
    }
    int rc = collect_strings(&r, 0, out, count);
    cbm_neo4j_result_free(&r);
    return rc;
}
#endif /* CBM_ENABLE_TEST_SEAMS */
