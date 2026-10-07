/*
 * store_impact.c — fixpoint impact walk over the stored graph. See
 * store_impact.h for the contract.
 *
 * The walk is level-synchronous. Each level's frontier is sorted by qualified
 * name and expanded edge type by edge type, in the rank order of the request:
 * the first time a node is seen is therefore through its smallest (edge rank,
 * parent name), which is the parent the contract promises, with no comparison
 * in the inner loop.
 */
#include "store/store_impact.h"

#include "foundation/constants.h"
#include "foundation/mem_core.h"

#include <limits.h>
#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>

enum {
    IMPACT_INIT_CAP = 64,
    IMPACT_GROWTH = 2,
    IMPACT_COL_QN = 0,
    IMPACT_COL_LABEL = 1,
    IMPACT_BIND_ID = 1,
    IMPACT_BIND_TYPE = 2,
    IMPACT_ROUTE_IN_COUNT = 3,
};

static const char IMPACT_HANDLES[] = "HANDLES";
static const char IMPACT_ROUTE_LABEL[] = "Route";
static const char *const IMPACT_ROUTE_IN[IMPACT_ROUTE_IN_COUNT] = {"HTTP_CALLS", "ASYNC_CALLS",
                                                                   "GRPC_CALLS"};

struct cbm_impact_walk {
    sqlite3 *db;
    cbm_store_read_scope_t *scope; /* borrowed; NULL preserves legacy path */
    int status;                    /* sticky, keep count intact for allocated-row cleanup */
    char *project;
    char **edge_types; /* owned copies: a hit's via_edge points into these */
    int edge_type_count;
    bool follow_routes;
    int max_hops; /* 0 = fixpoint */

    int64_t max_id; /* largest node id when the walk opened */
    int32_t *slot;  /* [max_id + 1]: index into hits + 1; 0 = not reached */
    uint8_t *sink;  /* [max_id + 1]: reached, never expanded */

    cbm_impact_hit_t *hits;
    int count;
    int cap;
    int max_hop;

    sqlite3_stmt *stmt_node; /* qualified name and label of one node */
    sqlite3_stmt *stmt_in;   /* sources of the edges of one type into a node */
    sqlite3_stmt *stmt_out;  /* targets of the edges of one type out of a node */
};

/* One frontier entry: the name to sort by and the hit it belongs to. The name
 * pointer stays valid while the hit array grows; an index into it does too. */
typedef struct {
    const char *qualified_name;
    int hit;
} impact_frontier_t;

typedef struct {
    impact_frontier_t *items;
    int count;
    int cap;
} impact_frontier_list_t;

static int impact_frontier_cmp(const void *left, const void *right) {
    const impact_frontier_t *a = (const impact_frontier_t *)left;
    const impact_frontier_t *b = (const impact_frontier_t *)right;
    return strcmp(a->qualified_name, b->qualified_name);
}

static int impact_hit_cmp(const void *left, const void *right) {
    const cbm_impact_hit_t *a = (const cbm_impact_hit_t *)left;
    const cbm_impact_hit_t *b = (const cbm_impact_hit_t *)right;
    if (a->hop != b->hop) {
        return a->hop < b->hop ? -1 : 1;
    }
    return strcmp(a->qualified_name, b->qualified_name);
}

/* Keep allocated rows for close even when their public view is invalidated. */
static int impact_fail(cbm_impact_walk_t *w, int status) {
    if (!w) {
        return CBM_STORE_ERR;
    }
    if (w->status == CBM_STORE_OK) {
        w->status = w->scope
                        ? cbm_store_read_scope_fail(w->scope, status)
                        : (status == CBM_STORE_CANCELLED ? CBM_STORE_CANCELLED : CBM_STORE_ERR);
    }
    sqlite3_reset(w->stmt_node);
    sqlite3_reset(w->stmt_in);
    sqlite3_reset(w->stmt_out);
    return w->status;
}

static bool impact_check(cbm_impact_walk_t *w) {
    if (!w || w->status != CBM_STORE_OK) {
        return false;
    }
    int status = w->scope ? cbm_store_read_scope_check(w->scope) : CBM_STORE_OK;
    if (status != CBM_STORE_OK) {
        impact_fail(w, status);
        return false;
    }
    return true;
}

static bool impact_visible(const cbm_impact_walk_t *w) {
    return w && w->status == CBM_STORE_OK &&
           (!w->scope || cbm_store_read_scope_check(w->scope) == CBM_STORE_OK);
}

/* Scoped sorting polls inside comparisons and heap work. Each swap is atomic
 * with respect to cancellation so the array remains a permutation for cleanup.
 * The legacy path retains its existing qsort and comparators exactly. */
static bool impact_sort_compare(cbm_impact_walk_t *w, const void *a, const void *b, bool hits,
                                int *order) {
    if (!impact_check(w)) {
        return false;
    }
    const char *left;
    const char *right;
    if (hits) {
        const cbm_impact_hit_t *x = a;
        const cbm_impact_hit_t *y = b;
        if (x->hop != y->hop) {
            *order = x->hop < y->hop ? -1 : 1;
            return true;
        }
        left = x->qualified_name;
        right = y->qualified_name;
    } else {
        left = ((const impact_frontier_t *)a)->qualified_name;
        right = ((const impact_frontier_t *)b)->qualified_name;
    }
    const unsigned char *x = (const unsigned char *)left;
    const unsigned char *y = (const unsigned char *)right;
    unsigned ticks = 0;
    while (*x && *x == *y) {
        x++;
        y++;
        if (++ticks == 256) {
            ticks = 0;
            if (!impact_check(w)) {
                return false;
            }
        }
    }
    *order = (*x > *y) - (*x < *y);
    return true;
}

static void impact_sort_swap(unsigned char *a, unsigned char *b, size_t size) {
    for (size_t i = 0; i < size; i++) {
        unsigned char byte = a[i];
        a[i] = b[i];
        b[i] = byte;
    }
}

static bool impact_sift(cbm_impact_walk_t *w, unsigned char *items, size_t count, size_t root,
                        size_t size, bool hits) {
    while (root < count / 2) {
        if (!impact_check(w)) {
            return false;
        }
        size_t child = root * 2 + 1;
        int order;
        if (child + 1 < count) {
            if (!impact_sort_compare(w, items + child * size, items + (child + 1) * size, hits,
                                     &order)) {
                return false;
            }
            if (order < 0) {
                child++;
            }
        }
        if (!impact_sort_compare(w, items + root * size, items + child * size, hits, &order)) {
            return false;
        }
        if (order >= 0) {
            break;
        }
        impact_sort_swap(items + root * size, items + child * size, size);
        root = child;
    }
    return true;
}

static bool impact_sort(cbm_impact_walk_t *w, void *array, int count, size_t size, bool hits) {
    if (!impact_check(w)) {
        return false;
    }
    if (!w->scope) {
        if (count > 1) {
            qsort(array, (size_t)count, size, hits ? impact_hit_cmp : impact_frontier_cmp);
        }
        return true;
    }
    unsigned char *items = array;
    for (size_t root = (size_t)count / 2; root > 0; root--) {
        if (!impact_sift(w, items, (size_t)count, root - 1, size, hits)) {
            return false;
        }
    }
    for (size_t end = (size_t)count; end > 1; end--) {
        if (!impact_check(w)) {
            return false;
        }
        impact_sort_swap(items, items + (end - 1) * size, size);
        if (!impact_sift(w, items, end - 1, 0, size, hits)) {
            return false;
        }
    }
    return impact_check(w);
}

static bool impact_frontier_push(impact_frontier_list_t *list, const char *qualified_name,
                                 int hit) {
    if (list->count == list->cap) {
        if (list->cap > INT_MAX / IMPACT_GROWTH) {
            return false;
        }
        int cap = list->cap ? list->cap * IMPACT_GROWTH : IMPACT_INIT_CAP;
        impact_frontier_t *grown = (impact_frontier_t *)cbm_realloc(
            CBM_MEM_CLASS_STORE, list->items, (size_t)cap * sizeof(*grown));
        if (!grown) {
            return false;
        }
        list->items = grown;
        list->cap = cap;
    }
    list->items[list->count].qualified_name = qualified_name;
    list->items[list->count].hit = hit;
    list->count++;
    return true;
}

static bool impact_known_id(const cbm_impact_walk_t *w, int64_t id) {
    return id > 0 && id <= w->max_id;
}

typedef enum {
    IMPACT_ADD_OK = 0,
    IMPACT_ADD_NO_NODE, /* the id names no node of this project */
    IMPACT_ADD_ERROR,   /* allocation or SQLite failure */
} impact_add_t;

/* Record `id` as reached and queue it for expansion. The caller has checked
 * that it is in range and not reached yet. */
static impact_add_t impact_add_hit(cbm_impact_walk_t *w, impact_frontier_list_t *next, int64_t id,
                                   int hop, int64_t via_id, const char *via_edge) {
    if (!impact_check(w) || sqlite3_reset(w->stmt_node) != SQLITE_OK ||
        sqlite3_bind_int64(w->stmt_node, IMPACT_BIND_ID, id) != SQLITE_OK) {
        return IMPACT_ADD_ERROR;
    }
    int rc = sqlite3_step(w->stmt_node);
    if (!impact_check(w)) {
        return IMPACT_ADD_ERROR;
    }
    if (rc == SQLITE_DONE) {
        sqlite3_reset(w->stmt_node);
        return IMPACT_ADD_NO_NODE;
    }
    if (rc != SQLITE_ROW) {
        return IMPACT_ADD_ERROR;
    }
    const char *qn = (const char *)sqlite3_column_text(w->stmt_node, IMPACT_COL_QN);
    const char *label = (const char *)sqlite3_column_text(w->stmt_node, IMPACT_COL_LABEL);
    if (w->count == w->cap) {
        if (w->cap > INT_MAX / IMPACT_GROWTH) {
            return IMPACT_ADD_ERROR;
        }
        int cap = w->cap ? w->cap * IMPACT_GROWTH : IMPACT_INIT_CAP;
        cbm_impact_hit_t *grown = (cbm_impact_hit_t *)cbm_realloc(CBM_MEM_CLASS_STORE, w->hits,
                                                                  (size_t)cap * sizeof(*grown));
        if (!grown) {
            return IMPACT_ADD_ERROR;
        }
        w->hits = grown;
        w->cap = cap;
    }
    char *qn_copy = cbm_mem_strdup(CBM_MEM_CLASS_STORE, qn ? qn : "");
    char *label_copy = cbm_mem_strdup(CBM_MEM_CLASS_STORE, label ? label : "");
    if (!qn_copy || !label_copy || !impact_frontier_push(next, qn_copy, w->count)) {
        cbm_free(CBM_MEM_CLASS_STORE, qn_copy);
        cbm_free(CBM_MEM_CLASS_STORE, label_copy);
        return IMPACT_ADD_ERROR;
    }
    cbm_impact_hit_t *hit = &w->hits[w->count];
    hit->id = id;
    hit->hop = hop;
    hit->via_id = via_id;
    hit->via_edge = via_edge;
    hit->qualified_name = qn_copy;
    hit->label = label_copy;
    w->slot[id] = w->count + SKIP_ONE;
    w->count++;
    if (hop > w->max_hop) {
        w->max_hop = hop;
    }
    if (sqlite3_reset(w->stmt_node) != SQLITE_OK || !impact_check(w)) {
        return IMPACT_ADD_ERROR;
    }
    return IMPACT_ADD_OK;
}

/* Follow one edge type from `from` (a frontier hit) to every neighbour not
 * reached yet. stmt selects the far end of the edges of that type. */
static bool impact_expand(cbm_impact_walk_t *w, sqlite3_stmt *stmt, int from, const char *edge_type,
                          impact_frontier_list_t *next) {
    if (!impact_check(w) || w->hits[from].hop == INT_MAX) {
        return false;
    }
    int64_t from_id = w->hits[from].id;
    int hop = w->hits[from].hop + SKIP_ONE;
    if (sqlite3_reset(stmt) != SQLITE_OK ||
        sqlite3_bind_int64(stmt, IMPACT_BIND_ID, from_id) != SQLITE_OK ||
        sqlite3_bind_text(stmt, IMPACT_BIND_TYPE, edge_type, CBM_NOT_FOUND, SQLITE_STATIC) !=
            SQLITE_OK) {
        return false;
    }
    int rc;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        if (!impact_check(w)) {
            return false;
        }
        int64_t id = sqlite3_column_int64(stmt, 0);
        if (!impact_known_id(w, id) || w->slot[id] != 0) {
            continue;
        }
        /* An edge whose far end has no node row (or one of another project)
         * leads nowhere; it is not an error of the request. */
        if (impact_add_hit(w, next, id, hop, from_id, edge_type) == IMPACT_ADD_ERROR) {
            return false;
        }
    }
    return rc == SQLITE_DONE && sqlite3_reset(stmt) == SQLITE_OK && impact_check(w);
}

static bool impact_expand_level(cbm_impact_walk_t *w, const impact_frontier_list_t *frontier,
                                impact_frontier_list_t *next) {
    for (int t = 0; t < w->edge_type_count; t++) {
        for (int f = 0; f < frontier->count; f++) {
            if (!impact_check(w)) {
                return false;
            }
            int hit = frontier->items[f].hit;
            if (w->sink[w->hits[hit].id]) {
                continue;
            }
            if (!impact_expand(w, w->stmt_in, hit, w->edge_types[t], next)) {
                return false;
            }
        }
    }
    if (!w->follow_routes) {
        return true;
    }
    for (int f = 0; f < frontier->count; f++) {
        if (!impact_check(w)) {
            return false;
        }
        int hit = frontier->items[f].hit;
        if (!w->sink[w->hits[hit].id] &&
            !impact_expand(w, w->stmt_out, hit, IMPACT_HANDLES, next)) {
            return false;
        }
    }
    for (int t = 0; t < IMPACT_ROUTE_IN_COUNT; t++) {
        for (int f = 0; f < frontier->count; f++) {
            if (!impact_check(w)) {
                return false;
            }
            int hit = frontier->items[f].hit;
            if (w->sink[w->hits[hit].id] || strcmp(w->hits[hit].label, IMPACT_ROUTE_LABEL) != 0) {
                continue;
            }
            if (!impact_expand(w, w->stmt_in, hit, IMPACT_ROUTE_IN[t], next)) {
                return false;
            }
        }
    }
    return true;
}

static void impact_free_strings(cbm_impact_walk_t *w) {
    for (int i = 0; i < w->count; i++) {
        cbm_free(CBM_MEM_CLASS_STORE, (char *)w->hits[i].qualified_name);
        cbm_free(CBM_MEM_CLASS_STORE, (char *)w->hits[i].label);
    }
}

void cbm_impact_walk_close(cbm_impact_walk_t *w) {
    if (!w) {
        return;
    }
    sqlite3_finalize(w->stmt_node);
    sqlite3_finalize(w->stmt_in);
    sqlite3_finalize(w->stmt_out);
    impact_free_strings(w);
    cbm_free(CBM_MEM_CLASS_STORE, w->hits);
    cbm_free(CBM_MEM_CLASS_STORE, w->slot);
    cbm_free(CBM_MEM_CLASS_STORE, w->sink);
    for (int i = 0; i < w->edge_type_count; i++) {
        cbm_free(CBM_MEM_CLASS_STORE, w->edge_types[i]);
    }
    cbm_free(CBM_MEM_CLASS_STORE, w->edge_types);
    cbm_free(CBM_MEM_CLASS_STORE, w->project);
    cbm_free(CBM_MEM_CLASS_STORE, w);
}

static bool impact_copy_policy(cbm_impact_walk_t *w, const cbm_impact_policy_t *policy) {
    w->project = cbm_mem_strdup(CBM_MEM_CLASS_STORE, policy->project);
    w->edge_types =
        (char **)cbm_calloc(CBM_MEM_CLASS_STORE, (size_t)policy->edge_type_count * sizeof(char *));
    if (!w->project || !w->edge_types) {
        return false;
    }
    for (int i = 0; i < policy->edge_type_count; i++) {
        if (!impact_check(w)) {
            return false;
        }
        if (!policy->edge_types[i] || !policy->edge_types[i][0]) {
            return false;
        }
        w->edge_types[i] = cbm_mem_strdup(CBM_MEM_CLASS_STORE, policy->edge_types[i]);
        if (!w->edge_types[i]) {
            return false;
        }
        w->edge_type_count = i + SKIP_ONE;
    }
    w->follow_routes = policy->follow_routes;
    w->max_hops = policy->max_hops > 0 ? policy->max_hops : 0;
    return true;
}

static bool impact_prepare(cbm_impact_walk_t *w) {
    sqlite3_stmt *max_stmt = NULL;
    if (!impact_check(w) || sqlite3_prepare_v2(w->db, "SELECT COALESCE(MAX(id), 0) FROM nodes",
                                               CBM_NOT_FOUND, &max_stmt, NULL) != SQLITE_OK) {
        return false;
    }
    bool have_max = sqlite3_step(max_stmt) == SQLITE_ROW;
    if (have_max) {
        w->max_id = sqlite3_column_int64(max_stmt, 0);
    }
    int finalized = sqlite3_finalize(max_stmt);
    if (!have_max || finalized != SQLITE_OK || !impact_check(w) || w->max_id < 0 ||
        w->max_id >= INT32_MAX) {
        return false;
    }
    /* The whole per-node state, sized once from the graph: whether the walk
     * can run is decided here, not while it is half way through. */
    size_t slots = (size_t)w->max_id + SKIP_ONE;
    w->slot = (int32_t *)cbm_calloc(CBM_MEM_CLASS_STORE, slots * sizeof(int32_t));
    w->sink = (uint8_t *)cbm_calloc(CBM_MEM_CLASS_STORE, slots);
    if (!w->slot || !w->sink) {
        return false;
    }
    if (!impact_check(w) ||
        sqlite3_prepare_v2(w->db,
                           "SELECT qualified_name, label FROM nodes WHERE id = ?1 AND project = ?2",
                           CBM_NOT_FOUND, &w->stmt_node, NULL) != SQLITE_OK ||
        !impact_check(w) ||
        sqlite3_prepare_v2(w->db, "SELECT source_id FROM edges WHERE target_id = ?1 AND type = ?2",
                           CBM_NOT_FOUND, &w->stmt_in, NULL) != SQLITE_OK ||
        !impact_check(w) ||
        sqlite3_prepare_v2(w->db, "SELECT target_id FROM edges WHERE source_id = ?1 AND type = ?2",
                           CBM_NOT_FOUND, &w->stmt_out, NULL) != SQLITE_OK) {
        return false;
    }
    /* The project never changes for the life of the walk. */
    return sqlite3_bind_text(w->stmt_node, IMPACT_BIND_TYPE, w->project, CBM_NOT_FOUND,
                             SQLITE_STATIC) == SQLITE_OK &&
           impact_check(w);
}

static int impact_open(cbm_store_t *s, cbm_store_read_scope_t *scope,
                       const cbm_impact_policy_t *policy, cbm_impact_walk_t **out) {
    if (out) {
        *out = NULL;
    }
    if (scope) {
        int status = cbm_store_read_scope_check(scope);
        if (status != CBM_STORE_OK) {
            return status;
        }
    }
    sqlite3 *db = cbm_store_get_db(s);
    if (!db || !out || !policy || !policy->project || !policy->project[0] || !policy->edge_types ||
        policy->edge_type_count <= 0) {
        return scope ? cbm_store_read_scope_fail(scope, CBM_STORE_ERR) : CBM_STORE_ERR;
    }
    cbm_impact_walk_t *w = (cbm_impact_walk_t *)cbm_calloc(CBM_MEM_CLASS_STORE, sizeof(*w));
    if (!w) {
        return scope ? cbm_store_read_scope_fail(scope, CBM_STORE_ERR) : CBM_STORE_ERR;
    }
    w->db = db;
    w->scope = scope;
    if (!impact_copy_policy(w, policy) || !impact_prepare(w) || !impact_check(w)) {
        int status = impact_fail(w, CBM_STORE_ERR);
        cbm_impact_walk_close(w);
        return status;
    }
    *out = w;
    return CBM_STORE_OK;
}

int cbm_impact_walk_open(cbm_store_t *s, const cbm_impact_policy_t *policy,
                         cbm_impact_walk_t **out) {
    return impact_open(s, NULL, policy, out);
}

int cbm_impact_walk_open_scoped(cbm_store_read_scope_t *scope, const cbm_impact_policy_t *policy,
                                cbm_impact_walk_t **out) {
    if (!scope) {
        if (out) {
            *out = NULL;
        }
        return CBM_STORE_ERR;
    }
    return impact_open(cbm_store_read_scope_store(scope), scope, policy, out);
}

/* True when `id` names a node of the walk's project. */
static bool impact_node_exists(cbm_impact_walk_t *w, int64_t id) {
    if (!impact_check(w) || !impact_known_id(w, id) || sqlite3_reset(w->stmt_node) != SQLITE_OK ||
        sqlite3_bind_int64(w->stmt_node, IMPACT_BIND_ID, id) != SQLITE_OK) {
        return false;
    }
    int rc = sqlite3_step(w->stmt_node);
    int reset = sqlite3_reset(w->stmt_node);
    return rc == SQLITE_ROW && reset == SQLITE_OK && impact_check(w);
}

int cbm_impact_walk_add_sinks(cbm_impact_walk_t *w, const int64_t *ids, int count) {
    if (!impact_check(w) || count < 0 || (count > 0 && !ids)) {
        return impact_fail(w, CBM_STORE_ERR);
    }
    for (int i = 0; i < count; i++) {
        if (!impact_node_exists(w, ids[i])) {
            return impact_fail(w, CBM_STORE_ERR);
        }
    }
    for (int i = 0; i < count; i++) {
        if (!impact_check(w)) {
            return impact_fail(w, CBM_STORE_ERR);
        }
        w->sink[ids[i]] = SKIP_ONE;
    }
    return impact_check(w) ? CBM_STORE_OK : impact_fail(w, CBM_STORE_ERR);
}

static bool impact_canonical_order(cbm_impact_walk_t *w) {
    if (!impact_sort(w, w->hits, w->count, sizeof(*w->hits), true)) {
        return false;
    }
    for (int i = 0; i < w->count; i++) {
        if (!impact_check(w)) {
            return false;
        }
        w->slot[w->hits[i].id] = i + SKIP_ONE;
    }
    return impact_check(w);
}

int cbm_impact_walk_run(cbm_impact_walk_t *w, const int64_t *seeds, int count) {
    if (!impact_check(w) || count < 0 || (count > 0 && !seeds) ||
        (w->count > 0 && w->max_hop == INT_MAX)) {
        return impact_fail(w, CBM_STORE_ERR);
    }
    for (int i = 0; i < count; i++) {
        if (!impact_check(w) || !impact_known_id(w, seeds[i])) {
            return impact_fail(w, CBM_STORE_ERR);
        }
    }
    impact_frontier_list_t frontier = {0};
    impact_frontier_list_t next = {0};
    int base_hop = w->count > 0 ? w->max_hop + SKIP_ONE : 0;
    int level = 0; /* hops walked from the seeds of this run */
    bool ok = true;
    for (int i = 0; i < count && ok; i++) {
        if (!impact_check(w)) {
            ok = false;
            break;
        }
        if (w->slot[seeds[i]] != 0) {
            continue; /* reached already, or the same seed twice */
        }
        ok = impact_add_hit(w, &frontier, seeds[i], base_hop, 0, NULL) == IMPACT_ADD_OK;
    }
    while (ok && frontier.count > 0 && (w->max_hops == 0 || level < w->max_hops)) {
        if (level == INT_MAX ||
            !impact_sort(w, frontier.items, frontier.count, sizeof(*frontier.items), false)) {
            ok = false;
            break;
        }
        level++;
        next.count = 0;
        ok = impact_expand_level(w, &frontier, &next);
        impact_frontier_list_t done = frontier;
        frontier = next;
        next = done;
    }
    cbm_free(CBM_MEM_CLASS_STORE, frontier.items);
    cbm_free(CBM_MEM_CLASS_STORE, next.items);
    sqlite3_reset(w->stmt_node);
    sqlite3_reset(w->stmt_in);
    sqlite3_reset(w->stmt_out);
    if (!ok || !impact_canonical_order(w) || !impact_check(w)) {
        return impact_fail(w, CBM_STORE_ERR);
    }
    return CBM_STORE_OK;
}

int cbm_impact_walk_count(const cbm_impact_walk_t *w) {
    return impact_visible(w) ? w->count : 0;
}

const cbm_impact_hit_t *cbm_impact_walk_hits(const cbm_impact_walk_t *w) {
    return impact_visible(w) ? w->hits : NULL;
}

bool cbm_impact_walk_reached(const cbm_impact_walk_t *w, int64_t id) {
    return impact_visible(w) && impact_known_id(w, id) && w->slot[id] != 0;
}

const cbm_impact_hit_t *cbm_impact_walk_hit(const cbm_impact_walk_t *w, int64_t id) {
    return cbm_impact_walk_reached(w, id) ? &w->hits[w->slot[id] - SKIP_ONE] : NULL;
}
