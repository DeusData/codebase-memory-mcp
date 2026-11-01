/*
 * graph_mem.c — read-only in-memory adjacency over a published store's edges.
 * See graph_mem.h for the identity, lifetime and budget contract.
 */
#include "store/graph_mem.h"
#include "foundation/constants.h"
#include "foundation/log.h"
#include "foundation/mem.h"
#include "foundation/mem_core.h"
#include "foundation/platform.h"

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    GM_MAX_TYPES = 64,   /* one bit each in cbm_graph_mem_types_t */
    GM_VERSION_MAX = 96, /* generation + file state, see store_graph_mem */
    GM_CAP_DIVISOR = 4,  /* all layers together: at most budget / 4 */
    GM_SET_MIN_CAP = 64, /* BFS visited set: initial slots (power of two) */
    GM_SET_LOAD_NUM = 7, /* grow the visited set past 7/10 load */
    GM_SET_LOAD_DEN = 10,
    GM_GROWTH = 2,
    GM_COL_TARGET = 1,
    GM_COL_TYPE = 2,
};

/* Ids and edge counts must fit the uint32 CSR with room for "+1" offsets. */
#define GM_ID_LIMIT ((int64_t)INT32_MAX - 1)
#define GM_HASH_MUL 2654435761U

struct cbm_graph_mem {
    /* Registry state, guarded by the registry mutex. */
    struct cbm_graph_mem *next;
    char *path;
    char version[GM_VERSION_MAX];
    int refs;
    bool ready;    /* built; false while its builder runs */
    bool detached; /* unlinked from the registry; freed at the last release */
    uint64_t last_use;
    uint64_t idle_since_ms; /* when the last reference went away */
    size_t bytes;           /* reserved estimate while building, actual once ready */

    /* The graph, immutable once ready. Offsets cover node ids 0..n_ids-1:
     * the out-edges of id u are out_nbr/out_type[out_off[u] .. out_off[u+1]). */
    uint32_t n_ids;
    uint32_t n_edges;
    uint32_t *out_off;
    uint32_t *out_nbr; /* target of each out-edge */
    uint8_t *out_type;
    uint32_t *in_off;
    uint32_t *in_nbr; /* source of each in-edge */
    uint8_t *in_type;
    int type_count;
    char *types[GM_MAX_TYPES];
    size_t type_len[GM_MAX_TYPES];
};

static cbm_graph_mem_t *g_layers; /* linked (not detached) layers */
static size_t g_total_bytes;      /* every live layer, linked or detached */
static uint64_t g_use_clock;      /* logical LRU clock */
static uint64_t g_builds;

/* A static SQLite mutex needs no init and works on every platform SQLite
 * builds on (SQLITE_THREADSAFE=1). store.c owns APP1. */
static sqlite3_mutex *gm_registry_mutex(void) {
    return sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_APP2);
}

#ifdef CBM_ENABLE_TEST_SEAMS
static uint64_t (*g_clock_seam)(void);
static size_t g_cached_bytes_seam;
void cbm_graph_mem_set_clock_for_testing(uint64_t (*now_ms)(void)) {
    g_clock_seam = now_ms;
}
void cbm_graph_mem_set_cached_bytes_for_testing(size_t bytes) {
    g_cached_bytes_seam = bytes;
}
static uint64_t gm_now_ms(void) {
    return g_clock_seam ? g_clock_seam() : cbm_now_ms();
}
#else
static uint64_t gm_now_ms(void) {
    return cbm_now_ms();
}
#endif

static void *gm_alloc(size_t bytes) {
    return cbm_alloc(CBM_MEM_CLASS_GRAPH_CACHE, bytes ? bytes : 1);
}

static void gm_free_ptr(void *p) {
    cbm_free(CBM_MEM_CLASS_GRAPH_CACHE, p);
}

static void gm_free_graph(cbm_graph_mem_t *g) {
    gm_free_ptr(g->out_off);
    gm_free_ptr(g->out_nbr);
    gm_free_ptr(g->out_type);
    gm_free_ptr(g->in_off);
    gm_free_ptr(g->in_nbr);
    gm_free_ptr(g->in_type);
    g->out_off = g->out_nbr = g->in_off = g->in_nbr = NULL;
    g->out_type = g->in_type = NULL;
    for (int i = 0; i < g->type_count; i++) {
        gm_free_ptr(g->types[i]);
        g->types[i] = NULL;
    }
    g->type_count = 0;
}

static void gm_destroy(cbm_graph_mem_t *g) {
    gm_free_graph(g);
    gm_free_ptr(g->path);
    gm_free_ptr(g);
}

/* ── Registry (caller holds the mutex) ───────────────────────────── */

static cbm_graph_mem_t *gm_find(const char *path, const char *version) {
    for (cbm_graph_mem_t *g = g_layers; g; g = g->next) {
        if (strcmp(g->path, path) == 0 && strcmp(g->version, version) == 0) {
            return g;
        }
    }
    return NULL;
}

static void gm_unlink(cbm_graph_mem_t *g) {
    for (cbm_graph_mem_t **p = &g_layers; *p; p = &(*p)->next) {
        if (*p == g) {
            *p = g->next;
            g->next = NULL;
            g->detached = true;
            return;
        }
    }
}

/* Unlink a layer; free it now when nobody holds it, else at its last release. */
static void gm_retire(cbm_graph_mem_t *g) {
    gm_unlink(g);
    if (g->refs == 0) {
        g_total_bytes -= g->bytes;
        gm_destroy(g);
    }
}

/* Older versions of a path are never asked for again by a new store (one
 * opened later reads the current version), so they go now. */
static void gm_retire_other_versions(const char *path, const char *version) {
    cbm_graph_mem_t *g = g_layers;
    while (g) {
        cbm_graph_mem_t *next = g->next;
        if (strcmp(g->path, path) == 0 && strcmp(g->version, version) != 0) {
            gm_retire(g);
        }
        g = next;
    }
}

/* Make room for `bytes` under the cap by evicting idle layers, oldest first.
 * False when it cannot fit: the cap is unset (no budget) or every remaining
 * byte is held by a request. */
static bool gm_make_room(size_t bytes) {
    size_t budget = cbm_mem_budget();
    size_t cap = budget / GM_CAP_DIVISOR;
    if (cap == 0 || bytes > cap) {
        return false;
    }
    while (g_total_bytes + bytes > cap) {
        cbm_graph_mem_t *victim = NULL;
        for (cbm_graph_mem_t *g = g_layers; g; g = g->next) {
            if (g->ready && g->refs == 0 && (!victim || g->last_use < victim->last_use)) {
                victim = g;
            }
        }
        if (!victim) {
            return false;
        }
        gm_retire(victim);
    }
    /* The cap bounds the layers; the process as a whole must still fit. */
    return cbm_mem_charged() + bytes <= budget;
}

/* Retire every layer no request has held for CBM_GRAPH_MEM_IDLE_EVICT_MS.
 * Returns the bytes given back. */
static size_t gm_evict_expired_locked(uint64_t now) {
    size_t freed = 0;
    cbm_graph_mem_t *g = g_layers;
    while (g) {
        cbm_graph_mem_t *next = g->next;
        if (g->ready && g->refs == 0 && now >= g->idle_since_ms &&
            now - g->idle_since_ms >= CBM_GRAPH_MEM_IDLE_EVICT_MS) {
            freed += g->bytes;
            gm_retire(g);
        }
        g = next;
    }
    return freed;
}

static void gm_log_evicted(size_t freed) {
    if (freed > 0) {
        char bytes[CBM_SZ_32];
        (void)snprintf(bytes, sizeof(bytes), "%zu", freed);
        cbm_log_info("graph_mem.evict", "reason", "idle", "bytes", bytes);
    }
}

/* ── Build ──────────────────────────────────────────────────────── */

static bool gm_bounds(sqlite3 *db, int64_t *max_node, int64_t *max_edge) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT (SELECT max(id) FROM nodes), (SELECT max(id) FROM edges);",
                           -1, &st, NULL) != SQLITE_OK) {
        sqlite3_finalize(st);
        return false;
    }
    bool ok = sqlite3_step(st) == SQLITE_ROW;
    if (ok) {
        *max_node = sqlite3_column_int64(st, 0);
        *max_edge = sqlite3_column_int64(st, 1);
    }
    sqlite3_finalize(st);
    return ok && *max_node >= 0 && *max_edge >= 0 && *max_node < GM_ID_LIMIT &&
           *max_edge < GM_ID_LIMIT;
}

/* Upper bound of a layer's bytes: edge ids are unique positive rowids, so the
 * edge count never exceeds max(edges.id). */
static size_t gm_estimate(int64_t max_node, int64_t max_edge) {
    size_t offsets = ((size_t)max_node + 2) * sizeof(uint32_t);
    size_t per_edge = sizeof(uint32_t) + sizeof(uint8_t);
    return GM_GROWTH * offsets + GM_GROWTH * (size_t)max_edge * per_edge;
}

static int gm_intern(cbm_graph_mem_t *g, const unsigned char *text, size_t len, int *last) {
    if (*last >= 0 && g->type_len[*last] == len && memcmp(g->types[*last], text, len) == 0) {
        return *last;
    }
    for (int i = 0; i < g->type_count; i++) {
        if (g->type_len[i] == len && memcmp(g->types[i], text, len) == 0) {
            *last = i;
            return i;
        }
    }
    if (g->type_count >= GM_MAX_TYPES) {
        return -1;
    }
    char *copy = gm_alloc(len + 1);
    if (!copy) {
        return -1;
    }
    memcpy(copy, text, len);
    copy[len] = '\0';
    g->types[g->type_count] = copy;
    g->type_len[g->type_count] = len;
    *last = g->type_count;
    return g->type_count++;
}

static bool gm_grow_u32(uint32_t **arr, size_t *cap, size_t need) {
    if (need <= *cap) {
        return true;
    }
    size_t next = *cap ? *cap : 1;
    while (next < need) {
        next *= GM_GROWTH;
    }
    uint32_t *grown = cbm_realloc(CBM_MEM_CLASS_GRAPH_CACHE, *arr, next * sizeof(uint32_t));
    if (!grown) {
        return false;
    }
    *arr = grown;
    *cap = next;
    return true;
}

/* The out-CSR, filled straight from the scan: rows arrive ordered by
 * source_id, so each node's list is contiguous and its start offset is the
 * edge count when its first row (or a later id's) shows up. */
typedef struct {
    size_t off_cap;
    size_t edge_cap;
    uint32_t next_start; /* smallest id whose start offset is not set yet */
    uint32_t max_id;     /* largest endpoint seen */
    int64_t prev_src;
    int last_type;
} gm_scan_t;

static bool gm_scan_row(cbm_graph_mem_t *g, gm_scan_t *sc, sqlite3_stmt *st) {
    int64_t src = sqlite3_column_int64(st, 0);
    int64_t dst = sqlite3_column_int64(st, GM_COL_TARGET);
    const unsigned char *type = sqlite3_column_text(st, GM_COL_TYPE);
    if (src < 0 || dst < 0 || src >= GM_ID_LIMIT || dst >= GM_ID_LIMIT || src < sc->prev_src ||
        !type || g->n_edges >= (uint32_t)GM_ID_LIMIT) {
        return false;
    }
    int type_id = gm_intern(g, type, (size_t)sqlite3_column_bytes(st, GM_COL_TYPE), &sc->last_type);
    if (type_id < 0 || !gm_grow_u32(&g->out_off, &sc->off_cap, (size_t)src + GM_GROWTH)) {
        return false;
    }
    while (sc->next_start <= (uint32_t)src) {
        g->out_off[sc->next_start++] = g->n_edges;
    }
    if (g->n_edges >= sc->edge_cap) {
        size_t next = sc->edge_cap ? sc->edge_cap * GM_GROWTH : 1;
        uint32_t *nbr = cbm_realloc(CBM_MEM_CLASS_GRAPH_CACHE, g->out_nbr, next * sizeof(uint32_t));
        if (!nbr) {
            return false;
        }
        g->out_nbr = nbr;
        uint8_t *types = cbm_realloc(CBM_MEM_CLASS_GRAPH_CACHE, g->out_type, next);
        if (!types) {
            return false;
        }
        g->out_type = types;
        sc->edge_cap = next;
    }
    g->out_nbr[g->n_edges] = (uint32_t)dst;
    g->out_type[g->n_edges] = (uint8_t)type_id;
    g->n_edges++;
    sc->prev_src = src;
    if ((uint32_t)src > sc->max_id) {
        sc->max_id = (uint32_t)src;
    }
    if ((uint32_t)dst > sc->max_id) {
        sc->max_id = (uint32_t)dst;
    }
    return true;
}

/* The in-CSR from the out-CSR: count, prefix-sum, then place each edge using
 * in_off[v] as v's cursor, which leaves in_off shifted by one id — shift it
 * back. Iterating sources in id order keeps every in-list sorted by source. */
static bool gm_build_inbound(cbm_graph_mem_t *g) {
    size_t n_ids = g->n_ids;
    g->in_off = cbm_calloc(CBM_MEM_CLASS_GRAPH_CACHE, (n_ids + 1) * sizeof(uint32_t));
    g->in_nbr = gm_alloc((size_t)g->n_edges * sizeof(uint32_t));
    g->in_type = gm_alloc(g->n_edges);
    if (!g->in_off || !g->in_nbr || !g->in_type) {
        return false;
    }
    for (uint32_t e = 0; e < g->n_edges; e++) {
        g->in_off[g->out_nbr[e] + 1]++;
    }
    for (size_t v = 0; v < n_ids; v++) {
        g->in_off[v + 1] += g->in_off[v];
    }
    for (uint32_t u = 0; u < g->n_ids; u++) {
        for (uint32_t e = g->out_off[u]; e < g->out_off[u + 1]; e++) {
            uint32_t pos = g->in_off[g->out_nbr[e]]++;
            g->in_nbr[pos] = u;
            g->in_type[pos] = g->out_type[e];
        }
    }
    for (size_t v = n_ids; v > 0; v--) {
        g->in_off[v] = g->in_off[v - 1];
    }
    g->in_off[0] = 0;
    return true;
}

static bool gm_build(cbm_graph_mem_t *g, sqlite3 *db, int64_t max_node, int64_t max_edge) {
    gm_scan_t sc = {.prev_src = 0, .last_type = -1, .max_id = (uint32_t)max_node};
    sc.edge_cap = (size_t)max_edge;
    g->out_nbr = gm_alloc(sc.edge_cap * sizeof(uint32_t));
    g->out_type = gm_alloc(sc.edge_cap);
    if (!g->out_nbr || !g->out_type ||
        !gm_grow_u32(&g->out_off, &sc.off_cap, (size_t)max_node + GM_GROWTH)) {
        return false;
    }
    /* ORDER BY the covering UNIQUE index's leading columns: a sequential walk
     * of sqlite_autoindex_edges_1, no sort and no table access. */
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db,
                           "SELECT source_id, target_id, type FROM edges "
                           "ORDER BY source_id, target_id, type;",
                           -1, &st, NULL) != SQLITE_OK) {
        sqlite3_finalize(st);
        return false;
    }
    int rc;
    bool ok = true;
    while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
        if (!gm_scan_row(g, &sc, st)) {
            ok = false;
            break;
        }
    }
    sqlite3_finalize(st);
    if (!ok || rc != SQLITE_DONE) {
        return false;
    }
    g->n_ids = sc.max_id + 1;
    if (!gm_grow_u32(&g->out_off, &sc.off_cap, (size_t)g->n_ids + 1)) {
        return false;
    }
    while (sc.next_start <= g->n_ids) {
        g->out_off[sc.next_start++] = g->n_edges;
    }
    /* max(edges.id) over-counts after deletions; give the slack back. */
    if (g->n_edges < sc.edge_cap) {
        size_t keep = g->n_edges ? g->n_edges : 1;
        uint32_t *nbr = cbm_realloc(CBM_MEM_CLASS_GRAPH_CACHE, g->out_nbr, keep * sizeof(uint32_t));
        uint8_t *types = nbr ? cbm_realloc(CBM_MEM_CLASS_GRAPH_CACHE, g->out_type, keep) : NULL;
        if (nbr) {
            g->out_nbr = nbr;
        }
        if (types) {
            g->out_type = types;
        }
    }
    return gm_build_inbound(g);
}

static size_t gm_actual_bytes(const cbm_graph_mem_t *g) {
    size_t offsets = ((size_t)g->n_ids + 1) * sizeof(uint32_t);
    size_t per_edge = sizeof(uint32_t) + sizeof(uint8_t);
    size_t bytes = GM_GROWTH * offsets + GM_GROWTH * (size_t)g->n_edges * per_edge;
    for (int i = 0; i < g->type_count; i++) {
        bytes += g->type_len[i] + 1;
    }
    return bytes + sizeof(*g) + strlen(g->path) + 1;
}

/* ── Acquire / release ──────────────────────────────────────────── */

/* A ready hit takes a reference; a hit still being built answers NULL (the
 * caller uses SQL rather than waiting on another thread's build). */
static cbm_graph_mem_t *gm_take(cbm_graph_mem_t *hit) {
    if (!hit->ready) {
        return NULL;
    }
    hit->refs++;
    hit->last_use = ++g_use_clock;
    return hit;
}

static cbm_graph_mem_t *gm_new_entry(const char *db_path, const char *version, size_t est) {
    cbm_graph_mem_t *g = cbm_calloc(CBM_MEM_CLASS_GRAPH_CACHE, sizeof(*g));
    if (!g) {
        return NULL;
    }
    g->path = cbm_mem_strdup(CBM_MEM_CLASS_GRAPH_CACHE, db_path);
    if (!g->path) {
        gm_free_ptr(g);
        return NULL;
    }
    memcpy(g->version, version, strlen(version) + 1);
    g->refs = 1;
    g->bytes = est;
    return g;
}

cbm_graph_mem_t *cbm_graph_mem_acquire(sqlite3 *db, const char *db_path, const char *version) {
    if (!db || !db_path || !version || !version[0] || strlen(version) >= GM_VERSION_MAX) {
        return NULL;
    }
    uint64_t now = gm_now_ms();
    sqlite3_mutex *mu = gm_registry_mutex();
    sqlite3_mutex_enter(mu);
    size_t freed = gm_evict_expired_locked(now);
    cbm_graph_mem_t *hit = gm_find(db_path, version);
    cbm_graph_mem_t *taken = hit ? gm_take(hit) : NULL;
    sqlite3_mutex_leave(mu);
    gm_log_evicted(freed);
    if (hit) {
        return taken;
    }

    int64_t max_node = 0;
    int64_t max_edge = 0;
    if (!gm_bounds(db, &max_node, &max_edge)) {
        return NULL;
    }
    size_t est = gm_estimate(max_node, max_edge);

    sqlite3_mutex_enter(mu);
    hit = gm_find(db_path, version);
    if (hit) {
        taken = gm_take(hit);
        sqlite3_mutex_leave(mu);
        return taken;
    }
    gm_retire_other_versions(db_path, version);
    bool fits = gm_make_room(est);
    cbm_graph_mem_t *g = fits ? gm_new_entry(db_path, version, est) : NULL;
    if (g) {
        g->next = g_layers;
        g_layers = g;
        g_total_bytes += est;
    }
    sqlite3_mutex_leave(mu);
    if (!g) {
        if (!fits) {
            char need[CBM_SZ_32];
            (void)snprintf(need, sizeof(need), "%zu", est);
            cbm_log_info("graph_mem.skip", "reason", "budget", "bytes", need);
        }
        return NULL;
    }

    /* The build runs outside the mutex: other keys stay servable, and a
     * request for this key meanwhile takes the SQL path (gm_take). */
    uint64_t started_ms = cbm_now_ms();
    bool ok = gm_build(g, db, max_node, max_edge);
    if (!ok) {
        gm_free_graph(g); /* all-or-nothing: no partial layer survives */
    }
    size_t actual = ok ? gm_actual_bytes(g) : 0;
    if (ok) {
        char edges[CBM_SZ_32];
        char bytes[CBM_SZ_32];
        char elapsed[CBM_SZ_32];
        (void)snprintf(edges, sizeof(edges), "%u", g->n_edges);
        (void)snprintf(bytes, sizeof(bytes), "%zu", actual);
        (void)snprintf(elapsed, sizeof(elapsed), "%llu",
                       (unsigned long long)(cbm_now_ms() - started_ms));
        cbm_log_info("graph_mem.build", "edges", edges, "bytes", bytes, "elapsed_ms", elapsed);
    }

    sqlite3_mutex_enter(mu);
    g_total_bytes = g_total_bytes - g->bytes + actual;
    g->bytes = actual;
    if (ok) {
        g->ready = true;
        g->last_use = ++g_use_clock;
        g_builds++;
    } else {
        g->refs = 0;
        gm_retire(g);
        g = NULL;
    }
    sqlite3_mutex_leave(mu);
    return g;
}

void cbm_graph_mem_release(cbm_graph_mem_t *g) {
    if (!g) {
        return;
    }
    uint64_t now = gm_now_ms();
    sqlite3_mutex *mu = gm_registry_mutex();
    sqlite3_mutex_enter(mu);
    g->refs--;
    if (g->refs == 0) {
        g->idle_since_ms = now;
    }
    if (g->refs == 0 && g->detached) {
        g_total_bytes -= g->bytes;
        gm_destroy(g);
    }
    sqlite3_mutex_leave(mu);
}

void cbm_graph_mem_evict_expired(void) {
    uint64_t now = gm_now_ms();
    sqlite3_mutex *mu = gm_registry_mutex();
    sqlite3_mutex_enter(mu);
    size_t freed = gm_evict_expired_locked(now);
    sqlite3_mutex_leave(mu);
    gm_log_evicted(freed);
}

/* ── Queries ────────────────────────────────────────────────────── */

cbm_graph_mem_types_t cbm_graph_mem_type_mask(const cbm_graph_mem_t *g, const char *const *types,
                                              int count) {
    cbm_graph_mem_types_t mask = 0;
    for (int i = 0; g && types && i < count; i++) {
        if (!types[i]) {
            continue;
        }
        size_t len = strlen(types[i]);
        for (int t = 0; t < g->type_count; t++) {
            if (g->type_len[t] == len && memcmp(g->types[t], types[i], len) == 0) {
                mask |= (cbm_graph_mem_types_t)1 << t;
                break;
            }
        }
    }
    return mask;
}

int cbm_graph_mem_degree(const cbm_graph_mem_t *g, int64_t id, bool inbound,
                         cbm_graph_mem_types_t mask) {
    if (!g || id < 0 || id >= (int64_t)g->n_ids) {
        return 0;
    }
    const uint32_t *off = inbound ? g->in_off : g->out_off;
    const uint8_t *type = inbound ? g->in_type : g->out_type;
    int n = 0;
    for (uint32_t e = off[id]; e < off[id + 1]; e++) {
        n += (int)((mask >> type[e]) & 1U);
    }
    return n;
}

size_t cbm_graph_mem_bytes(const cbm_graph_mem_t *g) {
    return g ? g->bytes : 0;
}

/* Visited set for one BFS: open addressing over id + 1 (0 marks a free slot). */
typedef struct {
    uint32_t *slots;
    size_t cap;
    size_t count;
} gm_set_t;

static size_t gm_set_home(uint32_t id, size_t mask) {
    return (size_t)(id * GM_HASH_MUL) & mask;
}

static bool gm_set_grow(gm_set_t *s) {
    size_t cap = s->cap ? s->cap * GM_GROWTH : GM_SET_MIN_CAP;
    uint32_t *slots = cbm_calloc(CBM_MEM_CLASS_GRAPH_CACHE, cap * sizeof(uint32_t));
    if (!slots) {
        return false;
    }
    for (size_t i = 0; i < s->cap; i++) {
        uint32_t key = s->slots[i];
        if (key) {
            size_t j = gm_set_home(key - 1, cap - 1);
            while (slots[j]) {
                j = (j + 1) & (cap - 1);
            }
            slots[j] = key;
        }
    }
    gm_free_ptr(s->slots);
    s->slots = slots;
    s->cap = cap;
    return true;
}

/* 1 = added, 0 = already present, -1 = out of memory. */
static int gm_set_add(gm_set_t *s, uint32_t id) {
    if ((s->count + 1) * GM_SET_LOAD_DEN > s->cap * GM_SET_LOAD_NUM && !gm_set_grow(s)) {
        return -1;
    }
    size_t mask = s->cap - 1;
    for (size_t i = gm_set_home(id, mask);; i = (i + 1) & mask) {
        if (s->slots[i] == 0) {
            s->slots[i] = id + 1;
            s->count++;
            return 1;
        }
        if (s->slots[i] == id + 1) {
            return 0;
        }
    }
}

typedef struct {
    uint32_t *ids;
    size_t count;
    size_t cap;
} gm_list_t;

static bool gm_list_push(gm_list_t *l, uint32_t id) {
    if (!gm_grow_u32(&l->ids, &l->cap, l->count + 1)) {
        return false;
    }
    l->ids[l->count++] = id;
    return true;
}

typedef struct {
    cbm_graph_mem_hop_t *rows;
    size_t count;
    size_t cap;
} gm_hops_t;

static bool gm_hops_push(gm_hops_t *h, uint32_t id, int hop) {
    if (h->count == h->cap) {
        size_t next = h->cap ? h->cap * GM_GROWTH : GM_SET_MIN_CAP;
        cbm_graph_mem_hop_t *rows =
            cbm_realloc(CBM_MEM_CLASS_GRAPH_CACHE, h->rows, next * sizeof(cbm_graph_mem_hop_t));
        if (!rows) {
            return false;
        }
        h->rows = rows;
        h->cap = next;
    }
    h->rows[h->count].id = id;
    h->rows[h->count].hop = hop;
    h->count++;
    return true;
}

static int gm_hop_id_cmp(const void *pa, const void *pb) {
    const cbm_graph_mem_hop_t *a = pa;
    const cbm_graph_mem_hop_t *b = pb;
    return (a->id > b->id) - (a->id < b->id);
}

/* One BFS level: expand every frontier node over the masked edges. A node is
 * first reached at its shortest distance; the start is never re-entered (its
 * neighbours were all reached at hop 1), only recorded once at the length of
 * the shortest cycle back to it — the CTE's (start, hop>0) row. */
typedef struct {
    const uint32_t *off;
    const uint32_t *nbr;
    const uint8_t *type;
    cbm_graph_mem_types_t mask;
    uint32_t start;
    bool start_back;
    gm_set_t seen;
    gm_hops_t out;
} gm_walk_t;

static bool gm_expand_level(gm_walk_t *w, const gm_list_t *frontier, gm_list_t *next, int hop) {
    for (size_t f = 0; f < frontier->count; f++) {
        uint32_t u = frontier->ids[f];
        for (uint32_t e = w->off[u]; e < w->off[u + 1]; e++) {
            if (((w->mask >> w->type[e]) & 1U) == 0) {
                continue;
            }
            uint32_t v = w->nbr[e];
            if (v == w->start) {
                if (!w->start_back) {
                    w->start_back = true;
                    if (!gm_hops_push(&w->out, v, hop)) {
                        return false;
                    }
                }
                continue;
            }
            int added = gm_set_add(&w->seen, v);
            if (added < 0) {
                return false;
            }
            if (added > 0 && (!gm_hops_push(&w->out, v, hop) || !gm_list_push(next, v))) {
                return false;
            }
        }
    }
    return true;
}

bool cbm_graph_mem_bfs(const cbm_graph_mem_t *g, int64_t start, bool inbound,
                       cbm_graph_mem_types_t mask, int max_depth, cbm_graph_mem_hop_t **out,
                       int *count) {
    *out = NULL;
    *count = 0;
    if (!g || max_depth < 1 || mask == 0 || start < 0 || start >= (int64_t)g->n_ids) {
        return true; /* nothing reachable: the CTE yields only the anchor */
    }
    gm_walk_t w = {
        .off = inbound ? g->in_off : g->out_off,
        .nbr = inbound ? g->in_nbr : g->out_nbr,
        .type = inbound ? g->in_type : g->out_type,
        .mask = mask,
        .start = (uint32_t)start,
    };
    gm_list_t frontier = {0};
    gm_list_t next = {0};
    bool ok = gm_list_push(&frontier, w.start);
    for (int hop = 1; ok && hop <= max_depth && frontier.count > 0; hop++) {
        size_t level_begin = w.out.count;
        next.count = 0;
        ok = gm_expand_level(&w, &frontier, &next, hop);
        if (ok && w.out.count - level_begin > 1) {
            qsort(w.out.rows + level_begin, w.out.count - level_begin, sizeof(cbm_graph_mem_hop_t),
                  gm_hop_id_cmp);
        }
        gm_list_t swap = frontier;
        frontier = next;
        next = swap;
    }
    gm_free_ptr(frontier.ids);
    gm_free_ptr(next.ids);
    gm_free_ptr(w.seen.slots);
    if (!ok || w.out.count > (size_t)INT32_MAX) {
        gm_free_ptr(w.out.rows);
        return false;
    }
    *out = w.out.rows;
    *count = (int)w.out.count;
    return true;
}

void cbm_graph_mem_hops_free(cbm_graph_mem_hop_t *hops) {
    gm_free_ptr(hops);
}

/* ── Registry introspection ─────────────────────────────────────── */

int cbm_graph_mem_cached_count(void) {
    sqlite3_mutex *mu = gm_registry_mutex();
    sqlite3_mutex_enter(mu);
    int n = 0;
    for (cbm_graph_mem_t *g = g_layers; g; g = g->next) {
        n++;
    }
    sqlite3_mutex_leave(mu);
    return n;
}

size_t cbm_graph_mem_cached_bytes(void) {
#ifdef CBM_ENABLE_TEST_SEAMS
    if (g_cached_bytes_seam) {
        return g_cached_bytes_seam;
    }
#endif
    sqlite3_mutex *mu = gm_registry_mutex();
    sqlite3_mutex_enter(mu);
    size_t bytes = g_total_bytes;
    sqlite3_mutex_leave(mu);
    return bytes;
}

uint64_t cbm_graph_mem_build_count(void) {
    sqlite3_mutex *mu = gm_registry_mutex();
    sqlite3_mutex_enter(mu);
    uint64_t n = g_builds;
    sqlite3_mutex_leave(mu);
    return n;
}

void cbm_graph_mem_drop_idle(void) {
    sqlite3_mutex *mu = gm_registry_mutex();
    sqlite3_mutex_enter(mu);
    cbm_graph_mem_t *g = g_layers;
    while (g) {
        cbm_graph_mem_t *next = g->next;
        if (g->ready && g->refs == 0) {
            gm_retire(g);
        }
        g = next;
    }
    sqlite3_mutex_leave(mu);
}
