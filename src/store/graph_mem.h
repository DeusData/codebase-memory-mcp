/*
 * graph_mem.h — read-only in-memory adjacency over a published store's edges.
 *
 * A graph walk on a kernel-scale index (8.5M nodes, 16.9M edges) spends its
 * time in SQLite B-tree seeks: trace_path's recursive CTE took 6.6 s and a
 * degree-filtered search_graph 3.9 s, where the same questions on django take
 * 20-100 ms. This layer reads the edges ONCE, in one sequential scan of the
 * covering UNIQUE(source_id, target_id, type, ...) index, into a typed CSR
 * (node id -> out/in neighbours, each with its interned edge type) and answers
 * those walks from RAM. Node rows still come from SQLite; only the edge
 * structure is held.
 *
 * Identity: a layer is keyed by (db path, version), where the caller promises
 * that one version names one edge set. store.c builds the version from the
 * store generation (every publish stamps a new one) plus the rollback-journal
 * file state (the header change counter every commit bumps — it also catches
 * writers that mutate a published DB in place without a new generation), and
 * admits only read-only query stores of a sealed, non-legacy DB.
 *
 * Lifetime: the daemon closes its query stores after every request, so layers
 * live in a process-wide registry, shared by refcount across request threads.
 * Every byte is allocated through the memory core (CBM_MEM_CLASS_GRAPH_CACHE)
 * and the registry is bounded: the layers together stay under a quarter of
 * the memory budget, unheld ones are evicted oldest-first to make room, and a
 * layer that still would not fit (or that would push the process over its
 * budget) is not built — the caller keeps the SQL path. A build is
 * all-or-nothing: a failed or refused build leaves no partial layer behind.
 * A layer nobody held for CBM_GRAPH_MEM_IDLE_EVICT_MS is evicted on the next
 * request (no timer thread), and the daemon subtracts the resident bytes from
 * what it hands index workers (cbm_graph_mem_cached_bytes).
 */
#ifndef CBM_GRAPH_MEM_H
#define CBM_GRAPH_MEM_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

struct sqlite3;

typedef struct cbm_graph_mem cbm_graph_mem_t;

/* A set of interned edge types: bit i = the layer's type i. */
typedef uint64_t cbm_graph_mem_types_t;

/* One reached node: its id and its shortest distance from the start. */
typedef struct {
    int64_t id;
    int hop;
} cbm_graph_mem_hop_t;

/* The layer for (db_path, version): the cached one, or one built now from
 * `db`'s edges table. The caller must hold one read transaction across the
 * version read and this call, so the scan sees that version's edges.
 * NULL means "use the SQL path": no budget, over the cap, another thread is
 * building this key right now, a schema or id range the layer cannot hold, or
 * a failed scan. A non-NULL layer is held until cbm_graph_mem_release. */
cbm_graph_mem_t *cbm_graph_mem_acquire(struct sqlite3 *db, const char *db_path,
                                       const char *version);
void cbm_graph_mem_release(cbm_graph_mem_t *g);

/* The type set naming `types` (exact byte match, like SQL `type IN (...)`).
 * NULL entries and types absent from the graph contribute nothing. */
cbm_graph_mem_types_t cbm_graph_mem_type_mask(const cbm_graph_mem_t *g, const char *const *types,
                                              int count);

/* Shortest-walk BFS with the exact semantics of store_bfs's recursive CTE:
 * every node reached by a walk of 1..max_depth edges whose types are in
 * `mask`, at its MINIMAL such length. The start itself appears only when a
 * cycle leads back to it (at the cycle's length). Rows come sorted by
 * (hop, id); ids absent from the nodes table are NOT filtered here.
 * Returns false only on allocation failure. *out is freed with
 * cbm_graph_mem_hops_free. */
bool cbm_graph_mem_bfs(const cbm_graph_mem_t *g, int64_t start, bool inbound,
                       cbm_graph_mem_types_t mask, int max_depth, cbm_graph_mem_hop_t **out,
                       int *count);
void cbm_graph_mem_hops_free(cbm_graph_mem_hop_t *hops);

/* Number of edges with a type in `mask` ending at (inbound) or leaving
 * (outbound) node `id` — SQL's COUNT(*) over target_id/source_id = id. */
int cbm_graph_mem_degree(const cbm_graph_mem_t *g, int64_t id, bool inbound,
                         cbm_graph_mem_types_t mask);

/* Bytes this layer holds (all through the memory core). */
size_t cbm_graph_mem_bytes(const cbm_graph_mem_t *g);

/* ── Idle eviction ──────────────────────────────────────────────── */

/* How long a layer may sit unheld before it goes. Ten minutes outlasts the
 * gaps between an agent's graph calls within one task (seconds to a few
 * minutes), so a working session keeps its layer, while a session that moved
 * on stops pinning it (237 MB for the kernel). A wrong guess costs one rebuild
 * — about 1.2-1.5 s on the 17 GB kernel index, tens of ms on ordinary repos —
 * against ten minutes of held memory. */
enum { CBM_GRAPH_MEM_IDLE_EVICT_MS = 10 * 60 * 1000 };

/* Evict every layer unheld for CBM_GRAPH_MEM_IDLE_EVICT_MS. The daemon calls
 * it at the end of every request and acquire runs it too: a daemon that stops
 * asking stops paying, with no timer thread. */
void cbm_graph_mem_evict_expired(void);

/* ── Registry introspection (tests, diagnostics) ─────────────────── */

/* Layers currently held by the registry, and their total bytes — every
 * resident layer, held or not, which is what index-worker budgets give up. */
int cbm_graph_mem_cached_count(void);
size_t cbm_graph_mem_cached_bytes(void);
/* Builds completed since process start: a test's proof that a call hit the
 * cache (unchanged) or built (incremented). */
uint64_t cbm_graph_mem_build_count(void);
/* Drop every unheld layer (refs == 0), however recently used. */
void cbm_graph_mem_drop_idle(void);

#ifdef CBM_ENABLE_TEST_SEAMS
/* The clock idle eviction reads; NULL restores cbm_now_ms. Tests move time
 * instead of waiting for it. */
void cbm_graph_mem_set_clock_for_testing(uint64_t (*now_ms)(void));
/* Pin cbm_graph_mem_cached_bytes to `bytes`; 0 restores the live reading. */
void cbm_graph_mem_set_cached_bytes_for_testing(size_t bytes);
#endif

#endif /* CBM_GRAPH_MEM_H */
