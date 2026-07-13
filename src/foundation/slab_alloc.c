/*
 * slab_alloc.c — Slab allocator for tree-sitter.
 *
 * Replaces malloc/calloc/realloc/free for ALL tree-sitter allocations,
 * eliminating ptmalloc2's per-thread arena fragmentation (the root cause
 * of 321GB VSZ when indexing large codebases with 12 workers).
 *
 * Tier 1 (≤64B): Fixed-size slab free list.
 *   Matches tree-sitter SubtreeHeapData (64 bytes). O(1) alloc/free.
 *   Backed by 64KB slab pages (malloc = mimalloc in production).
 *
 * All allocations >64B go directly to malloc() which is mimalloc
 * in production builds (MI_OVERRIDE=1). This eliminates the complex
 * tier2 bump allocator and its O(n) ownership checks.
 *
 * Cross-thread frees (tree-sitter's allocator callbacks are process-global,
 * so a chunk allocated on parser thread A can be freed on thread B):
 *
 *   - Each slab page is allocated SLAB_PAGE_SIZE-aligned, so the owning page
 *     of ANY chunk is recovered in O(1) by masking the pointer — no per-op
 *     scan and no global hot-path lock. A tiny 3-level radix page map (keyed
 *     by page number, lock-free reads, cold-path writes) tells slab_free
 *     whether a masked base is a real slab page or a plain heap pointer,
 *     without ever dereferencing an unrelated heap address.
 *   - A free by the owning thread returns the chunk to the thread-local free
 *     list (fast path, no atomics on the list). A foreign free is pushed onto
 *     that page's lock-free MPSC remote-free stack; the owner drains it on its
 *     next refill. Neither path takes a global lock.
 *
 * On reclaim/destroy: free pages with no live chunks; retire pages that still
 * have live chunks (owner=NULL, kept reachable) and free them when the final
 * chunk returns. Each page carries a refcount = (handed-out chunks) + (1 owner
 * guard while owned); the thread that drops it to zero frees the page. A page
 * holding a still-live tree-sitter lexer chunk is therefore never freed under
 * it (fixes the cross-thread invalid free and the #852 use-after-free).
 * realloc handles slab-to-heap promotion with minimal copying.
 */
#include "foundation/constants.h"
#include "foundation/slab_alloc.h"
#include "foundation/compat.h"
#include "foundation/compat_thread.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

/* ── Tier 1: Fixed-size slab (≤64B) ──────────────────────────────── */

/* Chunk size matches tree-sitter SubtreeHeapData (64 bytes).
 * All slab allocations are this size regardless of requested size. */
#define SLAB_CHUNK_SIZE 64

/* Each slab page is a 64KB region, allocated aligned to its own size so that
 * masking any interior chunk pointer with SLAB_PAGE_MASK yields the page base
 * (which is where the page header lives). */
#define SLAB_PAGE_SIZE ((size_t)65536)
#define SLAB_PAGE_SHIFT 16 /* log2(SLAB_PAGE_SIZE) */
#define SLAB_PAGE_MASK (~((uintptr_t)SLAB_PAGE_SIZE - 1))

/* Chunk region starts after the page header. SLAB_DATA_OFFSET must be a
 * multiple of SLAB_CHUNK_SIZE (so chunks stay 64-byte aligned) and at least
 * sizeof(slab_page_t). 64 bytes leaves 1023 usable chunks per page. */
#define SLAB_DATA_OFFSET 64
#define SLAB_PAGE_CHUNKS ((SLAB_PAGE_SIZE - SLAB_DATA_OFFSET) / SLAB_CHUNK_SIZE)

/* Free list node — occupies the first 8 bytes of a free chunk. */
typedef struct slab_free_node {
    struct slab_free_node *next;
} slab_free_node_t;

typedef struct slab_state slab_state_t;

/* One slab page. The header lives at the SLAB_PAGE_SIZE-aligned base; the
 * SLAB_PAGE_CHUNKS chunks begin at (char*)page + SLAB_DATA_OFFSET. */
typedef struct slab_page {
    struct slab_page *next;                       /* owner's page list (owner thread only) */
    _Atomic(slab_state_t *) owner;                /* owning TLS state; NULL once retired */
    _Atomic(slab_free_node_t *) remote_free_head; /* lock-free MPSC stack of cross-thread frees */
    atomic_uint refcount; /* handed-out chunks + 1 owner guard (while owned) */
} slab_page_t;

_Static_assert(sizeof(slab_page_t) <= SLAB_DATA_OFFSET,
               "slab_page header must fit in SLAB_DATA_OFFSET");
_Static_assert(SLAB_DATA_OFFSET % SLAB_CHUNK_SIZE == 0, "SLAB_DATA_OFFSET must be chunk-aligned");

/* Per-thread Tier 1 state. */
struct slab_state {
    slab_page_t *pages;         /* linked list of pages owned by this thread */
    slab_free_node_t *freelist; /* singly-linked free list (thread-local) */
    bool installed;
};

static CBM_TLS slab_state_t tls_slab;

/* ── Page map: safe O(1) "is this a slab page?" lookup ───────────────
 *
 * A 3-level radix trie keyed by page number (base >> SLAB_PAGE_SHIFT) covering
 * the low 32 bits of the page number — i.e. all 48-bit addresses that malloc/
 * posix_memalign/_aligned_malloc return on every supported platform. Reads are
 * lock-free (atomic-acquire loads only, never dereferencing the queried
 * pointer). Writes (page create / free / retire — all cold paths) take a
 * dedicated spinlock that is NEVER touched on the alloc/free hot path.
 *
 * Intermediate tables are never freed during the run and stay reachable from
 * the static root, so lock-free readers never touch freed table memory. */
#define SLAB_MAP_L1_BITS 11
#define SLAB_MAP_L2_BITS 11
#define SLAB_MAP_L3_BITS 10
#define SLAB_MAP_TOTAL_BITS (SLAB_MAP_L1_BITS + SLAB_MAP_L2_BITS + SLAB_MAP_L3_BITS) /* 32 */
#define SLAB_MAP_L1_SIZE ((size_t)1 << SLAB_MAP_L1_BITS)
#define SLAB_MAP_L2_SIZE ((size_t)1 << SLAB_MAP_L2_BITS)
#define SLAB_MAP_L3_SIZE ((size_t)1 << SLAB_MAP_L3_BITS)

typedef struct {
    _Atomic(slab_page_t *) e[SLAB_MAP_L3_SIZE];
} slab_map_l3_t;
typedef struct {
    _Atomic(slab_map_l3_t *) e[SLAB_MAP_L2_SIZE];
} slab_map_l2_t;

static _Atomic(slab_map_l2_t *) g_slab_map_root[SLAB_MAP_L1_SIZE];
static atomic_flag g_slab_map_lock = ATOMIC_FLAG_INIT;

static void slab_map_lock(void) {
    while (atomic_flag_test_and_set_explicit(&g_slab_map_lock, memory_order_acquire)) {
        /* Spin — cold path only (page create/destroy/retire). */
    }
}
static void slab_map_unlock(void) {
    atomic_flag_clear_explicit(&g_slab_map_lock, memory_order_release);
}

static bool slab_map_indices(uintptr_t base, size_t *i1, size_t *i2, size_t *i3) {
    uintptr_t pnum = base >> SLAB_PAGE_SHIFT;
    if ((pnum >> SLAB_MAP_TOTAL_BITS) != 0) {
        return false; /* address beyond coverage — never produced by malloc here */
    }
    *i1 = (size_t)((pnum >> (SLAB_MAP_L2_BITS + SLAB_MAP_L3_BITS)) & (SLAB_MAP_L1_SIZE - 1));
    *i2 = (size_t)((pnum >> SLAB_MAP_L3_BITS) & (SLAB_MAP_L2_SIZE - 1));
    *i3 = (size_t)(pnum & (SLAB_MAP_L3_SIZE - 1));
    return true;
}

/* Lock-free. Returns the slab page whose base == (ptr & mask), or NULL if the
 * masked base is not a registered slab page (i.e. ptr is a plain heap block). */
static slab_page_t *slab_map_lookup(uintptr_t base) {
    size_t i1, i2, i3;
    if (!slab_map_indices(base, &i1, &i2, &i3)) {
        return NULL;
    }
    slab_map_l2_t *l2 = atomic_load_explicit(&g_slab_map_root[i1], memory_order_acquire);
    if (!l2) {
        return NULL;
    }
    slab_map_l3_t *l3 = atomic_load_explicit(&l2->e[i2], memory_order_acquire);
    if (!l3) {
        return NULL;
    }
    return atomic_load_explicit(&l3->e[i3], memory_order_acquire);
}

/* Cold path. Sets the leaf entry for `base` to `val` (page to register, NULL to
 * unregister), allocating intermediate tables on demand. Returns false only if
 * a needed table allocation fails while registering. */
static bool slab_map_set(uintptr_t base, slab_page_t *val) {
    size_t i1, i2, i3;
    if (!slab_map_indices(base, &i1, &i2, &i3)) {
        return false;
    }
    slab_map_lock();
    slab_map_l2_t *l2 = atomic_load_explicit(&g_slab_map_root[i1], memory_order_relaxed);
    if (!l2) {
        if (!val) {
            slab_map_unlock();
            return true; /* nothing to unregister */
        }
        l2 = (slab_map_l2_t *)calloc(1, sizeof(*l2));
        if (!l2) {
            slab_map_unlock();
            return false;
        }
        atomic_store_explicit(&g_slab_map_root[i1], l2, memory_order_release);
    }
    slab_map_l3_t *l3 = atomic_load_explicit(&l2->e[i2], memory_order_relaxed);
    if (!l3) {
        if (!val) {
            slab_map_unlock();
            return true;
        }
        l3 = (slab_map_l3_t *)calloc(1, sizeof(*l3));
        if (!l3) {
            slab_map_unlock();
            return false;
        }
        atomic_store_explicit(&l2->e[i2], l3, memory_order_release);
    }
    atomic_store_explicit(&l3->e[i3], val, memory_order_release);
    slab_map_unlock();
    return true;
}

/* ── Tier 1 helpers ────────────────────────────────────────────────── */

static void slab_free(void *ptr);
static bool slab_map_register_page(slab_page_t *page);
static void slab_map_unregister_page(slab_page_t *page);

/* Allocate and register a new SLAB_PAGE_SIZE-aligned page, threading its chunks
 * onto the thread-local free list. Returns false on OOM (caller falls back to
 * malloc). Registration happens before any chunk is handed out so that a later
 * cross-thread free can always recover the page. */
static bool slab_grow(slab_state_t *s) {
    void *mem = NULL;
    if (cbm_aligned_alloc(&mem, SLAB_PAGE_SIZE, SLAB_PAGE_SIZE) != 0 || !mem) {
        return false;
    }
    slab_page_t *page = (slab_page_t *)mem;
    page->next = s->pages;
    atomic_init(&page->owner, s);
    atomic_init(&page->remote_free_head, (slab_free_node_t *)NULL);
    atomic_init(&page->refcount, 1u); /* owner guard */

    if (!slab_map_register_page(page)) {
        cbm_aligned_free(page);
        return false;
    }
    s->pages = page;

    for (size_t i = 0; i < SLAB_PAGE_CHUNKS; i++) {
        slab_free_node_t *node =
            (slab_free_node_t *)((char *)page + SLAB_DATA_OFFSET + i * SLAB_CHUNK_SIZE);
        node->next = s->freelist;
        s->freelist = node;
    }
    return true;
}

/* Refill the thread-local free list: first reclaim any cross-thread frees
 * queued on owned pages, then grow a fresh page if still empty. */
static void slab_refill(slab_state_t *s) {
    for (slab_page_t *p = s->pages; p; p = p->next) {
        slab_free_node_t *rf = atomic_exchange_explicit(
            &p->remote_free_head, (slab_free_node_t *)NULL, memory_order_acquire);
        while (rf) {
            slab_free_node_t *next = rf->next;
            rf->next = s->freelist;
            s->freelist = rf;
            rf = next;
        }
    }
    if (s->freelist) {
        return;
    }
    (void)slab_grow(s);
}

/* Retire ownership and drop the owner guard for every page this thread owns.
 * A page with no live chunks is freed immediately; a page still holding
 * foreign-live chunks is retired (owner=NULL, left registered) and freed by
 * whichever thread returns its last chunk. */
static void slab_reclaim_pages(slab_state_t *s, bool clear_installed) {
    slab_page_t *p = s->pages;
    while (p) {
        slab_page_t *next = p->next;
        atomic_store_explicit(&p->owner, (slab_state_t *)NULL, memory_order_relaxed);
        unsigned prev = atomic_fetch_sub_explicit(&p->refcount, 1u, memory_order_acq_rel);
        if (prev == 1u) {
            slab_map_unregister_page(p);
            cbm_aligned_free(p);
        }
        p = next;
    }
    s->pages = NULL;
    s->freelist = NULL;
    if (clear_installed) {
        s->installed = false;
    }
}

/* ── Allocator functions (installed as tree-sitter callbacks) ───── */

static void *slab_malloc(size_t size) {
    if (size == 0) {
        size = SKIP_ONE;
    }
    /* Tier 1: ≤64B → slab free list */
    if (size <= SLAB_CHUNK_SIZE) {
        slab_state_t *s = &tls_slab;
        if (!s->freelist) {
            slab_refill(s);
            if (!s->freelist) {
                return malloc(size); /* grow failed → heap fallback */
            }
        }
        slab_free_node_t *node = s->freelist;
        s->freelist = node->next;
        slab_page_t *page = (slab_page_t *)((uintptr_t)node & SLAB_PAGE_MASK);
        atomic_fetch_add_explicit(&page->refcount, 1u, memory_order_relaxed);
        return node;
    }

    /* >64B: straight to malloc (= mimalloc in production) */
    return malloc(size);
}

static void *slab_calloc(size_t count, size_t size) {
    /* Overflow check */
    if (count > 0 && size > SIZE_MAX / count) {
        return NULL;
    }
    size_t total = count * size;
    void *ptr = slab_malloc(total);
    if (ptr) {
        /* Must zero: free-list recycled blocks contain stale data. */
        memset(ptr, 0, total);
    }
    return ptr;
}

static void *slab_realloc(void *ptr, size_t new_size) {
    if (!ptr) {
        return slab_malloc(new_size);
    }
    if (new_size == 0) {
        /* realloc(ptr, 0) = free + return NULL */
        slab_free(ptr);
        return NULL;
    }

    /* Case 1: ptr is in a slab page (≤64B block) */
    slab_page_t *page = slab_map_lookup((uintptr_t)ptr & SLAB_PAGE_MASK);
    if (page) {
        if (new_size <= SLAB_CHUNK_SIZE) {
            /* Still fits in a slab chunk — reuse same slot */
            return ptr;
        }
        /* Promote slab → heap */
        void *new_ptr = malloc(new_size);
        if (!new_ptr) {
            return NULL;
        }
        memcpy(new_ptr, ptr, SLAB_CHUNK_SIZE);
        slab_free(ptr);
        return new_ptr;
    }

    /* Case 2: heap pointer (from malloc) */
    return realloc(ptr, new_size);
}

static void slab_free(void *ptr) {
    if (!ptr) {
        return;
    }
    uintptr_t base = (uintptr_t)ptr & SLAB_PAGE_MASK;
    slab_page_t *page = slab_map_lookup(base);
    if (!page) {
        /* Not a slab chunk → plain heap pointer (>64B or grow-fallback). */
        free(ptr);
        return;
    }

    /* It is a slab chunk. The chunk itself keeps its page alive (refcount ≥ 1)
     * until this very free, so the page cannot be freed underneath us. */
    slab_state_t *s = &tls_slab;
    slab_free_node_t *node = (slab_free_node_t *)ptr;

    if (atomic_load_explicit(&page->owner, memory_order_relaxed) == s) {
        /* Fast path: owner returns the chunk to its thread-local free list. */
        node->next = s->freelist;
        s->freelist = node;
        atomic_fetch_sub_explicit(&page->refcount, 1u, memory_order_acq_rel);
        return; /* owner guard keeps refcount ≥ 1; page is reused, not freed */
    }

    /* Foreign (or retired) free: push onto the page's lock-free MPSC stack. */
    slab_free_node_t *head = atomic_load_explicit(&page->remote_free_head, memory_order_relaxed);
    do {
        node->next = head;
    } while (!atomic_compare_exchange_weak_explicit(&page->remote_free_head, &head, node,
                                                    memory_order_release, memory_order_relaxed));
    unsigned prev = atomic_fetch_sub_explicit(&page->refcount, 1u, memory_order_acq_rel);
    if (prev == 1u) {
        /* We returned the final chunk of a retired page → release it. */
        slab_map_unregister_page(page);
        cbm_aligned_free(page);
    }
}

/* ── Page-map registration wrappers (declared after slab_map_set) ── */

static bool slab_map_register_page(slab_page_t *page) {
    return slab_map_set((uintptr_t)page, page);
}
static void slab_map_unregister_page(slab_page_t *page) {
    (void)slab_map_set((uintptr_t)page, (slab_page_t *)NULL);
}

/* ── Public API ─────────────────────────────────────────────────── */

/* Forward declaration of tree-sitter's allocator setter. */
extern void ts_set_allocator(void *(*new_malloc)(size_t), void *(*new_calloc)(size_t, size_t),
                             void *(*new_realloc)(void *, size_t), void (*new_free)(void *));

void cbm_slab_install(void) {
    ts_set_allocator(slab_malloc, slab_calloc, slab_realloc, slab_free);
}

void cbm_slab_reset_thread(void) {
    cbm_slab_reclaim();
}

void cbm_slab_destroy_thread(void) {
    slab_reclaim_pages(&tls_slab, true);
}

/* Reclaim slab memory owned by the current thread.
 *
 * Call ONLY when no LOCAL live allocations remain — i.e., after ts_tree_delete()
 * AND ts_parser_delete() have freed this thread's parser-owned chunks. If a
 * chunk from one of these pages is still held by another parser thread, the
 * page is retired instead of freed and released on the final cross-thread free.
 * This keeps peak memory bounded per-file without handing foreign slab chunks
 * to plain free(). */
void cbm_slab_reclaim(void) {
    slab_reclaim_pages(&tls_slab, false);
}

/* ── Test API (thin wrappers for unit testing) ──────────────────── */

void *cbm_slab_test_malloc(size_t size) {
    return slab_malloc(size);
}
void cbm_slab_test_free(void *ptr) {
    slab_free(ptr);
}
void *cbm_slab_test_realloc(void *ptr, size_t size) {
    return slab_realloc(ptr, size);
}
void *cbm_slab_test_calloc(size_t count, size_t size) {
    return slab_calloc(count, size);
}
