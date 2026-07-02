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
 *   Pages are owned by one thread for reuse, with a global registry so
 *   cross-thread tree-sitter frees cannot fall through to plain free().
 *
 * All allocations >64B go directly to malloc() which is mimalloc
 * in production builds (MI_OVERRIDE=1). This eliminates the complex
 * tier2 bump allocator and its O(n) ownership checks.
 *
 * On reclaim/destroy: free pages with no live chunks; retire pages that still
 * have foreign-live chunks and free them when the final chunk returns.
 * realloc handles slab-to-heap promotion with minimal copying.
 */
#include "foundation/constants.h"
#include "foundation/slab_alloc.h"
#include "foundation/compat.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdalign.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

/* ── Tier 1: Fixed-size slab (≤64B) ──────────────────────────────── */

/* Chunk size matches tree-sitter SubtreeHeapData (64 bytes).
 * All slab allocations are this size regardless of requested size. */
#define SLAB_CHUNK_SIZE 64

/* Each slab page: 64KB = 1024 chunks of 64 bytes.
 * Amortizes allocation overhead. One page per ~1 large file. */
#define SLAB_PAGE_CHUNKS 1024
#define SLAB_PAGE_SIZE (SLAB_CHUNK_SIZE * SLAB_PAGE_CHUNKS)

/* Free list node — occupies the first 8 bytes of a free chunk. */
typedef struct slab_free_node {
    struct slab_free_node *next;
} slab_free_node_t;

typedef struct slab_state slab_state_t;

/* One slab page — a contiguous block of SLAB_PAGE_CHUNKS chunks. */
typedef struct slab_page {
    struct slab_page *next;        /* linked list of pages owned by a TLS state */
    struct slab_page *global_next; /* global ownership registry */
    slab_state_t *owner;           /* TLS state that can reuse freed chunks */
    unsigned live_count;           /* chunks currently handed to tree-sitter */
    bool retired;                  /* owner reclaimed/destroyed while chunks live */
    alignas(max_align_t) char data[SLAB_PAGE_SIZE];
} slab_page_t;

/* Per-thread Tier 1 state. */
struct slab_state {
    slab_page_t *pages;         /* linked list of all allocated pages */
    slab_free_node_t *freelist; /* singly-linked free list */
    bool installed;
};

static CBM_TLS slab_state_t tls_slab;
static atomic_flag g_slab_lock = ATOMIC_FLAG_INIT;
static slab_page_t *g_slab_pages = NULL;

/* ── Tier 1 helpers ────────────────────────────────────────────────── */

static void slab_lock(void) {
    while (atomic_flag_test_and_set_explicit(&g_slab_lock, memory_order_acquire)) {
        /* Spin until the allocator registry is available. */
    }
}

static void slab_unlock(void) {
    atomic_flag_clear_explicit(&g_slab_lock, memory_order_release);
}

static void slab_register_page_locked(slab_page_t *page) {
    page->global_next = g_slab_pages;
    g_slab_pages = page;
}

static void slab_unregister_page_locked(slab_page_t *page) {
    slab_page_t **cur = &g_slab_pages;
    while (*cur) {
        if (*cur == page) {
            *cur = page->global_next;
            page->global_next = NULL;
            return;
        }
        cur = &(*cur)->global_next;
    }
}

static slab_page_t *slab_find_page_locked(const void *ptr) {
    uintptr_t p = (uintptr_t)ptr;
    for (slab_page_t *page = g_slab_pages; page; page = page->global_next) {
        uintptr_t lo = (uintptr_t)page->data;
        if (p >= lo && p < lo + (uintptr_t)SLAB_PAGE_SIZE) {
            return page;
        }
    }
    return NULL;
}

/* Add a new page to the slab and prepend its chunks to the free list.
 * Pages are allocated via malloc (= mimalloc in production). Caller holds
 * g_slab_lock because tree-sitter's allocator callbacks are global and a
 * chunk allocated by one parser thread may be freed by another parser thread. */
static bool slab_grow_locked(slab_state_t *s) {
    slab_page_t *page = (slab_page_t *)malloc(sizeof(slab_page_t));
    if (!page) {
        return false;
    }
    page->next = s->pages;
    page->global_next = NULL;
    page->owner = s;
    page->live_count = 0;
    page->retired = false;
    s->pages = page;
    slab_register_page_locked(page);

    /* Thread page's chunks onto the free list */
    for (size_t i = 0; i < SLAB_PAGE_CHUNKS; i++) {
        slab_free_node_t *node = (slab_free_node_t *)(page->data + (i * SLAB_CHUNK_SIZE));
        node->next = s->freelist;
        s->freelist = node;
    }
    return true;
}

static slab_page_t *slab_detach_owned_pages_locked(slab_state_t *s) {
    slab_page_t *free_pages = NULL;
    slab_page_t *p = s->pages;

    while (p) {
        slab_page_t *next = p->next;
        if (p->live_count == 0) {
            slab_unregister_page_locked(p);
            p->next = free_pages;
            free_pages = p;
        } else {
            p->owner = NULL;
            p->retired = true;
            p->next = NULL;
        }
        p = next;
    }

    s->pages = NULL;
    s->freelist = NULL;
    return free_pages;
}

static void slab_free_page_list(slab_page_t *pages) {
    while (pages) {
        slab_page_t *next = pages->next;
        free(pages);
        pages = next;
    }
}

static void slab_free(void *ptr);

/* ── Allocator functions (installed as tree-sitter callbacks) ───── */

static void *slab_malloc(size_t size) {
    if (size == 0) {
        size = SKIP_ONE;
    }
    /* Tier 1: ≤64B → slab free list */
    if (size <= SLAB_CHUNK_SIZE) {
        slab_state_t *s = &tls_slab;
        slab_lock();
        if (!s->freelist) {
            if (!slab_grow_locked(s)) {
                slab_unlock();
                return malloc(size); /* fallback */
            }
        }
        slab_free_node_t *node = s->freelist;
        s->freelist = node->next;
        slab_page_t *page = slab_find_page_locked(node);
        if (page) {
            page->live_count++;
        }
        slab_unlock();
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

    /* Case 1: ptr is in slab (≤64B block) */
    slab_lock();
    slab_page_t *page = slab_find_page_locked(ptr);
    slab_unlock();
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

    slab_lock();
    slab_page_t *page = slab_find_page_locked(ptr);
    if (page) {
        bool free_retired_page = false;
        if (page->live_count > 0) {
            page->live_count--;
        }

        slab_free_node_t *node = (slab_free_node_t *)ptr;
        if (page->owner && !page->retired) {
            node->next = page->owner->freelist;
            page->owner->freelist = node;
        } else {
            node->next = NULL;
        }

        if (page->retired && page->live_count == 0) {
            slab_unregister_page_locked(page);
            free_retired_page = true;
        }
        slab_unlock();

        if (free_retired_page) {
            free(page);
        }
        return;
    }
    slab_unlock();

    /* Heap fallback */
    free(ptr);
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
    slab_state_t *s = &tls_slab;

    slab_lock();
    slab_page_t *free_pages = slab_detach_owned_pages_locked(s);
    s->installed = false;
    slab_unlock();

    slab_free_page_list(free_pages);
}

/* Reclaim slab memory owned by the current thread.
 *
 * Call ONLY when no live allocations remain — i.e., after ts_tree_delete()
 * AND ts_parser_delete() have freed local parser-owned chunks. If tree-sitter
 * still returns a foreign-live chunk later, its page is retired and freed when
 * live_count reaches zero. This keeps peak memory bounded per-file without
 * handing foreign slab chunks to plain free(). */
void cbm_slab_reclaim(void) {
    slab_state_t *s = &tls_slab;

    slab_lock();
    slab_page_t *free_pages = slab_detach_owned_pages_locked(s);
    /* NOTE: keep s->installed true — allocator is still active,
     * just with empty pages. Next slab_malloc will call slab_grow. */
    slab_unlock();

    slab_free_page_list(free_pages);
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
