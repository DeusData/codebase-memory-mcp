/*
 * slab_alloc.c — Thread-local slab allocator for tree-sitter.
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
 * On slab_destroy_thread: free all slab pages.
 * realloc handles slab-to-heap promotion with minimal copying.
 */
#include "foundation/constants.h"
#include "foundation/slab_alloc.h"
#include "foundation/compat.h"

#include <stdbool.h>
#include <stdint.h>
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

/* One slab page — a contiguous block of SLAB_PAGE_CHUNKS chunks. */
typedef struct slab_page {
    struct slab_page *next; /* linked list of pages */
    char data[SLAB_PAGE_SIZE];
} slab_page_t;

/* Per-thread Tier 1 state. */
typedef struct {
    slab_page_t *pages;         /* linked list of all allocated pages */
    slab_free_node_t *freelist; /* singly-linked free list */
    bool installed;
} slab_state_t;

static CBM_TLS slab_state_t tls_slab;

/* ── Tier 1 helpers ────────────────────────────────────────────────── */

/* Rebuild free list from all existing pages. O(pages * SLAB_PAGE_CHUNKS). */
static void slab_rebuild_freelist(slab_state_t *s) {
    s->freelist = NULL;
    for (slab_page_t *p = s->pages; p; p = p->next) {
        for (size_t i = 0; i < SLAB_PAGE_CHUNKS; i++) {
            slab_free_node_t *node = (slab_free_node_t *)(p->data + (i * SLAB_CHUNK_SIZE));
            node->next = s->freelist;
            s->freelist = node;
        }
    }
}

/* Add a new page to the slab and prepend its chunks to the free list.
 * Pages are allocated via malloc (= mimalloc in production). */
static bool slab_grow(slab_state_t *s) {
    slab_page_t *page = (slab_page_t *)malloc(sizeof(slab_page_t));
    if (!page) {
        return false;
    }
    page->next = s->pages;
    s->pages = page;

    /* Thread page's chunks onto the free list */
    for (size_t i = 0; i < SLAB_PAGE_CHUNKS; i++) {
        slab_free_node_t *node = (slab_free_node_t *)(page->data + (i * SLAB_CHUNK_SIZE));
        node->next = s->freelist;
        s->freelist = node;
    }
    return true;
}

/* Check if a pointer belongs to any slab page (for realloc/free).
 * Linear scan is bounded: per-file reclaim keeps page count small. */
static bool slab_owns(const slab_state_t *s, const void *ptr) {
    uintptr_t p = (uintptr_t)ptr;
    for (const slab_page_t *page = s->pages; page; page = page->next) {
        uintptr_t lo = (uintptr_t)page->data;
        if (p >= lo && p < lo + (uintptr_t)SLAB_PAGE_SIZE) {
            return true;
        }
    }
    return false;
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
            if (!slab_grow(s)) {
                return malloc(size); /* fallback */
            }
        }
        slab_free_node_t *node = s->freelist;
        s->freelist = node->next;
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
        if (slab_owns(&tls_slab, ptr)) {
            slab_free_node_t *node = (slab_free_node_t *)ptr;
            node->next = tls_slab.freelist;
            tls_slab.freelist = node;
        } else {
            free(ptr);
        }
        return NULL;
    }

    /* Case 1: ptr is in slab (≤64B block) */
    if (slab_owns(&tls_slab, ptr)) {
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
        /* Return slab slot to free list */
        slab_free_node_t *node = (slab_free_node_t *)ptr;
        node->next = tls_slab.freelist;
        tls_slab.freelist = node;
        return new_ptr;
    }

    /* Case 2: heap pointer (from malloc) */
    return realloc(ptr, new_size);
}

static void slab_free(void *ptr) {
    if (!ptr) {
        return;
    }
    /* Slab page */
    if (slab_owns(&tls_slab, ptr)) {
        slab_free_node_t *node = (slab_free_node_t *)ptr;
        node->next = tls_slab.freelist;
        tls_slab.freelist = node;
        return;
    }
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
    slab_state_t *s = &tls_slab;
    if (!s->pages) {
        return;
    }
    slab_rebuild_freelist(s);
}

void cbm_slab_destroy_thread(void) {
    slab_state_t *s = &tls_slab;
    slab_page_t *p = s->pages;
    while (p) {
        slab_page_t *next = p->next;
        free(p);
        p = next;
    }
    s->pages = NULL;
    s->freelist = NULL;
    s->installed = false;
}

/* Reclaim all slab memory for the current thread.
 *
 * Call ONLY when no live allocations remain — i.e., after ts_tree_delete()
 * AND ts_parser_delete() have freed everything back to the free lists.
 * This keeps peak memory bounded per-file (not cumulative across files). */
void cbm_slab_reclaim(void) {
    slab_state_t *s = &tls_slab;
    slab_page_t *p = s->pages;
    while (p) {
        slab_page_t *next = p->next;
        free(p);
        p = next;
    }
    s->pages = NULL;
    s->freelist = NULL;
    /* NOTE: keep s->installed true — allocator is still active,
     * just with empty pages. Next slab_malloc will call slab_grow. */
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
