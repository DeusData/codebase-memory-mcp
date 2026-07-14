/*
 * slab_alloc.h — Slab allocator for tree-sitter.
 *
 * Replaces malloc/calloc/realloc/free for ALL tree-sitter allocations
 * to eliminate ptmalloc2's per-thread arena fragmentation.
 *
 * Tier 1 (≤64B): Fixed-size slab free list — O(1) alloc/free.
 *   Matches tree-sitter SubtreeHeapData (CBM_SZ_64 bytes). Backed by
 *   64KB slab pages, each allocated aligned to its own size so the owning
 *   page of any chunk is recovered in O(1) by masking the pointer. Pages are
 *   reused per thread; cross-thread frees are recognized as slab pointers
 *   (never handed to plain free()) and a page holding a still-live chunk is
 *   retired, then freed when the final chunk returns. No global lock and no
 *   per-op scan on the alloc/free hot path.
 *
 * All allocations >64B go directly to malloc (= mimalloc in production),
 * which handles size classes, thread caching, and OS page return
 * far better than a hand-rolled tier2 bump allocator.
 *
 * Usage:
 *   cbm_slab_install();         // once, before any parsing
 *   ... parse files ...
 *   cbm_slab_destroy_thread();  // on thread exit — frees owned memory
 */
#ifndef CBM_SLAB_ALLOC_H
#define CBM_SLAB_ALLOC_H

#include <stddef.h>

/* Install slab allocator as tree-sitter's malloc/calloc/realloc/free.
 * Must be called once before any ts_parser_new() calls. Thread-safe. */
void cbm_slab_install(void);

/* Reset the current thread's slab: owned pages are reclaimed or retired.
 * WARNING: Do NOT call between files if the parser retains live state.
 * Only safe after cbm_destroy_thread_parser() has been called. */
void cbm_slab_reset_thread(void);

/* Destroy the current thread's allocator state. Pages with no live chunks are
 * freed; pages still holding cross-thread live chunks are retired and freed on
 * the last free. Call on thread exit. */
void cbm_slab_destroy_thread(void);

/* Reclaim current-thread slab memory.
 * Call ONLY when no LOCAL live allocations remain (after ts_tree_delete AND
 * ts_parser_delete). If another parser thread still holds a chunk from one of
 * these pages, that page is retired instead of freed and released on the final
 * cross-thread free. Keeps the allocator installed — next allocation will grow
 * fresh pages as needed. This bounds peak memory per-file rather than
 * accumulating across all files in a worker. */
void cbm_slab_reclaim(void);

/* Test/diagnostic API: direct access to the slab allocator.
 * Use these to unit test slab (≤64B) and heap (>64B) paths. */
void *cbm_slab_test_malloc(size_t size);
void cbm_slab_test_free(void *ptr);
void *cbm_slab_test_realloc(void *ptr, size_t size);
void *cbm_slab_test_calloc(size_t count, size_t size);

#endif /* CBM_SLAB_ALLOC_H */
