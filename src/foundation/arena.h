/*
 * arena.h — Bump allocator with block-based growth.
 *
 * All memory is freed at once via cbm_arena_destroy(). Individual frees are
 * not supported — this is by design for per-file extraction where all data
 * has the same lifetime.
 *
 * Restructured from internal/cbm/arena.h for the pure C rewrite.
 * New additions: cbm_arena_reset() for reuse without realloc.
 */
#ifndef CBM_ARENA_H
#define CBM_ARENA_H

#include <stddef.h>
#include <stdarg.h>

#define CBM_ARENA_MAX_BLOCKS 256
#define CBM_ARENA_DEFAULT_BLOCK_SIZE ((size_t)64 * 1024) /* 64KB */

typedef struct {
    char *blocks[CBM_ARENA_MAX_BLOCKS];
    size_t block_sizes[CBM_ARENA_MAX_BLOCKS]; /* per-block sizes (for stats) */
    int nblocks;
    size_t block_size;  /* current block capacity */
    size_t used;        /* bytes used in current block */
    size_t total_alloc; /* cumulative bytes allocated (for stats) */
    size_t grow_size;   /* size of the NEXT block added; doubles per growth */
    int cur;            /* index of the block allocations come from (rewind sets 0) */
    /* Waste sanitizer (mem_events.h): where the arena was created and how many
     * bytes were left behind at the end of blocks it moved past. Present in
     * every build so the struct is the same in every translation unit. */
    void *waste_site;
    size_t waste_tail;
    size_t waste_grows; /* fresh blocks allocated since the last report */
} CBMArena;

/* Initialize arena with default block size. */
void cbm_arena_init(CBMArena *a);

/* Initialize arena with a custom initial block size. */
void cbm_arena_init_sized(CBMArena *a, size_t block_size);

/* Initialize an arena that holds no memory until its first allocation, which
 * opens a first block of `block_size` (later blocks double as usual). For an
 * arena many callers create and most never use: the per-file extraction
 * scratch opened a 512 KB block for every file, and 3.3 GB of those blocks were
 * never read or written on the Go corpus (waste sanitizer access lane,
 * 2026-09-17). A zeroed or destroyed arena stays closed: allocating from it
 * still returns NULL. */
void cbm_arena_init_lazy(CBMArena *a, size_t block_size);

/* The block an exact arena takes when something appends to it after the fact:
 * the cross-file LSP pass adds a few resolved calls to a compacted result, so
 * a 64 KB default block was ~60 KB of untouched memory per appended-to result
 * and 0.5 GB of the worker's peak on the Go corpus (waste sanitizer,
 * 2026-09-17). It doubles from here like any other arena. */
#define CBM_ARENA_APPEND_BLOCK ((size_t)8 * 1024)

/* Initialize an arena as ONE block of exactly `bytes` (rounded to alignment),
 * with later growth restarting at CBM_ARENA_APPEND_BLOCK rather than doubling
 * the exact block. This is the compaction target: a result whose reachable
 * data measures N bytes lands in one N-byte block with no tail, and a later
 * append (the cross-file LSP pass adds resolved calls) costs one small block,
 * not 2N. */
void cbm_arena_init_exact(CBMArena *a, size_t bytes);

/* Allocate n bytes (8-byte aligned). Returns NULL on OOM. */
void *cbm_arena_alloc(CBMArena *a, size_t n);

/* Allocate n bytes, zero-initialized. */
void *cbm_arena_calloc(CBMArena *a, size_t n);

/* Duplicate a NUL-terminated string. */
char *cbm_arena_strdup(CBMArena *a, const char *s);

/* Duplicate a string of known length, NUL-terminate. */
char *cbm_arena_strndup(CBMArena *a, const char *s, size_t len);

/* sprintf into arena memory. */
char *cbm_arena_sprintf(CBMArena *a, const char *fmt, ...) __attribute__((format(printf, 2, 3)));

/* Reset arena for reuse: keeps first block, frees the rest. */
void cbm_arena_reset(CBMArena *a);

/* Rewind: keep EVERY block, start allocating from the first one again. The
 * pages stay mapped and are overwritten by the next use -- no free, no
 * purge, no re-commit. This is what a per-worker working arena wants between
 * files: the same addresses reused directly. cbm_arena_capacity() says how
 * much such an arena holds, so a caller can drop an outsized one. */
void cbm_arena_rewind(CBMArena *a);
size_t cbm_arena_capacity(const CBMArena *a);

/* Free all blocks. Arena is zeroed after this. */
void cbm_arena_destroy(CBMArena *a);

/* Return total bytes allocated (for diagnostics). */
size_t cbm_arena_total(const CBMArena *a);

#endif /* CBM_ARENA_H */
