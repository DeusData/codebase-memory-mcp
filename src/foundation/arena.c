/*
 * arena.c — Bump allocator implementation.
 *
 * Restructured from internal/cbm/arena.c with additions:
 *   - cbm_arena_init_sized() for custom block sizes
 *   - cbm_arena_calloc() for zero-initialized allocations
 *   - cbm_arena_reset() for reuse without full destroy
 *   - cbm_arena_total() for allocation tracking
 *
 * Arena blocks use malloc/free (= mimalloc in production builds).
 */
#include "arena.h"
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>

void cbm_arena_init(CBMArena *a) {
    cbm_arena_init_sized(a, CBM_ARENA_DEFAULT_BLOCK_SIZE);
}

void cbm_arena_init_sized(CBMArena *a, size_t block_size) {
    memset(a, 0, sizeof(*a));
    if (block_size < 64) {
        block_size = 64; /* minimum sanity */
    }
    a->block_size = block_size;
    a->blocks[0] = (char *)malloc(block_size);
    if (a->blocks[0]) {
        a->block_sizes[0] = block_size;
        a->nblocks = 1;
    }
}

static int arena_grow(CBMArena *a, size_t min_size) {
    if (a->nblocks >= CBM_ARENA_MAX_BLOCKS) {
        return 0;
    }
    size_t new_size = a->block_size * 2;
    if (new_size < min_size) {
        new_size = min_size;
    }
    char *block = (char *)malloc(new_size);
    if (!block) {
        return 0;
    }
    a->blocks[a->nblocks] = block;
    a->block_sizes[a->nblocks] = new_size;
    a->nblocks++;
    a->block_size = new_size;
    a->used = 0;
    return 1;
}

void *cbm_arena_alloc(CBMArena *a, size_t n) {
    if (n == 0) {
        return NULL;
    }
    /* 8-byte alignment */
    n = (n + 7) & ~(size_t)7;
    if (a->nblocks == 0) {
        return NULL;
    }
    if (a->used + n > a->block_size) {
        if (!arena_grow(a, n)) {
            return NULL;
        }
    }
    char *ptr = a->blocks[a->nblocks - 1] + a->used;
    a->used += n;
    a->total_alloc += n;
    return ptr;
}

void *cbm_arena_calloc(CBMArena *a, size_t n) {
    void *p = cbm_arena_alloc(a, n);
    if (p) {
        memset(p, 0, n);
    }
    return p;
}

char *cbm_arena_strdup(CBMArena *a, const char *s) {
    if (!s) {
        return NULL;
    }
    size_t len = strlen(s);
    char *dst = (char *)cbm_arena_alloc(a, len + 1);
    if (dst) {
        memcpy(dst, s, len + 1);
    }
    return dst;
}

char *cbm_arena_strndup(CBMArena *a, const char *s, size_t len) {
    if (!s) {
        return NULL;
    }
    char *dst = (char *)cbm_arena_alloc(a, len + 1);
    if (dst) {
        memcpy(dst, s, len);
        dst[len] = '\0';
    }
    return dst;
}

char *cbm_arena_sprintf(CBMArena *a, const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    int needed = vsnprintf(NULL, 0, fmt, args);
    va_end(args);
    if (needed < 0) {
        return NULL;
    }

    char *dst = (char *)cbm_arena_alloc(a, (size_t)needed + 1);
    if (!dst) {
        return NULL;
    }

    va_start(args, fmt);
    vsnprintf(dst, (size_t)needed + 1, fmt, args);
    va_end(args);
    return dst;
}

void cbm_arena_reset(CBMArena *a) {
    /* Keep first block, free the rest */
    for (int i = 1; i < a->nblocks; i++) {
        free(a->blocks[i]);
        a->blocks[i] = NULL;
        a->block_sizes[i] = 0;
    }
    if (a->nblocks > 1) {
        a->nblocks = 1;
    }
    a->used = 0;
    a->total_alloc = 0;
    /* Reset block_size to match surviving block — prevents overflow if
     * block_size grew during previous allocations (e.g., 128 → 256). */
    if (a->nblocks == 1) {
        a->block_size = a->block_sizes[0];
    }
}

void cbm_arena_destroy(CBMArena *a) {
    for (int i = 0; i < a->nblocks; i++) {
        free(a->blocks[i]);
    }
    memset(a, 0, sizeof(*a));
}

size_t cbm_arena_total(const CBMArena *a) {
    return a->total_alloc;
}
