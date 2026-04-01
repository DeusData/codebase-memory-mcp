#include "arena.h"
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>

void cbm_arena_init(CBMArena *a) {
    memset(a, 0, sizeof(*a));
    a->block_size = CBM_ARENA_DEFAULT_BLOCK_SIZE;
    a->blocks[0] = (char *)malloc(a->block_size);
    if (a->blocks[0]) {
        a->nblocks = SKIP_ONE;
    }
}

static int arena_grow(CBMArena *a, size_t min_size) {
    if (a->nblocks >= CBM_ARENA_MAX_BLOCKS) {
        return 0;
    }
    size_t new_size = a->block_size * PAIR_LEN;
    if (new_size < min_size) {
        new_size = min_size;
    }
    char *block = (char *)malloc(new_size);
    if (!block) {
        return 0;
    }
    a->blocks[a->nblocks] = block;
    a->nblocks++;
    a->block_size = new_size;
    a->used = 0;
    return 1;
}

void *cbm_arena_alloc(CBMArena *a, size_t n) {
    if (n == 0) {
        return NULL;
    }
    // 8-byte alignment
    n = (n + 7) & ~(size_t)7;

    if (a->nblocks == 0) {
        return NULL;
    }

    if (a->used + n > a->block_size) {
        if (!arena_grow(a, n)) {
            return NULL;
        }
    }

    char *ptr = a->blocks[a->nblocks - SKIP_ONE] + a->used;
    a->used += n;
    return ptr;
}

char *cbm_arena_strdup(CBMArena *a, const char *s) {
    if (!s)
        return NULL;
    size_t len = strlen(s);
    char *dst = (char *)cbm_arena_alloc(a, len + SKIP_ONE);
    if (dst) {
        memcpy(dst, s, len + SKIP_ONE);
    }
    return dst;
}

char *cbm_arena_strndup(CBMArena *a, const char *s, size_t len) {
    if (!s)
        return NULL;
    char *dst = (char *)cbm_arena_alloc(a, len + SKIP_ONE);
    if (dst) {
        memcpy(dst, s, len);
        dst[len] = '\0';
    }
    return dst;
}

char *cbm_arena_sprintf(CBMArena *a, const char *fmt, ...) {
    // First pass: compute length
    va_list args;
    va_start(args, fmt);
    int needed = vsnprintf(NULL, 0, fmt, args);
    va_end(args);

    if (needed < 0) {
        return NULL;
    }

    char *dst = (char *)cbm_arena_alloc(a, (size_t)needed + SKIP_ONE);
    if (!dst) {
        return NULL;
    }

    va_start(args, fmt);
    vsnprintf(dst, (size_t)needed + SKIP_ONE, fmt, args);
    va_end(args);

    return dst;
}

void cbm_arena_destroy(CBMArena *a) {
    for (int i = 0; i < a->nblocks; i++) {
        free(a->blocks[i]);
    }
    memset(a, 0, sizeof(*a));
}
