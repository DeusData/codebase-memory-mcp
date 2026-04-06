/*
 * str_intern.c — String interning pool.
 *
 * Uses a hash table for O(1) dedup lookup and an arena for string storage.
 * All strings are copied into the arena — the pool owns the memory.
 */
#include "str_intern.h"
#include "arena.h"
#include "foundation/constants.h"

enum { INTERN_LOAD_NUM = 7 };
#include <stdint.h> // uint32_t
#include <stdlib.h>
#include <string.h>

/* FNV-1a hash constants (published by Fowler/Noll/Vo) */
#define FNV_OFFSET_BASIS 2166136261U
#define FNV_PRIME 16777619U

/* FNV-1a for interning (matches hash_table.c) */
static uint32_t intern_hash(const char *s, size_t len) {
    uint32_t h = FNV_OFFSET_BASIS;
    for (size_t i = 0; i < len; i++) {
        h ^= (unsigned char)s[i];
        h *= FNV_PRIME;
    }
    return h;
}

/* Bucket for open-addressing */
typedef struct {
    const char *str; /* arena-owned pointer */
    uint32_t hash;
    uint32_t len; /* string length (excluding NUL) */
} InternEntry;

struct CBMInternPool {
    CBMArena arena;
    InternEntry *buckets;
    uint32_t capacity;
    uint32_t count;
    uint32_t mask;
    size_t total_bytes; /* sum of string lengths stored */
};

CBMInternPool *cbm_intern_create(void) {
    CBMInternPool *p = (CBMInternPool *)calloc(CBM_ALLOC_ONE, sizeof(*p));
    if (!p) {
        return NULL;
    }
    cbm_arena_init(&p->arena);
    p->capacity = CBM_SZ_256;
    p->mask = p->capacity - SKIP_ONE;
    p->buckets = (InternEntry *)calloc(p->capacity, sizeof(InternEntry));
    if (!p->buckets) {
        cbm_arena_destroy(&p->arena);
        free(p);
        return NULL;
    }
    return p;
}

void cbm_intern_free(CBMInternPool *pool) {
    if (!pool) {
        return;
    }
    cbm_arena_destroy(&pool->arena);
    free(pool->buckets);
    free(pool);
}

static void intern_resize(CBMInternPool *p) {
    uint32_t new_cap = p->capacity * PAIR_LEN;
    uint32_t new_mask = new_cap - SKIP_ONE;
    InternEntry *new_buckets = (InternEntry *)calloc(new_cap, sizeof(InternEntry));
    if (!new_buckets) {
        return;
    }

    for (uint32_t i = 0; i < p->capacity; i++) {
        const InternEntry *e = &p->buckets[i];
        if (!e->str) {
            continue;
        }
        uint32_t idx = e->hash & new_mask;
        while (new_buckets[idx].str) {
            idx = (idx + SKIP_ONE) & new_mask;
        }
        new_buckets[idx] = *e;
    }

    free(p->buckets);
    p->buckets = new_buckets;
    p->capacity = new_cap;
    p->mask = new_mask;
}

const char *cbm_intern_n(CBMInternPool *pool, const char *s, size_t len) {
    if (!pool || !s) {
        return NULL;
    }

    uint32_t h = intern_hash(s, len);
    uint32_t idx = h & pool->mask;

    /* Probe for existing entry */
    for (;;) {
        const InternEntry *e = &pool->buckets[idx];
        if (!e->str) {
            break; /* empty slot — not found */
        }
        if (e->hash == h && e->len == (uint32_t)len && memcmp(e->str, s, len) == 0) {
            return e->str; /* found — return existing pointer */
        }
        idx = (idx + SKIP_ONE) & pool->mask;
    }

    /* Resize at 70% load */
    if (pool->count * CBM_DECIMAL_BASE >= pool->capacity * INTERN_LOAD_NUM) {
        intern_resize(pool);
        /* Re-probe after resize */
        idx = h & pool->mask;
        while (pool->buckets[idx].str) {
            idx = (idx + SKIP_ONE) & pool->mask;
        }
    }

    /* Copy string into arena */
    char *copy = cbm_arena_strndup(&pool->arena, s, len);
    if (!copy) {
        return NULL;
    }

    pool->buckets[idx] = (InternEntry){.str = copy, .hash = h, .len = (uint32_t)len};
    pool->count++;
    pool->total_bytes += len;
    return copy;
}

const char *cbm_intern(CBMInternPool *pool, const char *s) {
    if (!s) {
        return NULL;
    }
    return cbm_intern_n(pool, s, strlen(s));
}

uint32_t cbm_intern_count(const CBMInternPool *pool) {
    return pool ? pool->count : 0;
}

size_t cbm_intern_bytes(const CBMInternPool *pool) {
    return pool ? pool->total_bytes : 0;
}
