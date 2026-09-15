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
#include "foundation/constants.h"
#include "foundation/mem_core.h"

enum { ARENA_ALIGN = 7, ARENA_GROW_OK = 1 };
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#if defined(__APPLE__)
#include <pthread.h>
#include <xlocale.h>
#endif
#include <stdio.h>

void cbm_arena_init(CBMArena *a) {
    cbm_arena_init_sized(a, CBM_ARENA_DEFAULT_BLOCK_SIZE);
}

void cbm_arena_init_sized(CBMArena *a, size_t block_size) {
    memset(a, 0, sizeof(*a));
    if (block_size < CBM_SZ_64) {
        block_size = CBM_SZ_64; /* minimum sanity */
    }
    a->block_size = block_size;
    a->grow_size = block_size * PAIR_LEN;
    a->blocks[0] = (char *)cbm_alloc(CBM_MEM_CLASS_ARENA, block_size);
    if (a->blocks[0]) {
        a->block_sizes[0] = block_size;
        a->nblocks = SKIP_ONE;
    }
}

void cbm_arena_init_exact(CBMArena *a, size_t bytes) {
    size_t block = (bytes + ARENA_ALIGN) & ~(size_t)ARENA_ALIGN;
    cbm_arena_init_sized(a, block);
    a->grow_size = CBM_ARENA_DEFAULT_BLOCK_SIZE;
    /* An exact block is a compacted result's image and may be written to
     * disk whole (result_spill): the alignment gaps between objects and the
     * padding inside structs are never written by the copy, and MemorySanitizer
     * refuses an fwrite of uninitialized bytes (CI MSan lane on #2202, offset
     * 4087 of a 6,952-byte block). Zeroed once here, every byte of the image
     * is defined; the cost is one pass over memory the copy is about to touch. */
    if (a->blocks[0]) {
        memset(a->blocks[0], 0, a->block_sizes[0]);
    }
}

static int arena_grow(CBMArena *a, size_t min_size) {
    /* A rewound arena still owns blocks past the cursor: use the next one if
     * it fits, otherwise drop it and everything after it and grow fresh. */
    if (a->cur + SKIP_ONE < a->nblocks && a->blocks[a->cur + SKIP_ONE]) {
        if (a->block_sizes[a->cur + SKIP_ONE] >= min_size) {
            a->cur++;
            a->block_size = a->block_sizes[a->cur];
            a->used = 0;
            return ARENA_GROW_OK;
        }
        for (int i = a->cur + SKIP_ONE; i < a->nblocks; i++) {
            cbm_free(CBM_MEM_CLASS_ARENA, a->blocks[i]);
            a->blocks[i] = NULL;
            a->block_sizes[i] = 0;
        }
        a->nblocks = a->cur + SKIP_ONE;
    }
    if (a->nblocks >= CBM_ARENA_MAX_BLOCKS) {
        return 0;
    }
    size_t new_size = a->grow_size;
    if (new_size < min_size) {
        new_size = min_size;
    }
    a->grow_size = new_size * PAIR_LEN;
    char *block = (char *)cbm_alloc(CBM_MEM_CLASS_ARENA, new_size);
    if (!block) {
        return 0;
    }
    a->blocks[a->nblocks] = block;
    a->block_sizes[a->nblocks] = new_size;
    a->cur = a->nblocks;
    a->nblocks++;
    a->block_size = new_size;
    a->used = 0;
    return ARENA_GROW_OK;
}

void *cbm_arena_alloc(CBMArena *a, size_t n) {
    if (!a || n == 0) {
        return NULL;
    }
    /* 8-byte alignment */
    n = (n + ARENA_ALIGN) & ~(size_t)ARENA_ALIGN;
    if (a->nblocks == 0) {
        return NULL;
    }
    if (a->used + n > a->block_size) {
        if (!arena_grow(a, n)) {
            return NULL;
        }
    }
    char *ptr = a->blocks[a->cur] + a->used;
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
    char *dst = (char *)cbm_arena_alloc(a, len + SKIP_ONE);
    if (dst) {
        memcpy(dst, s, len + SKIP_ONE);
    }
    return dst;
}

char *cbm_arena_strndup(CBMArena *a, const char *s, size_t len) {
    if (!s) {
        return NULL;
    }
    char *dst = (char *)cbm_arena_alloc(a, len + SKIP_ONE);
    if (dst) {
        memcpy(dst, s, len);
        dst[len] = '\0';
    }
    return dst;
}

/* On macOS every vsnprintf consults the process locale under an unfair lock
 * (localeconv_l inside __vfprintf). Eighteen workers formatting type names
 * collapsed into that lock on the C# corpus -- the 10 MB JIT test files took
 * 68 s each instead of under a second (sampled 2026-09-14) -- and the old
 * two-pass form paid it twice per string. Each thread formats with a C locale
 * object of its own, so no thread ever waits for another, and the common
 * short string is formatted once into a stack buffer. */
#if defined(__APPLE__)
/* One locale object per thread, freed when the thread exits: the object is
 * heap memory that the thread-local pointer alone kept, so every worker
 * thread that ever formatted a name leaked 1,472 bytes at exit (the macOS
 * LSan lane on PR #2202, 16-101 objects per test process). A pthread key
 * destructor is the one hook that runs at thread exit for a TLS-held
 * resource; the main thread keeps its locale until process exit. */
static _Thread_local locale_t tl_c_locale;
static pthread_key_t tl_c_locale_key;
static pthread_once_t tl_c_locale_once = PTHREAD_ONCE_INIT;
static void arena_c_locale_free(void *loc) {
    if (loc) {
        freelocale((locale_t)loc);
    }
}
static void arena_c_locale_key_init(void) {
    (void)pthread_key_create(&tl_c_locale_key, arena_c_locale_free);
}
static locale_t arena_c_locale(void) {
    if (!tl_c_locale) {
        tl_c_locale = newlocale(LC_ALL_MASK, "C", NULL);
        if (tl_c_locale) {
            pthread_once(&tl_c_locale_once, arena_c_locale_key_init);
            (void)pthread_setspecific(tl_c_locale_key, tl_c_locale);
        }
    }
    return tl_c_locale; /* NULL = the global locale, the pre-fix behaviour */
}
#define ARENA_VSNPRINTF(buf, n, fmt, ap) vsnprintf_l((buf), (n), arena_c_locale(), (fmt), (ap))
#else
#define ARENA_VSNPRINTF(buf, n, fmt, ap) vsnprintf((buf), (n), (fmt), (ap))
#endif

enum { ARENA_SPRINTF_LOCAL = 512 };

char *cbm_arena_sprintf(CBMArena *a, const char *fmt, ...) {
    char local[ARENA_SPRINTF_LOCAL];
    va_list args;
    va_start(args, fmt);
    int needed = ARENA_VSNPRINTF(local, sizeof(local), fmt, args);
    va_end(args);
    if (needed < 0) {
        return NULL;
    }

    char *dst = (char *)cbm_arena_alloc(a, (size_t)needed + SKIP_ONE);
    if (!dst) {
        return NULL;
    }
    if ((size_t)needed < sizeof(local)) {
        memcpy(dst, local, (size_t)needed + SKIP_ONE);
        return dst;
    }

    va_start(args, fmt);
    ARENA_VSNPRINTF(dst, (size_t)needed + SKIP_ONE, fmt, args);
    va_end(args);
    return dst;
}

void cbm_arena_rewind(CBMArena *a) {
    if (!a || a->nblocks == 0) {
        return;
    }
    a->cur = 0;
    a->block_size = a->block_sizes[0];
    a->used = 0;
    a->total_alloc = 0;
}

size_t cbm_arena_capacity(const CBMArena *a) {
    size_t total = 0;
    for (int i = 0; a && i < a->nblocks; i++) {
        total += a->block_sizes[i];
    }
    return total;
}

void cbm_arena_reset(CBMArena *a) {
    /* Keep first block, free the rest */
    for (int i = SKIP_ONE; i < a->nblocks; i++) {
        cbm_free(CBM_MEM_CLASS_ARENA, a->blocks[i]);
        a->blocks[i] = NULL;
        a->block_sizes[i] = 0;
    }
    if (a->nblocks > SKIP_ONE) {
        a->nblocks = SKIP_ONE;
    }
    a->cur = 0;
    a->used = 0;
    a->total_alloc = 0;
    /* Reset block_size to match surviving block — prevents overflow if
     * block_size grew during previous allocations (e.g., CBM_SZ_128 → CBM_SZ_256). */
    if (a->nblocks == SKIP_ONE) {
        a->block_size = a->block_sizes[0];
        a->grow_size = a->block_size * PAIR_LEN;
    }
}

void cbm_arena_destroy(CBMArena *a) {
    for (int i = 0; i < a->nblocks; i++) {
        cbm_free(CBM_MEM_CLASS_ARENA, a->blocks[i]);
    }
    memset(a, 0, sizeof(*a));
}

size_t cbm_arena_total(const CBMArena *a) {
    return a->total_alloc;
}
