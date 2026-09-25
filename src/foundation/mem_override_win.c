/*
 * mem_override_win.c — Route Windows allocations through mimalloc.
 *
 * WHY THIS FILE EXISTS
 *
 * On POSIX the build links mimalloc's object first, so its strong malloc/free
 * symbols win and every allocation in everything we compile — our code,
 * SQLite, the tree-sitter grammars, yyjson — lands in mimalloc. cbm_mem_init
 * then sets purge_delay=0 and purge_decommits=1, so a page that becomes empty
 * is decommitted immediately and the OS takes it straight back. That is why a
 * long-lived POSIX daemon stays flat.
 *
 * Windows got none of that. mimalloc's own static override is gated on
 * `#elif defined(_MSC_VER)` (vendored/mimalloc/src/alloc-override.c), and this
 * project builds with clang/MinGW, which never defines _MSC_VER. So the branch
 * compiled out, the generic fallback's plain malloc/free definitions lost the
 * link against the static CRT, and -DMI_MALLOC_OVERRIDE=1 was defined but
 * inert. Ordinary malloc went to the CRT heap, which keeps freed pages
 * committed — so every allocator tuning in cbm_mem_init was POSIX-only, and a
 * long-lived daemon ratcheted committed memory until Windows fell over (#581).
 *
 * HOW THIS FIXES IT
 *
 * The linker's --wrap redirects every undefined reference to `malloc` in the
 * objects we link to `__wrap_malloc`, and gives us `__real_malloc` for the
 * original. That captures exactly the same population of allocations that
 * link-order override captures on POSIX, so parity follows by construction
 * rather than by trimming a heap afterwards.
 *
 * THE SAFETY PROPERTY THAT MATTERS
 *
 * --wrap only rewrites references in objects passed to this link. Code already
 * compiled into ucrt/msvcrt and the system DLLs still calls the CRT's
 * allocator, so the CRT can hand us a pointer mimalloc never made. Passing
 * such a pointer to mi_free aborts with "mi_free: invalid pointer" — that is
 * #424, which crashed `index` on every run. Every deallocating wrapper here
 * therefore asks mi_is_in_heap_region() who owns the pointer and routes it to
 * the matching allocator. Cross-allocator frees become impossible by
 * construction instead of by convention.
 *
 * Allocating wrappers need no such check: serving a new allocation from
 * mimalloc is always safe.
 */
#include "foundation/mem.h"
#include "foundation/mem_events.h" /* CBM_MEMEV_BACKING: empty outside the access lane */

#include <mimalloc.h>
#include <stddef.h>
#include <string.h>

#ifdef _WIN32
#include <malloc.h> /* _msize, _aligned_* */

/* The originals, supplied by the linker because of --wrap. */
void *__real_malloc(size_t size);
void *__real_calloc(size_t count, size_t size);
void *__real_realloc(void *block, size_t size);
void __real_free(void *block);
char *__real_strdup(const char *text);
size_t __real__msize(void *block);
/* Wrappers must call __real_* for anything that is itself wrapped: a plain
 * _msize() call inside this file is an undefined reference to _msize, so the
 * linker would redirect it straight back into __wrap__msize -- infinite
 * recursion rather than the CRT's answer. */

/* True when mimalloc made this block, so it must be returned to mimalloc.
 * NULL is nobody's: callers handle it before asking. */
static inline bool mem_override_is_ours(const void *block) {
    return mi_is_in_heap_region(block);
}

/* Waste sanitizer (mem_events.h). The wrapper IS the observation point, so the
 * caller's return address is the allocation site. Compiled to nothing outside
 * the `memwaste` flavour. Frees are reported BEFORE the block goes back. */
#if defined(CBM_MEMWASTE) && CBM_MEMWASTE
#define OBS_ALLOC_F(block, requested, flags)                                    \
    do {                                                                        \
        if ((block) && cbm_memev_enabled()) {                                   \
            cbm_memev_alloc_ex((block), (requested), mi_usable_size(block),     \
                               __builtin_return_address(0), (unsigned)(flags)); \
        }                                                                       \
    } while (0)
#define OBS_ALLOC(block, requested) OBS_ALLOC_F((block), (requested), 0)
#define OBS_REALLOC(old_block, grown, requested)                                        \
    do {                                                                                \
        if ((grown) && cbm_memev_enabled()) {                                           \
            cbm_memev_realloc((old_block), (grown), (requested), mi_usable_size(grown), \
                              __builtin_return_address(0));                             \
        }                                                                               \
    } while (0)
/* A block mimalloc did not make came from the CRT or a system DLL: the layer
 * could never have seen its allocation, so it is not an untracked free. */
#define OBS_FREE(block) \
    (mem_override_is_ours(block) ? cbm_memev_free(block) : cbm_memev_free_foreign(block))
#else
#define OBS_ALLOC(block, requested) ((void)0)
#define OBS_ALLOC_F(block, requested, flags) ((void)0)
#define OBS_REALLOC(old_block, grown, requested) ((void)0)
#define OBS_FREE(block) ((void)0)
#endif

void *__wrap_malloc(size_t size) {
    void *block = mi_malloc(size);
    OBS_ALLOC(block, size);
    return block;
}

void *__wrap_calloc(size_t count, size_t size) {
    void *block = mi_calloc(count, size);
    OBS_ALLOC_F(block, count * size, CBM_MEMEV_ZEROED);
    return block;
}

char *__wrap_strdup(const char *text) {
    char *copy = mi_strdup(text);
    OBS_ALLOC_F(copy, copy ? strlen(copy) + 1 : 0, CBM_MEMEV_WRITTEN);
    return copy;
}

char *__wrap_strndup(const char *text, size_t length) {
    char *copy = mi_strndup(text, length);
    OBS_ALLOC_F(copy, copy ? strlen(copy) + 1 : 0, CBM_MEMEV_WRITTEN);
    return copy;
}

void *__wrap__aligned_malloc(size_t size, size_t alignment) {
    /* Windows spells aligned allocation _aligned_malloc(size, alignment) —
     * note the argument order is the reverse of mi_malloc_aligned's. */
    void *block = mi_malloc_aligned(size, alignment);
    OBS_ALLOC(block, size);
    return block;
}

void __wrap_free(void *block) {
    if (!block) {
        return;
    }
    OBS_FREE(block);
    if (mem_override_is_ours(block)) {
        mi_free(block);
        return;
    }
    /* Allocated inside the CRT or a system DLL, where --wrap could not reach.
     * Returning it to mimalloc would abort (#424), so give it back to its
     * real owner. */
    __real_free(block);
}

void __wrap__aligned_free(void *block) {
    if (!block) {
        return;
    }
    OBS_FREE(block);
    if (mem_override_is_ours(block)) {
        mi_free(block);
        return;
    }
    __real_free(block);
}

void *__wrap_realloc(void *block, size_t size) {
    if (!block) {
        void *fresh = mi_malloc(size);
        OBS_ALLOC(fresh, size);
        return fresh;
    }
    if (mem_override_is_ours(block)) {
        CBM_MEMEV_BACKING(1);
        void *grown = mi_realloc(block, size);
        CBM_MEMEV_BACKING(-1);
        OBS_REALLOC(block, grown, size);
        return grown;
    }
    /* A CRT block cannot be handed to mi_realloc, and the CRT's realloc must
     * not be asked to grow into mimalloc's space either. Copy across the
     * boundary once: allocate from mimalloc, move the smaller of the two
     * sizes, release the original to the CRT. mi_usable_size is unavailable
     * for a foreign block, so the CRT's own accounting bounds the copy. */
    size_t old_size = __real__msize(block);
    void *moved = mi_malloc(size);
    if (!moved) {
        return NULL; /* original stays valid, as realloc requires */
    }
    memcpy(moved, block, old_size < size ? old_size : size);
    OBS_REALLOC(block, moved, size);
    __real_free(block);
    return moved;
}

size_t __wrap__msize(void *block) {
    if (block && mem_override_is_ours(block)) {
        return mi_usable_size(block);
    }
    return block ? __real__msize(block) : 0;
}

#endif /* _WIN32 */
