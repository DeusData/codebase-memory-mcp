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

void *__wrap_malloc(size_t size) {
    return mi_malloc(size);
}

void *__wrap_calloc(size_t count, size_t size) {
    return mi_calloc(count, size);
}

char *__wrap_strdup(const char *text) {
    return mi_strdup(text);
}

char *__wrap_strndup(const char *text, size_t length) {
    return mi_strndup(text, length);
}

void *__wrap__aligned_malloc(size_t size, size_t alignment) {
    /* Windows spells aligned allocation _aligned_malloc(size, alignment) —
     * note the argument order is the reverse of mi_malloc_aligned's. */
    return mi_malloc_aligned(size, alignment);
}

void __wrap_free(void *block) {
    if (!block) {
        return;
    }
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
    if (mem_override_is_ours(block)) {
        mi_free(block);
        return;
    }
    __real_free(block);
}

void *__wrap_realloc(void *block, size_t size) {
    if (!block) {
        return mi_malloc(size);
    }
    if (mem_override_is_ours(block)) {
        void *grown = mi_realloc(block, size);
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
