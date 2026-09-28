/*
 * mem_override_posix.c — allocation-site visibility on POSIX.
 *
 * What is missing on POSIX is a place to OBSERVE allocations: the Windows side
 * gets it for free from the --wrap interposer it needs anyway, and without an
 * equivalent here the profiler could only ever describe Windows, which makes the
 * platform comparison — the one measurement that actually isolates #581 —
 * impossible.
 *
 * CORRECTION (#1360). This header previously claimed that "on POSIX the build
 * links mimalloc's object first, so its strong malloc/free win and every
 * allocation already lands in mimalloc". That was never true, and believing it
 * inverts the meaning of the startup ownership audit. mimalloc emits those
 * strong malloc/free definitions only when MI_MALLOC_OVERRIDE is set — the
 * define its source actually gates alloc-override.c on — while MI_OVERRIDE,
 * which the build sets everywhere, is this project's own prod/test marker that
 * mimalloc never reads. With the override body compiled out there were no
 * strong symbols for link order to prefer, so ordinary malloc went to libc. The
 * shipped v0.9.1-rc.1 binaries confirm it: linux-arm64 (glibc AND musl-static)
 * and darwin-arm64 all reported 0/6 allocator-owned size classes.
 *
 * Linux PROD builds now set MI_MALLOC_OVERRIDE (see Makefile.cbm), so the strong
 * symbols exist there and ordinary malloc genuinely reaches mimalloc. macOS
 * stays off permanently — its two-level namespace would split this binary's
 * free from the system libraries' malloc — and test builds stay off everywhere
 * by construction, which is why this observation shim remains the only way to
 * see POSIX allocation sites.
 *
 * So this file exists purely for symmetry of measurement. Each wrapper forwards
 * to the same mi_* entry point the symbol would have reached on its own and
 * records the block, so both platforms are profiled through the same code path
 * and a difference in the output is a difference in behaviour rather than an
 * artifact of two different instrumentation schemes.
 *
 * Linux only: --wrap is a GNU ld/lld feature. macOS ld has no equivalent; there
 * the waste layer observes through a forwarding malloc zone instead
 * (mem_override_darwin.c), so all three platforms report the same site tables.
 */
#include <mimalloc.h>
#include <stddef.h>
#include <string.h>

#if defined(__linux__)

/* Waste sanitizer (mem_events.h). The wrapper IS the observation point, so the
 * caller's return address is the allocation site. Compiled to nothing outside
 * the `memwaste` flavour. Frees are reported BEFORE the block goes back. */
#if defined(CBM_MEMWASTE) && CBM_MEMWASTE
#include "foundation/mem_events.h"
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
#define OBS_FREE(block) cbm_memev_free(block)
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

void *__wrap_realloc(void *block, size_t size) {
    CBM_MEMEV_BACKING(1);
    void *grown = mi_realloc(block, size);
    CBM_MEMEV_BACKING(-1);
    OBS_REALLOC(block, grown, size);
    return grown;
}

void __wrap_free(void *block) {
    if (!block) {
        return;
    }
    /* Unlike Windows there is no foreign-allocator hazard to route around:
     * every malloc in this image is already mimalloc's. */
    OBS_FREE(block);
    mi_free(block);
}

char *__wrap_strdup(const char *text) {
    char *copy = mi_strdup(text);
    OBS_ALLOC_F(copy, copy ? strlen(copy) + 1 : 0, CBM_MEMEV_WRITTEN);
    return copy;
}

/* The aligned family and strndup reach mimalloc too; without these a block
 * from posix_memalign (the slab allocator's pages) is invisible, and its
 * free reads as untracked. */
char *__wrap_strndup(const char *text, size_t length) {
    char *copy = mi_strndup(text, length);
    OBS_ALLOC_F(copy, copy ? strlen(copy) + 1 : 0, CBM_MEMEV_WRITTEN);
    return copy;
}

int __wrap_posix_memalign(void **out, size_t alignment, size_t size) {
    int rc = mi_posix_memalign(out, alignment, size);
    if (rc == 0 && out) {
        OBS_ALLOC(*out, size);
    }
    return rc;
}

void *__wrap_aligned_alloc(size_t alignment, size_t size) {
    void *block = mi_aligned_alloc(alignment, size);
    OBS_ALLOC(block, size);
    return block;
}

void *__wrap_memalign(size_t alignment, size_t size) {
    void *block = mi_memalign(alignment, size);
    OBS_ALLOC(block, size);
    return block;
}

#endif /* __linux__ */
