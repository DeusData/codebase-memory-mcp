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
 * Linux only: --wrap is a GNU ld/lld feature. macOS ld has no equivalent, so
 * macOS runs census-only and the profiler reports nothing there. That gap is
 * stated in the harness spec rather than hidden behind zeros that look like
 * "no allocations".
 */
#include "foundation/mem_profile.h"

#include <mimalloc.h>
#include <stddef.h>

#if defined(__linux__)

void *__wrap_malloc(size_t size) {
    void *block = mi_malloc(size);
    cbm_mem_profile_alloc(block, size);
    return block;
}

void *__wrap_calloc(size_t count, size_t size) {
    void *block = mi_calloc(count, size);
    cbm_mem_profile_alloc(block, count * size);
    return block;
}

void *__wrap_realloc(void *block, size_t size) {
    if (block) {
        cbm_mem_profile_free(block);
    }
    void *grown = mi_realloc(block, size);
    cbm_mem_profile_alloc(grown, size);
    return grown;
}

void __wrap_free(void *block) {
    if (!block) {
        return;
    }
    /* Unlike Windows there is no foreign-allocator hazard to route around:
     * every malloc in this image is already mimalloc's. */
    cbm_mem_profile_free(block);
    mi_free(block);
}

char *__wrap_strdup(const char *text) {
    char *copy = mi_strdup(text);
    if (copy) {
        cbm_mem_profile_alloc(copy, mi_usable_size(copy));
    }
    return copy;
}

#endif /* __linux__ */
