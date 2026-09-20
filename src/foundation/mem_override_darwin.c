/*
 * mem_override_darwin.c — the macOS allocation observer of the waste sanitizer.
 *
 * WHY THIS FILE EXISTS
 *
 * Linux and Windows observe the malloc family through the linker's --wrap
 * (mem_override_posix.c, mem_override_win.c). macOS ld has no --wrap, and the
 * mimalloc global override is off there permanently: the two-level namespace
 * splits this binary's free from the system libraries' malloc, and a pointer
 * crossing that boundary aborts. So on macOS nothing saw raw malloc at all —
 * the platform the project is developed on was the one platform that could not
 * be measured.
 *
 * HOW IT WORKS
 *
 * A malloc ZONE that forwards every entry to the original default zone and
 * reports the event. Forwarding is the whole safety argument: a block never
 * changes allocator, so there is no boundary for it to cross. The zone claims
 * ownership through size(), which is what routes free() back through it.
 *
 * Proven on 2026-09-17 inside the real indexer (Go corpus): no abort, a block
 * allocated BEFORE installation frees cleanly after it, wall time 41.2 s ->
 * 41.3 s, and 92.3 million raw allocations that nothing had ever counted.
 *
 * THREE TRAPS, each one hit during that proof:
 *   1. malloc_default_zone() returns a VIRTUAL wrapper that dispatches to
 *      whatever is default at call time. Forwarding to it recurses forever once
 *      this zone is the default. The real slot 0 comes from
 *      malloc_get_all_zones().
 *   2. Registration appends. The default is slot 0, and the only way in is the
 *      rotation jemalloc uses: unregister slot 0 (the last zone moves into it),
 *      re-register it (appended), until this zone is first.
 *   3. free() arrives through free_definite_size, not through free.
 *
 * Compiled only into the `memwaste` flavour; dormant unless CBM_MEMWASTE=1.
 */
#if defined(__APPLE__) && defined(CBM_MEMWASTE) && CBM_MEMWASTE

#include "foundation/mem_events.h"

#include <mach-o/dyld.h>
#include <mach-o/getsect.h>
#include <mach/mach.h>
#include <malloc/malloc.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>

enum { OBS_ZONE_VERSION = 9, OBS_MAX_ROTATIONS = 16, OBS_MAX_FRAMES = 24 };

static malloc_zone_t *g_orig;
static malloc_zone_t g_obs;
static uintptr_t g_text_lo;
static uintptr_t g_text_hi;

/* The site of a raw allocation is the first return address inside OUR image:
 * above the zone entry sit libmalloc's own frames, and often libc's (strdup,
 * getpwuid), before the code of ours that caused the call. Frames are walked
 * through the frame-pointer chain, bounded by this thread's stack, so a
 * foreign or corrupt frame ends the walk instead of faulting. */
static void *caller_site(void *frame, unsigned *flags) {
    *flags = 0;
    if (cbm_memev_hint_pending()) {
        return NULL; /* the memory core already named the site */
    }
    void *library_caller = cbm_memev_library_site();
    if (library_caller) {
        *flags = CBM_MEMEV_LIBRARY; /* a FILE buffer, a DIR stream: the library's own */
        return library_caller;
    }
    pthread_t self = pthread_self();
    uintptr_t hi = (uintptr_t)pthread_get_stackaddr_np(self);
    uintptr_t lo = hi - pthread_get_stacksize_np(self);
    uintptr_t *fp = (uintptr_t *)frame;
    void *first = NULL;
    for (int depth = 0; depth < OBS_MAX_FRAMES; depth++) {
        if ((uintptr_t)fp < lo || (uintptr_t)fp + (2 * sizeof(uintptr_t)) > hi ||
            ((uintptr_t)fp & (sizeof(uintptr_t) - 1)) != 0) {
            break;
        }
        uintptr_t ret = fp[1];
        uintptr_t *next = (uintptr_t *)fp[0];
        if (ret == 0) {
            break;
        }
        if (!first) {
            first = (void *)ret;
        }
        if (ret >= g_text_lo && ret < g_text_hi) {
            return (void *)ret;
        }
        if (next <= fp) {
            break; /* the chain must climb */
        }
        fp = next;
    }
    *flags = CBM_MEMEV_FOREIGN;
    return first; /* a system library allocating for itself: observe, never fill */
}

static size_t obs_size(malloc_zone_t *z, const void *p) {
    (void)z;
    return g_orig->size(g_orig, p);
}

static void *obs_malloc(malloc_zone_t *z, size_t n) {
    (void)z;
    void *p = g_orig->malloc(g_orig, n);
    if (p && cbm_memev_enabled()) {
        unsigned flags;
        void *site = caller_site(__builtin_frame_address(0), &flags);
        cbm_memev_alloc_ex(p, n, g_orig->size(g_orig, p), site, flags);
    }
    return p;
}

static void *obs_calloc(malloc_zone_t *z, size_t count, size_t n) {
    (void)z;
    void *p = g_orig->calloc(g_orig, count, n);
    if (p && cbm_memev_enabled()) {
        unsigned flags;
        void *site = caller_site(__builtin_frame_address(0), &flags);
        cbm_memev_alloc_ex(p, count * n, g_orig->size(g_orig, p), site, flags | CBM_MEMEV_ZEROED);
    }
    return p;
}

static void *obs_valloc(malloc_zone_t *z, size_t n) {
    (void)z;
    void *p = g_orig->valloc(g_orig, n);
    if (p && cbm_memev_enabled()) {
        unsigned flags;
        void *site = caller_site(__builtin_frame_address(0), &flags);
        cbm_memev_alloc_ex(p, n, g_orig->size(g_orig, p), site, flags);
    }
    return p;
}

static void *obs_memalign(malloc_zone_t *z, size_t align, size_t n) {
    (void)z;
    void *p = g_orig->memalign(g_orig, align, n);
    if (p && cbm_memev_enabled()) {
        unsigned flags;
        void *site = caller_site(__builtin_frame_address(0), &flags);
        cbm_memev_alloc_ex(p, n, g_orig->size(g_orig, p), site, flags);
    }
    return p;
}

static void *obs_realloc(malloc_zone_t *z, void *p, size_t n) {
    (void)z;
    unsigned flags = 0;
    void *site = cbm_memev_enabled() ? caller_site(__builtin_frame_address(0), &flags) : NULL;
    CBM_MEMEV_BACKING(1);
    void *q = g_orig->realloc(g_orig, p, n);
    CBM_MEMEV_BACKING(-1);
    if (q && cbm_memev_enabled()) {
        cbm_memev_realloc(p, q, n, g_orig->size(g_orig, q), site);
    }
    return q;
}

static void obs_free(malloc_zone_t *z, void *p) {
    (void)z;
    cbm_memev_free(p);
    g_orig->free(g_orig, p);
}

static void obs_free_definite_size(malloc_zone_t *z, void *p, size_t n) {
    (void)z;
    cbm_memev_free(p);
    if (g_orig->free_definite_size) {
        g_orig->free_definite_size(g_orig, p, n);
    } else {
        g_orig->free(g_orig, p);
    }
}

static unsigned obs_batch_malloc(malloc_zone_t *z, size_t size, void **results, unsigned count) {
    (void)z;
    unsigned got = g_orig->batch_malloc ? g_orig->batch_malloc(g_orig, size, results, count) : 0;
    if (cbm_memev_enabled()) {
        unsigned flags;
        void *site = caller_site(__builtin_frame_address(0), &flags);
        for (unsigned i = 0; i < got; i++) {
            cbm_memev_alloc_ex(results[i], size, g_orig->size(g_orig, results[i]), site, flags);
        }
    }
    return got;
}

static void obs_batch_free(malloc_zone_t *z, void **blocks, unsigned count) {
    (void)z;
    for (unsigned i = 0; i < count; i++) {
        cbm_memev_free(blocks[i]);
    }
    if (g_orig->batch_free) {
        g_orig->batch_free(g_orig, blocks, count);
    }
}

static void obs_destroy(malloc_zone_t *z) {
    (void)z;
}

static size_t obs_pressure_relief(malloc_zone_t *z, size_t goal) {
    (void)z;
    return g_orig->pressure_relief ? g_orig->pressure_relief(g_orig, goal) : 0;
}

static malloc_zone_t *slot_zero(void) {
    vm_address_t *zones = NULL;
    unsigned count = 0;
    if (malloc_get_all_zones(mach_task_self(), NULL, &zones, &count) != KERN_SUCCESS ||
        count == 0) {
        return NULL;
    }
    return (malloc_zone_t *)zones[0];
}

__attribute__((constructor)) static void cbm_mem_observe_install(void) {
    if (!cbm_memev_enabled()) {
        return;
    }
    const struct mach_header_64 *hdr = (const struct mach_header_64 *)_dyld_get_image_header(0);
    unsigned long text_size = 0;
    uint8_t *text = hdr ? getsegmentdata(hdr, "__TEXT", &text_size) : NULL;
    g_text_lo = (uintptr_t)text;
    g_text_hi = g_text_lo + text_size;
    cbm_memev_set_image_base((uintptr_t)hdr);

    g_orig = slot_zero();
    if (!g_orig) {
        return;
    }
    memset(&g_obs, 0, sizeof(g_obs));
    g_obs.size = obs_size;
    g_obs.malloc = obs_malloc;
    g_obs.calloc = obs_calloc;
    g_obs.valloc = obs_valloc;
    g_obs.free = obs_free;
    g_obs.realloc = obs_realloc;
    g_obs.destroy = obs_destroy;
    g_obs.zone_name = "cbm_memwaste_observer";
    g_obs.batch_malloc = obs_batch_malloc;
    g_obs.batch_free = obs_batch_free;
    g_obs.introspect = g_orig->introspect; /* leaks/heap keep working */
    g_obs.version = OBS_ZONE_VERSION;
    g_obs.memalign = obs_memalign;
    g_obs.free_definite_size = obs_free_definite_size;
    g_obs.pressure_relief = obs_pressure_relief;
    malloc_zone_register(&g_obs);
    for (int spin = 0; spin < OBS_MAX_ROTATIONS; spin++) {
        malloc_zone_t *first = slot_zero();
        if (!first || first == &g_obs) {
            break;
        }
        malloc_zone_unregister(first);
        malloc_zone_register(first);
    }
    cbm_memev_set_observer_installed(slot_zero() == &g_obs);
}

#else
typedef int cbm_mem_override_darwin_unused_t;
#endif
