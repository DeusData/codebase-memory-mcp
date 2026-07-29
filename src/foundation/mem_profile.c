/*
 * mem_profile.c — see mem_profile.h.
 *
 * Two fixed tables, both open-addressed with linear probing:
 *   sites[]    stack hash -> {frames, live bytes, live blocks, totals}
 *   pointers[] block address -> {site index, size}
 *
 * A single mutex guards both. That is heavy for a hot path and entirely
 * acceptable here: profiling is opt-in, and a diagnostic that is fast but
 * racy would produce numbers nobody can trust — which is the failure mode
 * this whole harness exists to correct.
 */
#include "mem_profile.h"

#include "compat_fs.h"
#include "compat_thread.h"
#include "foundation/constants.h"
#include "platform.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <execinfo.h>
#endif

enum {
    PROFILE_FRAMES = 8,        /* stack depth per site */
    PROFILE_SKIP_FRAMES = 2,   /* this file's own frames */
    PROFILE_SITES = 4096,      /* distinct call sites */
    PROFILE_POINTERS = 262144, /* tracked live blocks */
    PROFILE_DEFAULT_MIN = 32768,
};

typedef struct {
    uint64_t hash; /* 0 = empty slot */
    void *frames[PROFILE_FRAMES];
    int frame_count;
    size_t live_bytes;
    size_t live_blocks;
    size_t total_bytes;
    size_t total_blocks;
    size_t peak_live_bytes;
} profile_site_t;

typedef struct {
    void *block; /* NULL = empty slot */
    size_t size;
    size_t site;
} profile_pointer_t;

/* The profiler runs INSIDE the allocator: every hook here is reached from
 * malloc/free. Anything it calls that itself allocates — reading an env var,
 * the logger, even a probe — re-enters the hook, which recurses until the stack
 * dies (silently: the process produces no stdout, no stderr and no log) and
 * would deadlock the non-recursive mutex on the way. This guard makes the
 * profiler invisible to itself. */
static _Thread_local bool g_profile_reentrant;

static cbm_mutex_t g_profile_mutex;
static profile_site_t g_sites[PROFILE_SITES];
static profile_pointer_t g_pointers[PROFILE_POINTERS];
static cbm_mem_profile_totals_t g_totals;
static atomic_int g_profile_enabled = -1;
static size_t g_profile_min = PROFILE_DEFAULT_MIN;

size_t cbm_mem_profile_threshold(void) {
    (void)cbm_mem_profile_enabled();
    return g_profile_min;
}

bool cbm_mem_profile_enabled(void) {
    int state = atomic_load_explicit(&g_profile_enabled, memory_order_acquire);
    if (state >= 0) {
        return state == 1;
    }
    char buf[CBM_SZ_32];
    bool on = cbm_safe_getenv("CBM_MEM_PROFILE", buf, sizeof(buf), NULL) != NULL && buf[0] == '1';
    if (on) {
        char min_buf[CBM_SZ_32];
        if (cbm_safe_getenv("CBM_MEM_PROFILE_MIN", min_buf, sizeof(min_buf), NULL) != NULL) {
            long parsed = strtol(min_buf, NULL, 10);
            if (parsed > 0) {
                g_profile_min = (size_t)parsed;
            }
        }
        cbm_mutex_init(&g_profile_mutex);
    }
    atomic_store_explicit(&g_profile_enabled, on ? 1 : 0, memory_order_release);
    return on;
}

/* FNV-1a over the return addresses: cheap, and collisions merely merge two
 * sites rather than corrupting counts. */
static uint64_t profile_hash(void *const *frames, int count) {
    uint64_t hash = 1469598103934665603ULL;
    for (int i = 0; i < count; i++) {
        uintptr_t value = (uintptr_t)frames[i];
        for (size_t byte = 0; byte < sizeof(value); byte++) {
            hash ^= (value >> (byte * 8)) & 0xFFU;
            hash *= 1099511628211ULL;
        }
    }
    return hash == 0 ? 1 : hash; /* 0 marks an empty slot */
}

static int profile_capture(void **frames, void *caller) {
#ifdef _WIN32
    int captured = (int)CaptureStackBackTrace(PROFILE_SKIP_FRAMES, PROFILE_FRAMES, frames, NULL);
    if (captured > 0) {
        return captured;
    }
    /* Fall back to the caller the hook supplied. Using a return address from
     * THIS frame would name the profiler, not the allocating code. */
    if (caller) {
        frames[0] = caller;
        return 1;
    }
    frames[0] = __builtin_return_address(0);
    return frames[0] != NULL ? 1 : 0;
#else
    (void)caller;
    void *raw[PROFILE_FRAMES + PROFILE_SKIP_FRAMES];
    int got = backtrace(raw, PROFILE_FRAMES + PROFILE_SKIP_FRAMES);
    int count = got - PROFILE_SKIP_FRAMES;
    if (count <= 0) {
        return 0;
    }
    if (count > PROFILE_FRAMES) {
        count = PROFILE_FRAMES;
    }
    memcpy(frames, raw + PROFILE_SKIP_FRAMES, (size_t)count * sizeof(*frames));
    return count;
#endif
}

/* Returns SIZE_MAX when the table is full, so the caller can count the loss. */
static size_t profile_intern_site_locked(void **frames, int count) {
    uint64_t hash = profile_hash(frames, count);
    size_t index = (size_t)(hash % PROFILE_SITES);
    for (size_t probe = 0; probe < PROFILE_SITES; probe++) {
        profile_site_t *site = &g_sites[index];
        if (site->hash == hash) {
            return index;
        }
        if (site->hash == 0) {
            site->hash = hash;
            site->frame_count = count;
            memcpy(site->frames, frames, (size_t)count * sizeof(*frames));
            g_totals.sites++;
            return index;
        }
        index = (index + 1) % PROFILE_SITES;
    }
    return SIZE_MAX;
}

static size_t profile_pointer_slot(const void *block) {
    uintptr_t value = (uintptr_t)block;
    /* Block addresses are page-aligned at these sizes, so mix the high bits
     * down before masking or every entry lands in the same few buckets. */
    value ^= value >> 20;
    value *= 2654435761U;
    return (size_t)(value % PROFILE_POINTERS);
}

void cbm_mem_profile_alloc(void *block, size_t size) {
    cbm_mem_profile_alloc_at(block, size, NULL);
}

void cbm_mem_profile_alloc_at(void *block, size_t size, void *caller) {
    if (!block || g_profile_reentrant) {
        return;
    }
    g_profile_reentrant = true;
    if (!cbm_mem_profile_enabled() || size < g_profile_min) {
        g_profile_reentrant = false;
        return;
    }
    void *frames[PROFILE_FRAMES];
    int count = profile_capture(frames, caller);
    if (count <= 0) {
        cbm_mutex_lock(&g_profile_mutex);
        g_totals.capture_failed++;
        cbm_mutex_unlock(&g_profile_mutex);
        g_profile_reentrant = false;
        return;
    }
    cbm_mutex_lock(&g_profile_mutex);
    size_t site_index = profile_intern_site_locked(frames, count);
    if (site_index == SIZE_MAX) {
        g_totals.site_table_full++;
        cbm_mutex_unlock(&g_profile_mutex);
        g_profile_reentrant = false;
        return;
    }
    size_t slot = profile_pointer_slot(block);
    bool stored = false;
    for (size_t probe = 0; probe < PROFILE_POINTERS; probe++) {
        if (g_pointers[slot].block == NULL) {
            g_pointers[slot].block = block;
            g_pointers[slot].size = size;
            g_pointers[slot].site = site_index;
            stored = true;
            break;
        }
        slot = (slot + 1) % PROFILE_POINTERS;
    }
    if (!stored) {
        g_totals.pointer_table_full++;
        cbm_mutex_unlock(&g_profile_mutex);
        g_profile_reentrant = false;
        return;
    }
    profile_site_t *site = &g_sites[site_index];
    site->live_bytes += size;
    site->live_blocks++;
    site->total_bytes += size;
    site->total_blocks++;
    if (site->live_bytes > site->peak_live_bytes) {
        site->peak_live_bytes = site->live_bytes;
    }
    g_totals.live_bytes += size;
    g_totals.live_blocks++;
    g_totals.total_bytes += size;
    cbm_mutex_unlock(&g_profile_mutex);
    g_profile_reentrant = false;
}

void cbm_mem_profile_free(void *block) {
    if (!block || g_profile_reentrant) {
        return;
    }
    g_profile_reentrant = true;
    if (!cbm_mem_profile_enabled()) {
        g_profile_reentrant = false;
        return;
    }
    cbm_mutex_lock(&g_profile_mutex);
    size_t slot = profile_pointer_slot(block);
    for (size_t probe = 0; probe < PROFILE_POINTERS; probe++) {
        profile_pointer_t *entry = &g_pointers[slot];
        if (entry->block == block) {
            profile_site_t *site = &g_sites[entry->site];
            site->live_bytes -= entry->size;
            site->live_blocks--;
            g_totals.live_bytes -= entry->size;
            g_totals.live_blocks--;
            entry->block = NULL;
            entry->size = 0;
            entry->site = 0;
            cbm_mutex_unlock(&g_profile_mutex);
            g_profile_reentrant = false;
            return;
        }
        if (entry->block == NULL) {
            break; /* linear probe ended: never tracked (below threshold) */
        }
        slot = (slot + 1) % PROFILE_POINTERS;
    }
    /* Expected for every sub-threshold allocation, so this is a scale check,
     * not an error: it should track the small-allocation rate, not grow with
     * retained bytes. */
    g_totals.untracked_frees++;
    cbm_mutex_unlock(&g_profile_mutex);
    g_profile_reentrant = false;
}

void cbm_mem_profile_totals(cbm_mem_profile_totals_t *out) {
    if (!out) {
        return;
    }
    memset(out, 0, sizeof(*out));
    if (!cbm_mem_profile_enabled()) {
        return;
    }
    cbm_mutex_lock(&g_profile_mutex);
    *out = g_totals;
    cbm_mutex_unlock(&g_profile_mutex);
}

bool cbm_mem_profile_dump(const char *path, const char *label) {
    if (!path || !cbm_mem_profile_enabled()) {
        return false;
    }
    FILE *out = cbm_fopen(path, "ab");
    if (!out) {
        return false;
    }
    cbm_mutex_lock(&g_profile_mutex);
    (void)fprintf(out,
                  "{\"label\":\"%s\",\"threshold\":%zu,\"sites\":%zu,\"live_bytes\":%zu,"
                  "\"live_blocks\":%zu,\"total_bytes\":%zu,\"untracked_frees\":%zu,"
                  "\"site_table_full\":%zu,\"pointer_table_full\":%zu}\n",
                  label ? label : "", g_profile_min, g_totals.sites, g_totals.live_bytes,
                  g_totals.live_blocks, g_totals.total_bytes, g_totals.untracked_frees,
                  g_totals.site_table_full, g_totals.pointer_table_full);
    for (size_t i = 0; i < PROFILE_SITES; i++) {
        profile_site_t *site = &g_sites[i];
        if (site->hash == 0 || site->total_blocks == 0) {
            continue;
        }
        (void)fprintf(out,
                      "{\"label\":\"%s\",\"site\":%llu,\"live_bytes\":%zu,\"live_blocks\":%zu,"
                      "\"total_bytes\":%zu,\"total_blocks\":%zu,\"peak_live\":%zu,\"frames\":[",
                      label ? label : "", (unsigned long long)site->hash, site->live_bytes,
                      site->live_blocks, site->total_bytes, site->total_blocks,
                      site->peak_live_bytes);
        for (int f = 0; f < site->frame_count; f++) {
            (void)fprintf(out, "%s\"%p\"", f == 0 ? "" : ",", site->frames[f]);
        }
        (void)fprintf(out, "]}\n");
    }
    cbm_mutex_unlock(&g_profile_mutex);
    (void)fclose(out);
    return true;
}
