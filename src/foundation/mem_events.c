/*
 * mem_events.c — see mem_events.h.
 *
 * Layout:
 *   sites[]      global, open-addressed, keyed by return address. Keys are
 *                claimed with one CAS; counters are touched only under the
 *                merge lock, from per-thread deltas.
 *   shards[]     the pointer table, 256 independently locked open-addressed
 *                maps. A block is routinely freed by a thread that did not
 *                allocate it, so the pointer table cannot be per-thread.
 *   work[]       CPU work counters keyed by (return address, kind), merged
 *                the same way as the sites.
 *   tl_state     per-thread deltas for every site and work row the thread
 *                touched, dirty lists so a flush walks only those, the core
 *                handshake, the re-entrancy guard and the repeat caches.
 *   access lane  (CBM_MEMWASTE_ACCESS) a shadow map from every 16-byte slot of
 *                a tracked block to a per-block record the compiler's load and
 *                store callbacks update.
 *
 * Everything the layer owns comes from raw virtual memory: it runs INSIDE the
 * allocator it observes and must never call it.
 */
#include "foundation/mem_events.h"

#if (defined(CBM_MEMWASTE) && CBM_MEMWASTE) || \
    (defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS)

#include "foundation/mem.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <fcntl.h> /* _O_WRONLY, _O_CREAT, _O_APPEND, _O_BINARY */
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <unistd.h>
#if defined(__linux__)
#include <link.h> /* dl_iterate_phdr: the load bias of the main program */
#endif
#endif

/* The layer's own code is never instrumented by the access lane: its tables
 * are not program data, and a hook that reached instrumented code would pay
 * for itself on every table probe. */
#if defined(__clang__)
#pragma clang attribute push(__attribute__((no_sanitize("coverage"))), apply_to = function)
#endif

enum {
    MEMEV_SITE_BITS = 16,
    MEMEV_SITE_CAP = 1 << MEMEV_SITE_BITS,
    MEMEV_WORK_BITS = 17,
    MEMEV_WORK_CAP = 1 << MEMEV_WORK_BITS,
    MEMEV_SHARD_BITS = 8,
    MEMEV_SHARDS = 1 << MEMEV_SHARD_BITS,
    MEMEV_SHARD_INIT_BITS = 10,
    MEMEV_SHARD_MAX_BITS = 26,
    MEMEV_LOAD_NUM = 7, /* grow at 70 % occupancy (live + tombstones) */
    MEMEV_LOAD_DEN = 10,
    MEMEV_SEQ_BITS = 24,
    MEMEV_SEQ_MASK = (1 << MEMEV_SEQ_BITS) - 1,
    MEMEV_PHASE_CAP = 63, /* stored in 6 bits */
    MEMEV_PHASE_LABEL = 48,
    MEMEV_LINE = 2048,   /* a site line with every field at 20 digits is ~1,150 bytes */
    MEMEV_PTR_SHIFT = 4, /* allocators align to 16: the low bits carry no entropy */
    MEMEV_REPEAT_SLOTS = 256,
    MEMEV_PATH_BITS = 20,
    MEMEV_PATH_CAP = 1 << MEMEV_PATH_BITS,
    MEMEV_RUN_MIN = 8, /* a run of the fill byte this long was never written */
    MEMEV_FILL_BYTE = 0xA5,
};

#define MEMEV_NO_INDEX UINT32_MAX
#define MEMEV_TOMBSTONE ((uintptr_t)1)
#define MEMEV_FILL_CAP ((size_t)256 * 1024 * 1024) /* never fill (or scan) beyond this */
#define MEMEV_SITE_MASK ((uintptr_t)0x00FFFFFFFFFFFFFFULL)

/* ── raw virtual memory ───────────────────────────────────────────────*/
/* The layer's own footprint, reported in every dump header: a sanitizer that
 * cannot say what it costs cannot be trusted to measure anybody else's waste.
 * On Windows every byte here is committed; on POSIX it is lazily backed. */
static _Atomic uint64_t g_layer_vm_bytes;
static _Atomic int64_t g_layer_threads;

static void *vm_get(size_t bytes) {
#ifdef _WIN32
    void *p = VirtualAlloc(NULL, bytes, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE);
#else
    void *p = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (p == MAP_FAILED) {
        p = NULL;
    }
#endif
    if (p) {
        atomic_fetch_add_explicit(&g_layer_vm_bytes, bytes, memory_order_relaxed);
    }
    return p;
}

static void vm_put(void *p, size_t bytes) {
    if (!p) {
        return;
    }
    atomic_fetch_sub_explicit(&g_layer_vm_bytes, bytes, memory_order_relaxed);
#ifdef _WIN32
    (void)bytes;
    VirtualFree(p, 0, MEM_RELEASE);
#else
    munmap(p, bytes);
#endif
}

/* ── spin lock ────────────────────────────────────────────────────────
 * Held for a handful of instructions around one table probe. A mutex would
 * have to be initialised, and initialisation may allocate. */
typedef atomic_flag memev_lock_t;

static void lock_take(memev_lock_t *l) {
    while (atomic_flag_test_and_set_explicit(l, memory_order_acquire)) {}
}

static void lock_drop(memev_lock_t *l) {
    atomic_flag_clear_explicit(l, memory_order_release);
}

/* ── tables ───────────────────────────────────────────────────────────*/
typedef struct {
    uint64_t allocs;
    uint64_t frees;
    uint64_t reallocs;
    uint64_t requested_bytes;
    uint64_t usable_bytes;
    uint64_t realloc_copy_bytes;
    uint64_t short_lived;
    uint64_t raw_bytes;
    uint64_t scanned_blocks;
    uint64_t never_written_bytes;
    uint64_t over_requested_bytes;
    uint64_t zero_untouched_bytes;
    uint64_t acc_untouched_blocks;
    uint64_t acc_untouched_bytes;
    uint64_t acc_dead_blocks;
    uint64_t acc_dead_bytes;
    uint64_t acc_uninit_reads;
    uint64_t acc_idle_bytes;
    uint64_t acc_opaque_blocks;
    int64_t live_bytes;
    int64_t live_blocks;
} site_counters_t;

typedef struct {
    uint64_t calls;
    uint64_t bytes;
    uint64_t aux1;
    uint64_t aux2;
    uint64_t repeats;
    uint64_t peak;
} work_counters_t;

enum {
    META_PHASE_MASK = 0x3F,
    META_FILLED = 0x40,
    META_ZEROED = 0x80,
};

typedef struct {
    uintptr_t block; /* 0 empty, MEMEV_TOMBSTONE deleted */
    uint64_t usable;
    uint64_t requested;
    uint32_t seq; /* thread slot << MEMEV_SEQ_BITS | that thread's alloc counter */
    uint16_t site;
    int8_t mem_class;
    uint8_t meta; /* phase (6 bits) | FILLED | ZEROED */
} ptr_entry_t;

typedef struct {
    memev_lock_t lock;
    ptr_entry_t *entries;
    uint32_t bits;
    uint32_t used; /* live + tombstones */
    uint32_t live;
} shard_t;

typedef struct {
    const void *table;
    uint64_t key_hash;
    uint64_t generation;
} ht_pair_t;

typedef struct {
    const char *ptr;
    size_t len;
    uint64_t content_hash;
} strlen_pair_t;

typedef struct {
    site_counters_t *delta; /* MEMEV_SITE_CAP entries */
    uint32_t *dirty;        /* site ids touched since the last flush */
    uint8_t *is_dirty;
    uint32_t dirty_count;
    work_counters_t *work_delta; /* MEMEV_WORK_CAP entries */
    uint32_t *work_dirty;
    uint8_t *work_is_dirty;
    uint32_t work_dirty_count;
    uint32_t slot;      /* thread slot, 8 bits */
    uint32_t alloc_seq; /* this thread's allocation counter */
    uint64_t untracked_frees;
    uint64_t pointer_table_full;
    uint64_t site_table_full;
    uint64_t work_table_full;
    strlen_pair_t strlen_cache[MEMEV_REPEAT_SLOTS];
    ht_pair_t ht_cache[MEMEV_REPEAT_SLOTS];
    /* The handshake and the re-entrancy guard live HERE, not in _Thread_local
     * variables. On macOS the first touch of a _Thread_local in a thread
     * allocates its storage THROUGH MALLOC: inside an allocation hook that
     * re-enters the hook, which touches the variable again, until the stack is
     * gone (SIGSEGV at start-up, 2026-09-17; MinGW's emulated TLS allocates the
     * same way). A pthread key / FLS slot never allocates, and the state it
     * points at comes from raw virtual memory. */
    void *hint_site;
    int hint_class;
    bool hint_set;
    bool busy;        /* the layer is invisible to itself */
    uint32_t backing; /* depth of backing allocator calls (access lane) */
    uint64_t ops;     /* allocations + work notes by this thread */
    uint32_t library_depth;
    void *library_site;
#if defined(CBM_MEMWASTE_ACCESS) && CBM_MEMWASTE_ACCESS
    uint32_t acc_ids[512];
    uint32_t acc_id_count;
#endif
} tl_state_t;

static _Atomic uintptr_t g_site_keys[MEMEV_SITE_CAP];
static site_counters_t g_site_counters[MEMEV_SITE_CAP];
static uint64_t g_snap_retained[MEMEV_SITE_CAP];
static uint64_t g_snap_dup_bytes[MEMEV_SITE_CAP];
static uint64_t g_snap_dup_blocks[MEMEV_SITE_CAP];
static _Atomic uintptr_t g_work_keys[MEMEV_WORK_CAP];
static work_counters_t g_work_counters[MEMEV_WORK_CAP];
static _Atomic uint64_t g_path_hashes[MEMEV_PATH_CAP];
static memev_lock_t g_merge_lock = ATOMIC_FLAG_INIT;
static shard_t g_shards[MEMEV_SHARDS];
static _Atomic uint64_t g_untracked_frees;
static _Atomic uint64_t g_foreign_frees;
static _Atomic uint64_t g_pointer_table_full;
static _Atomic uint64_t g_site_table_full;
static _Atomic uint64_t g_work_table_full;
static _Atomic uint32_t g_next_slot;
static _Atomic uint32_t g_phase;
static char g_phase_label[MEMEV_PHASE_LABEL] = "start";
static _Atomic int g_enabled = -1;
static _Atomic int g_fill = -1;
static _Atomic int g_dupes = -1;
static _Atomic bool g_observer_installed;
static _Atomic uintptr_t g_image_base;

static const char *const g_work_names[CBM_WORK_KIND_COUNT] = {
    "memcpy", "memmove", "memset",     "memcmp",   "strlen",       "strcmp",        "strncmp",
    "read",   "write",   "pread",      "pwrite",   "open",         "stat",          "fopen",
    "fread",  "fwrite",  "opendir",    "readdir",  "mutex",        "parallel_for",  "pool_ops",
    "ht_get", "ht_set",  "dead_write", "ct_arena", "ct_dyn_array", "ct_hash_table",
};

const char *cbm_work_kind_name(cbm_work_kind_t kind) {
    if ((int)kind < 0 || kind >= CBM_WORK_KIND_COUNT) {
        return "invalid";
    }
    return g_work_names[kind];
}

static int env_flag(const char *name, int dflt) {
    const char *v = getenv(name);
    if (!v || !v[0]) {
        return dflt;
    }
    return (v[0] == '1' && v[1] == '\0') ? 1 : 0;
}

#if defined(__linux__)
extern char **environ;
#endif

bool cbm_memev_enabled(void) {
    int state = atomic_load_explicit(&g_enabled, memory_order_acquire);
    if (state >= 0) {
        return state == 1;
    }
#if defined(__linux__)
    /* Code can run before libc has set up the environment: the loader's
     * pre-init hooks (the UBSan runtime that -fsanitize-coverage links, which
     * allocates through dlsym) reach this through the malloc and mutex wrappers.
     * getenv answers NULL there, and caching that answer switched the layer
     * off for the whole process (Linux access lane, no dump, 2026-09-17).
     * Decide only once the environment exists. */
    if (!environ) {
        return false;
    }
#endif
    int on = env_flag("CBM_MEMWASTE", 0);
    atomic_store_explicit(&g_enabled, on, memory_order_release);
    return on == 1;
}

static bool fill_enabled(void) {
    int state = atomic_load_explicit(&g_fill, memory_order_relaxed);
    if (state < 0) {
        state = env_flag("CBM_MEMWASTE_FILL", 1);
        atomic_store_explicit(&g_fill, state, memory_order_relaxed);
    }
    return state == 1;
}

static bool dupes_enabled(void) {
    int state = atomic_load_explicit(&g_dupes, memory_order_relaxed);
    if (state < 0) {
        state = env_flag("CBM_MEMWASTE_DUPES", 1);
        atomic_store_explicit(&g_dupes, state, memory_order_relaxed);
    }
    return state == 1;
}

void cbm_memev_scan_for_tests(bool fill, bool dupes) {
    atomic_store_explicit(&g_fill, fill ? 1 : 0, memory_order_relaxed);
    atomic_store_explicit(&g_dupes, dupes ? 1 : 0, memory_order_relaxed);
}

void cbm_memev_force_for_tests(bool on) {
    atomic_store_explicit(&g_enabled, on ? 1 : 0, memory_order_release);
}

void cbm_memev_set_image_base(uintptr_t base) {
    atomic_store_explicit(&g_image_base, base, memory_order_release);
}

void cbm_memev_set_observer_installed(bool installed) {
    atomic_store_explicit(&g_observer_installed, installed, memory_order_release);
}

bool cbm_memev_observer_installed(void) {
    return atomic_load_explicit(&g_observer_installed, memory_order_acquire);
}

/* ── per-thread slot: a key, never _Thread_local (see tl_state_t) ───────*/
static _Atomic int g_key_state; /* 0 none, 1 being created, 2 ready */
#ifdef _WIN32
static DWORD g_key;
#else
static pthread_key_t g_key;
#endif

static size_t tl_bytes(void);
static void tl_release(void *raw);
#ifdef _WIN32
#endif

static bool key_ready(void) {
    int state = atomic_load_explicit(&g_key_state, memory_order_acquire);
    while (state != 2) {
        int expect = 0;
        if (state == 0 &&
            atomic_compare_exchange_strong_explicit(&g_key_state, &expect, 1, memory_order_acq_rel,
                                                    memory_order_acquire)) {
#ifdef _WIN32
            /* A plain TLS slot, released from the loader's thread-detach
             * callback (cbm_memev_thread_end, compat_thread.c). An FLS
             * destructor never ran in the static MinGW link, and by the time
             * the detach callback runs the FLS value is already gone -- the
             * state leaked 22 MB per thread (71 threads by the similarity pass,
             * then commit exhaustion, 2026-09-17). A TLS slot is still readable
             * there. */
            g_key = TlsAlloc();
            bool ok = g_key != TLS_OUT_OF_INDEXES;
#else
            bool ok = pthread_key_create(&g_key, tl_release) == 0;
#endif
            atomic_store_explicit(&g_key_state, ok ? 2 : 0, memory_order_release);
            if (!ok) {
                return false;
            }
        }
        state = atomic_load_explicit(&g_key_state, memory_order_acquire);
    }
    return true;
}

/* A thread whose state was already released keeps this marker in its slot. Its
 * teardown still frees (and on Windows allocates): without the marker every such
 * call would create a fresh 22 MB state that no destructor ever releases --
 * Windows runs an FLS callback once per thread, it does not re-run it the way
 * POSIX re-runs key destructors. Observed as commit exhaustion in the Windows
 * index worker, 2026-09-17. */
static char g_tl_exited_marker;
#define TL_EXITED ((void *)&g_tl_exited_marker)

static void *tl_slot_value(void) {
#ifdef _WIN32
    return TlsGetValue(g_key);
#else
    return pthread_getspecific(g_key);
#endif
}

static tl_state_t *tl_peek(void) {
    if (!key_ready()) {
        return NULL;
    }
    void *v = tl_slot_value();
    return v == TL_EXITED ? NULL : (tl_state_t *)v;
}

static size_t tl_bytes(void) {
    return sizeof(tl_state_t) + ((size_t)MEMEV_SITE_CAP * sizeof(site_counters_t)) +
           ((size_t)MEMEV_SITE_CAP * sizeof(uint32_t)) + (size_t)MEMEV_SITE_CAP +
           ((size_t)MEMEV_WORK_CAP * sizeof(work_counters_t)) +
           ((size_t)MEMEV_WORK_CAP * sizeof(uint32_t)) + (size_t)MEMEV_WORK_CAP;
}

static tl_state_t *tl_get(void) {
    if (!key_ready()) {
        return NULL;
    }
    void *v = tl_slot_value();
    if (v == TL_EXITED) {
        return NULL; /* this thread is ending: observe nothing more */
    }
    if (v) {
        return (tl_state_t *)v;
    }
    char *raw = (char *)vm_get(tl_bytes());
    if (!raw) {
        return NULL;
    }
    tl_state_t *st = (tl_state_t *)raw;
    raw += sizeof(tl_state_t);
    st->delta = (site_counters_t *)raw;
    raw += (size_t)MEMEV_SITE_CAP * sizeof(site_counters_t);
    st->dirty = (uint32_t *)raw;
    raw += (size_t)MEMEV_SITE_CAP * sizeof(uint32_t);
    st->is_dirty = (uint8_t *)raw;
    raw += (size_t)MEMEV_SITE_CAP;
    st->work_delta = (work_counters_t *)raw;
    raw += (size_t)MEMEV_WORK_CAP * sizeof(work_counters_t);
    st->work_dirty = (uint32_t *)raw;
    raw += (size_t)MEMEV_WORK_CAP * sizeof(uint32_t);
    st->work_is_dirty = (uint8_t *)raw;
    atomic_fetch_add_explicit(&g_layer_threads, 1, memory_order_relaxed);
    st->slot = atomic_fetch_add_explicit(&g_next_slot, 1, memory_order_relaxed) &
               ((1U << MEMEV_SHARD_BITS) - 1U);
#ifdef _WIN32
    if (!TlsSetValue(g_key, st)) {
        vm_put(st, tl_bytes());
        return NULL;
    }
#else
    if (pthread_setspecific(g_key, st) != 0) {
        vm_put(st, tl_bytes());
        return NULL;
    }
#endif
    return st;
}

static site_counters_t *tl_site(tl_state_t *st, uint32_t site) {
    if (!st->is_dirty[site]) {
        st->is_dirty[site] = 1;
        st->dirty[st->dirty_count++] = site;
    }
    return &st->delta[site];
}

static work_counters_t *tl_work(tl_state_t *st, uint32_t idx) {
    if (!st->work_is_dirty[idx]) {
        st->work_is_dirty[idx] = 1;
        st->work_dirty[st->work_dirty_count++] = idx;
    }
    return &st->work_delta[idx];
}

/* ── keyed tables ─────────────────────────────────────────────────────*/
static uint64_t mix(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

/* Index of `key` in a CAS-claimed open-addressed key table, claiming a slot on
 * first sight. MEMEV_NO_INDEX when the table is full — counted by the caller,
 * never silently dropped. */
static uint32_t key_index(_Atomic uintptr_t *keys, uint32_t cap, uintptr_t key) {
    uint32_t idx = (uint32_t)(mix((uint64_t)key) & (cap - 1));
    for (uint32_t probe = 0; probe < cap; probe++) {
        uintptr_t seen = atomic_load_explicit(&keys[idx], memory_order_acquire);
        if (seen == key) {
            return idx;
        }
        if (seen == 0) {
            uintptr_t expect = 0;
            if (atomic_compare_exchange_strong_explicit(
                    &keys[idx], &expect, key, memory_order_acq_rel, memory_order_acquire) ||
                expect == key) {
                return idx;
            }
        }
        idx = (idx + 1) & (cap - 1);
    }
    return MEMEV_NO_INDEX;
}

static uint32_t site_index(uintptr_t site) {
    site &= MEMEV_SITE_MASK;
    if (site == 0) {
        site = (uintptr_t)1; /* "unknown caller" is a site like any other */
    }
    return key_index(g_site_keys, MEMEV_SITE_CAP, site);
}

static uint32_t work_index(cbm_work_kind_t kind, uintptr_t site) {
    uintptr_t key = (site & MEMEV_SITE_MASK) | ((uintptr_t)((unsigned)kind + 1U) << 56);
    return key_index(g_work_keys, MEMEV_WORK_CAP, key);
}

/* ── pointer shards ───────────────────────────────────────────────────*/
static shard_t *shard_of(uintptr_t block) {
    return &g_shards[mix((uint64_t)(block >> MEMEV_PTR_SHIFT)) >> (64 - MEMEV_SHARD_BITS)];
}

static uint32_t slot_of(uintptr_t block, uint32_t bits) {
    return (uint32_t)(mix((uint64_t)(block >> MEMEV_PTR_SHIFT)) & ((1U << bits) - 1U));
}

static bool shard_resize(shard_t *sh, uint32_t bits) {
    if (bits > MEMEV_SHARD_MAX_BITS) {
        return false;
    }
    size_t bytes = ((size_t)1 << bits) * sizeof(ptr_entry_t);
    ptr_entry_t *next = (ptr_entry_t *)vm_get(bytes);
    if (!next) {
        return false;
    }
    uint32_t live = 0;
    if (sh->entries) {
        uint32_t old_cap = 1U << sh->bits;
        for (uint32_t i = 0; i < old_cap; i++) {
            ptr_entry_t *e = &sh->entries[i];
            if (e->block == 0 || e->block == MEMEV_TOMBSTONE) {
                continue;
            }
            uint32_t s = slot_of(e->block, bits);
            while (next[s].block != 0) {
                s = (s + 1) & ((1U << bits) - 1U);
            }
            next[s] = *e;
            live++;
        }
        vm_put(sh->entries, ((size_t)1 << sh->bits) * sizeof(ptr_entry_t));
    }
    sh->entries = next;
    sh->bits = bits;
    sh->used = live;
    sh->live = live;
    return true;
}

/* Caller holds the shard lock. */
static ptr_entry_t *shard_insert(shard_t *sh, uintptr_t block) {
    if (!sh->entries && !shard_resize(sh, MEMEV_SHARD_INIT_BITS)) {
        return NULL;
    }
    uint32_t cap = 1U << sh->bits;
    if ((uint64_t)(sh->used + 1) * MEMEV_LOAD_DEN > (uint64_t)cap * MEMEV_LOAD_NUM) {
        /* Mostly tombstones: rehash in place at the same size; otherwise double. */
        uint32_t want = ((uint64_t)sh->live * 2 * MEMEV_LOAD_DEN > (uint64_t)cap * MEMEV_LOAD_NUM)
                            ? sh->bits + 1
                            : sh->bits;
        if (!shard_resize(sh, want)) {
            return NULL;
        }
        cap = 1U << sh->bits;
    }
    uint32_t s = slot_of(block, sh->bits);
    ptr_entry_t *grave = NULL;
    for (uint32_t probe = 0; probe < cap; probe++) {
        ptr_entry_t *e = &sh->entries[s];
        if (e->block == block) {
            return e; /* address reuse without a free we saw: overwrite */
        }
        if (e->block == MEMEV_TOMBSTONE && !grave) {
            grave = e;
        } else if (e->block == 0) {
            ptr_entry_t *dst = grave ? grave : e;
            if (!grave) {
                sh->used++;
            }
            sh->live++;
            dst->block = block;
            return dst;
        }
        s = (s + 1) & (cap - 1);
    }
    return NULL;
}

/* Caller holds the shard lock. */
static ptr_entry_t *shard_find(shard_t *sh, uintptr_t block) {
    if (!sh->entries) {
        return NULL;
    }
    uint32_t cap = 1U << sh->bits;
    uint32_t s = slot_of(block, sh->bits);
    for (uint32_t probe = 0; probe < cap; probe++) {
        ptr_entry_t *e = &sh->entries[s];
        if (e->block == block) {
            return e;
        }
        if (e->block == 0) {
            return NULL;
        }
        s = (s + 1) & (cap - 1);
    }
    return NULL;
}

/* ── fill pattern and scan ────────────────────────────────────────────
 * A new block is filled with MEMEV_FILL_BYTE. At free, a run of at least
 * MEMEV_RUN_MIN fill bytes is memory nobody ever wrote, and the last byte that
 * is NOT the fill byte is the high-water mark of what was written. Runs are
 * counted at word granularity (aligned 8-byte words), which is within 7 bytes
 * per run edge and fast enough for hundreds of gigabytes. A calloc block is
 * scanned the same way for zero bytes, which is an upper bound: a written zero
 * is indistinguishable from an untouched one. */
typedef struct {
    uint64_t run_bytes;
    uint64_t high_water;
} scan_t;

static scan_t scan_block(const uint8_t *p, size_t n, uint8_t pat) {
    scan_t out = {0, 0};
    uint64_t patw = 0x0101010101010101ULL * pat;
    size_t hw = n;
    while (hw > 0 && (hw & 7U) != 0 && p[hw - 1] == pat) {
        hw--;
    }
    while (hw >= 8) {
        uint64_t w;
        memcpy(&w, p + hw - 8, 8);
        if (w != patw) {
            break;
        }
        hw -= 8;
    }
    while (hw > 0 && p[hw - 1] == pat) {
        hw--;
    }
    size_t tail = n - hw;
    if (tail < MEMEV_RUN_MIN) {
        hw = n; /* a short tail of the fill byte is as likely written as not */
        tail = 0;
    }
    uint64_t runs = tail;
    size_t i = 0;
    while (i + 8 <= hw) {
        uint64_t w;
        memcpy(&w, p + i, 8);
        if (w == patw) {
            runs += 8;
        }
        i += 8;
    }
    out.run_bytes = runs;
    out.high_water = hw;
    return out;
}

/* ── access lane ──────────────────────────────────────────────────────*/
#if defined(CBM_MEMWASTE_ACCESS) && CBM_MEMWASTE_ACCESS
enum {
    /* 48-bit user address space: arm64 Linux maps the program and its heap
     * above 2^47 (0xaaaa... and 0xffff...), which a 47-bit map silently ignored. */
    ACC_L1_BITS = 16,
    ACC_L1_CAP = 1 << ACC_L1_BITS,
    ACC_SLOT_SHIFT = 4,
    ACC_REGION_SLOTS = 1 << 28, /* one 4 GB region in 16-byte slots */
    ACC_RECS_BITS = 27,
    ACC_WRITTEN = 1,
    ACC_READ = 2,
    ACC_READ_BEFORE_WRITE = 4,
    ACC_ID_BATCH = 256,
};

typedef struct {
    _Atomic uint8_t flags;
    uint8_t last_phase;
    uint8_t zeroed;
    uint8_t filled; /* holds the fill pattern: a byte that differs was written by someone */
    uint32_t site;
    uint64_t usable;
    _Atomic uintptr_t writer;
} acc_rec_t;

static _Atomic(uint32_t *) g_acc_l2[ACC_L1_CAP];
static acc_rec_t *g_acc_recs;
static uint32_t *g_acc_free;
static uint32_t g_acc_free_count;
static uint32_t g_acc_next = 1;
static memev_lock_t g_acc_lock = ATOMIC_FLAG_INIT;
static _Atomic int g_acc_ready;

static bool acc_init(void) {
    if (atomic_load_explicit(&g_acc_ready, memory_order_acquire) == 2) {
        return true;
    }
    lock_take(&g_acc_lock);
    if (atomic_load_explicit(&g_acc_ready, memory_order_relaxed) != 2) {
        g_acc_recs = (acc_rec_t *)vm_get(((size_t)1 << ACC_RECS_BITS) * sizeof(acc_rec_t));
        g_acc_free = (uint32_t *)vm_get(((size_t)1 << ACC_RECS_BITS) * sizeof(uint32_t));
        atomic_store_explicit(&g_acc_ready, (g_acc_recs && g_acc_free) ? 2 : 1,
                              memory_order_release);
    }
    lock_drop(&g_acc_lock);
    return atomic_load_explicit(&g_acc_ready, memory_order_acquire) == 2;
}

static uint32_t *acc_region(uint32_t l1, bool create) {
    uint32_t *l2 = atomic_load_explicit(&g_acc_l2[l1], memory_order_acquire);
    if (l2 || !create) {
        return l2;
    }
    uint32_t *fresh = (uint32_t *)vm_get((size_t)ACC_REGION_SLOTS * sizeof(uint32_t));
    if (!fresh) {
        return NULL;
    }
    uint32_t *expect = NULL;
    if (!atomic_compare_exchange_strong_explicit(&g_acc_l2[l1], &expect, fresh,
                                                 memory_order_acq_rel, memory_order_acquire)) {
        vm_put(fresh, (size_t)ACC_REGION_SLOTS * sizeof(uint32_t));
        return expect;
    }
    return fresh;
}

/* Write `id` into every slot of [a, a + n). */
static void acc_map(uintptr_t a, uint64_t n, uint32_t id) {
    uintptr_t end = a + (uintptr_t)(n ? n : 1);
    while (a < end) {
        uint32_t l1 = (uint32_t)(a >> 32);
        if (l1 >= ACC_L1_CAP) {
            return;
        }
        uint32_t *l2 = acc_region(l1, id != 0);
        uintptr_t region_end = ((uintptr_t)l1 + 1) << 32;
        uintptr_t stop = end < region_end ? end : region_end;
        if (l2) {
            uint64_t first = ((uint32_t)a) >> ACC_SLOT_SHIFT;
            uint64_t last = (stop == region_end) ? (uint64_t)ACC_REGION_SLOTS
                                                 : ((((uint32_t)(stop - 1)) >> ACC_SLOT_SHIFT) + 1);
            for (uint64_t s = first; s < last; s++) {
                l2[s] = id;
            }
        }
        a = stop;
    }
}

static uint32_t acc_lookup(uintptr_t a) {
    uint32_t l1 = (uint32_t)(a >> 32);
    if (l1 >= ACC_L1_CAP) {
        return 0;
    }
    uint32_t *l2 = atomic_load_explicit(&g_acc_l2[l1], memory_order_relaxed);
    return l2 ? l2[((uint32_t)a) >> ACC_SLOT_SHIFT] : 0;
}

static uint32_t acc_take_id(tl_state_t *st) {
    if (st->acc_id_count == 0) {
        lock_take(&g_acc_lock);
        while (st->acc_id_count < ACC_ID_BATCH) {
            uint32_t id;
            if (g_acc_free_count > 0) {
                id = g_acc_free[--g_acc_free_count];
            } else if (g_acc_next < (1U << ACC_RECS_BITS)) {
                id = g_acc_next++;
            } else {
                break;
            }
            st->acc_ids[st->acc_id_count++] = id;
        }
        lock_drop(&g_acc_lock);
    }
    return st->acc_id_count ? st->acc_ids[--st->acc_id_count] : 0;
}

static void acc_give_id(tl_state_t *st, uint32_t id) {
    if (st->acc_id_count < (uint32_t)(sizeof(st->acc_ids) / sizeof(st->acc_ids[0]))) {
        st->acc_ids[st->acc_id_count++] = id;
        return;
    }
    lock_take(&g_acc_lock);
    g_acc_free[g_acc_free_count++] = id;
    while (st->acc_id_count > ACC_ID_BATCH) {
        g_acc_free[g_acc_free_count++] = st->acc_ids[--st->acc_id_count];
    }
    lock_drop(&g_acc_lock);
}

static void acc_track(tl_state_t *st, uintptr_t block, uint64_t usable, uint32_t site, bool zeroed,
                      bool filled, bool written) {
    if (!acc_init()) {
        return;
    }
    uint32_t id = acc_take_id(st);
    if (!id) {
        return;
    }
    acc_rec_t *r = &g_acc_recs[id];
    /* A block that already holds content when the layer hears of it (the
     * Linux/Windows strdup wrappers report AFTER the copy) was written by the
     * allocation itself: without this every such block read a first time was a
     * read-before-write -- 16.5 M false verdicts on Linux, 12.6 M of them the
     * reach-cache key copies (2026-09-17). */
    atomic_store_explicit(&r->flags, written ? ACC_WRITTEN : 0, memory_order_relaxed);
    atomic_store_explicit(&r->writer, 0, memory_order_relaxed);
    r->last_phase = (uint8_t)atomic_load_explicit(&g_phase, memory_order_relaxed);
    r->zeroed = zeroed ? 1 : 0;
    r->filled = filled ? 1 : 0;
    r->site = site;
    r->usable = usable;
    acc_map(block, usable, id);
}

static void work_add(tl_state_t *st, cbm_work_kind_t kind, uintptr_t site, uint64_t bytes,
                     uint64_t aux1, uint64_t aux2, uint64_t repeats);

/* The verdict on a block that is going away: what its life was spent on.
 * `scan_written` is the fill scan's word on it (1 written, 0 not, -1 no scan):
 * a library call the compiler did not instrument (snprintf, strcpy, qsort)
 * writes without a store callback, and the pattern still shows it. */
static void acc_untrack(tl_state_t *st, uintptr_t block, uint64_t usable, int scan_written) {
    if (atomic_load_explicit(&g_acc_ready, memory_order_relaxed) != 2) {
        return;
    }
    uint32_t id = acc_lookup(block);
    if (!id) {
        return;
    }
    acc_rec_t *r = &g_acc_recs[id];
    uint8_t f = atomic_load_explicit(&r->flags, memory_order_relaxed);
    site_counters_t *c = tl_site(st, r->site);
    /* OPAQUE: the fill pattern shows writes, yet no callback ever fired. The
     * block was used only by code the lane cannot see -- an uninstrumented
     * library, or an aggregate copy (`*dst = *src` of a struct) that the
     * compiler expands inline after instrumentation, emitting no load or store
     * callback. Its reads are invisible too, so neither "untouched" nor
     * "written, never read" nor "idle" can be claimed about it. */
    bool opaque = scan_written == 1 && !(f & (ACC_WRITTEN | ACC_READ));
    if (opaque) {
        c->acc_opaque_blocks++;
    } else if (!(f & (ACC_WRITTEN | ACC_READ))) {
        c->acc_untouched_blocks++;
        c->acc_untouched_bytes += r->usable;
    } else if ((f & ACC_WRITTEN) && !(f & ACC_READ)) {
        /* Seen written, never seen read. A read through an inline struct copy is
         * invisible, so this is "no instrumented read", named as such in reports. */
        c->acc_dead_blocks++;
        c->acc_dead_bytes += r->usable;
        uintptr_t writer = atomic_load_explicit(&r->writer, memory_order_relaxed);
        if (writer) { /* 0: only a library wrote it -- counted at the site, no writer to name */
            work_add(st, CBM_WORK_DEAD_WRITE, writer, r->usable, 1, 0, 0);
        }
    }
    if ((f & ACC_READ_BEFORE_WRITE) && !r->zeroed) {
        c->acc_uninit_reads++;
    }
    if (!opaque && r->last_phase < (uint8_t)atomic_load_explicit(&g_phase, memory_order_relaxed)) {
        c->acc_idle_bytes += r->usable;
    }
    acc_map(block, usable, 0);
    acc_give_id(st, id);
}

static uint32_t acc_move(uintptr_t old_block, uint64_t old_usable, uintptr_t grown,
                         uint64_t usable) {
    if (atomic_load_explicit(&g_acc_ready, memory_order_relaxed) != 2) {
        return 0;
    }
    uint32_t id = acc_lookup(old_block);
    if (!id) {
        return 0;
    }
    acc_map(old_block, old_usable, 0);
    g_acc_recs[id].usable = usable;
    acc_map(grown, usable, id);
    return id;
}

static inline void acc_touch(uintptr_t a, bool store, uintptr_t pc) {
    uint32_t id = acc_lookup(a);
    if (!id) {
        return;
    }
    acc_rec_t *r = &g_acc_recs[id];
    uint8_t f = atomic_load_explicit(&r->flags, memory_order_relaxed);
    if (store) {
        if (!(f & ACC_WRITTEN)) {
            atomic_fetch_or_explicit(&r->flags, ACC_WRITTEN, memory_order_relaxed);
            atomic_store_explicit(&r->writer, pc, memory_order_relaxed);
        }
    } else if (!(f & ACC_READ)) {
        uint8_t mark = ACC_READ;
        if (!(f & ACC_WRITTEN)) {
            /* The program is about to read this byte, so looking at it is safe. */
            bool library_wrote = r->filled && *(const uint8_t *)a != MEMEV_FILL_BYTE;
            mark |= library_wrote ? ACC_WRITTEN : ACC_READ_BEFORE_WRITE;
        }
        atomic_fetch_or_explicit(&r->flags, mark, memory_order_relaxed);
    }
    uint8_t ph = (uint8_t)atomic_load_explicit(&g_phase, memory_order_relaxed);
    if (r->last_phase != ph) {
        r->last_phase = ph;
    }
}

#define ACC_PC() ((uintptr_t)__builtin_return_address(0))
void __sanitizer_cov_load1(uint8_t *a) {
    acc_touch((uintptr_t)a, false, 0);
}
void __sanitizer_cov_load2(uint16_t *a) {
    acc_touch((uintptr_t)a, false, 0);
}
void __sanitizer_cov_load4(uint32_t *a) {
    acc_touch((uintptr_t)a, false, 0);
}
void __sanitizer_cov_load8(uint64_t *a) {
    acc_touch((uintptr_t)a, false, 0);
}
void __sanitizer_cov_load16(__int128 *a) {
    acc_touch((uintptr_t)a, false, 0);
}
void __sanitizer_cov_store1(uint8_t *a) {
    acc_touch((uintptr_t)a, true, ACC_PC());
}
void __sanitizer_cov_store2(uint16_t *a) {
    acc_touch((uintptr_t)a, true, ACC_PC());
}
void __sanitizer_cov_store4(uint32_t *a) {
    acc_touch((uintptr_t)a, true, ACC_PC());
}
void __sanitizer_cov_store8(uint64_t *a) {
    acc_touch((uintptr_t)a, true, ACC_PC());
}
void __sanitizer_cov_store16(__int128 *a) {
    acc_touch((uintptr_t)a, true, ACC_PC());
}
void __sanitizer_cov_bool_flag_init(uint8_t *start, uint8_t *stop) {
    (void)start;
    (void)stop;
}

/* Verdicts are per block, so touching both ends of the range marks every block
 * it can belong to (a range never spans two allocations in a correct program). */
void cbm_memev_access(const void *p, size_t n, bool store, void *pc) {
    if (!p || n == 0 || atomic_load_explicit(&g_acc_ready, memory_order_relaxed) != 2) {
        return;
    }
    tl_state_t *st = tl_get();
    if (!st || st->busy || st->backing) {
        return;
    }
    uintptr_t a = (uintptr_t)p;
    acc_touch(a, store, (uintptr_t)pc);
    if (n > 1) {
        acc_touch(a + (uintptr_t)(n - 1), store, (uintptr_t)pc);
    }
}

void cbm_memev_backing(int delta) {
    tl_state_t *st = tl_get();
    if (st) {
        st->backing = (uint32_t)((int)st->backing + delta);
    }
}
#else
static void work_add(tl_state_t *st, cbm_work_kind_t kind, uintptr_t site, uint64_t bytes,
                     uint64_t aux1, uint64_t aux2, uint64_t repeats);
#endif /* CBM_MEMWASTE_ACCESS */

/* ── the handshake ────────────────────────────────────────────────────*/
void cbm_memev_hint(void *site, int mem_class) {
    if (!cbm_memev_enabled()) {
        return;
    }
    tl_state_t *st = tl_get();
    if (st) {
        st->hint_site = site;
        st->hint_class = mem_class;
        st->hint_set = true;
    }
}

bool cbm_memev_hint_pending(void) {
    if (!cbm_memev_enabled()) {
        return false;
    }
    const tl_state_t *st = tl_peek();
    return st && st->hint_set;
}

/* ── events ───────────────────────────────────────────────────────────*/
static void record_alloc(tl_state_t *st, uintptr_t block, size_t requested, size_t usable,
                         uintptr_t site, int mem_class, unsigned flags) {
    st->ops++;
    uint32_t sid = site_index(site);
    if (sid == MEMEV_NO_INDEX) {
        st->site_table_full++;
        return;
    }
    uint64_t charged = usable ? usable : requested;
    st->alloc_seq = (st->alloc_seq + 1) & (uint32_t)MEMEV_SEQ_MASK;

    bool zeroed = (flags & CBM_MEMEV_ZEROED) != 0;
    bool filled = false;
    if (!zeroed && !(flags & (CBM_MEMEV_FOREIGN | CBM_MEMEV_WRITTEN)) &&
        charged <= MEMEV_FILL_CAP && fill_enabled()) {
        memset((void *)block, MEMEV_FILL_BYTE, (size_t)charged);
        filled = true;
    }

    shard_t *sh = shard_of(block);
    lock_take(&sh->lock);
    ptr_entry_t *e = shard_insert(sh, block);
    if (e) {
        e->usable = charged;
        e->requested = requested;
        e->seq = (st->slot << MEMEV_SEQ_BITS) | st->alloc_seq;
        e->site = (uint16_t)sid;
        e->mem_class = (int8_t)mem_class;
        e->meta =
            (uint8_t)((atomic_load_explicit(&g_phase, memory_order_relaxed) & META_PHASE_MASK) |
                      (filled ? META_FILLED : 0) | (zeroed ? META_ZEROED : 0));
    }
    lock_drop(&sh->lock);
    if (!e) {
        st->pointer_table_full++;
    }

    site_counters_t *c = tl_site(st, sid);
    c->allocs++;
    c->requested_bytes += requested;
    c->usable_bytes += charged;
    if (mem_class == CBM_MEMEV_CLASS_RAW) {
        c->raw_bytes += charged;
    }
    if (e) {
        c->live_bytes += (int64_t)charged;
        c->live_blocks++;
#if defined(CBM_MEMWASTE_ACCESS) && CBM_MEMWASTE_ACCESS
        if (!(flags & (CBM_MEMEV_FOREIGN | CBM_MEMEV_LIBRARY))) {
            acc_track(st, block, charged, sid, zeroed, filled, (flags & CBM_MEMEV_WRITTEN) != 0);
        }
#endif
    }
}

void cbm_memev_alloc_ex(void *block, size_t requested, size_t usable, void *site, unsigned flags) {
    if (!cbm_memev_enabled()) {
        return;
    }
    tl_state_t *st = tl_get();
    if (!st || st->busy) {
        return;
    }
    bool hinted = st->hint_set;
    st->hint_set = false; /* consumed even when the allocation failed */
    if (!block) {
        return;
    }
    st->busy = true;
    record_alloc(st, (uintptr_t)block, requested, usable,
                 (uintptr_t)(hinted ? st->hint_site : site),
                 hinted ? st->hint_class : CBM_MEMEV_CLASS_RAW,
                 hinted ? (flags & ~(unsigned)CBM_MEMEV_FOREIGN) : flags);
    st->busy = false;
}

void cbm_memev_alloc(void *block, size_t requested, size_t usable, void *site) {
    cbm_memev_alloc_ex(block, requested, usable, site, 0);
}

/* Removes the record of `block`. Returns false when the layer never saw it. */
static bool take_entry(uintptr_t block, ptr_entry_t *out) {
    shard_t *sh = shard_of(block);
    lock_take(&sh->lock);
    ptr_entry_t *e = shard_find(sh, block);
    bool found = e != NULL;
    if (found) {
        *out = *e;
        e->block = MEMEV_TOMBSTONE;
        sh->live--;
    }
    lock_drop(&sh->lock);
    return found;
}

void cbm_memev_free(void *block) {
    if (!block || !cbm_memev_enabled()) {
        return;
    }
    tl_state_t *st = tl_get();
    if (!st || st->busy) {
        return;
    }
    st->busy = true;
    ptr_entry_t e;
    if (take_entry((uintptr_t)block, &e)) {
        site_counters_t *c = tl_site(st, e.site);
        c->frees++;
        c->live_bytes -= (int64_t)e.usable;
        c->live_blocks--;
        uint32_t slot = e.seq >> MEMEV_SEQ_BITS;
        uint32_t age =
            (st->alloc_seq - (e.seq & (uint32_t)MEMEV_SEQ_MASK)) & (uint32_t)MEMEV_SEQ_MASK;
        if (slot == st->slot && age <= CBM_MEMEV_CHURN_WINDOW) {
            c->short_lived++;
        }
        int scan_written = -1;
        if ((e.meta & (META_FILLED | META_ZEROED)) && e.usable <= MEMEV_FILL_CAP) {
            bool zeroed = (e.meta & META_ZEROED) != 0;
            scan_t sc =
                scan_block((const uint8_t *)block, (size_t)e.usable, zeroed ? 0 : MEMEV_FILL_BYTE);
            scan_written = sc.high_water > 0 ? 1 : 0;
            c->scanned_blocks++;
            if (zeroed) {
                c->zero_untouched_bytes += e.usable - sc.high_water;
            } else {
                c->never_written_bytes += sc.run_bytes;
                if (e.requested > sc.high_water) {
                    c->over_requested_bytes += e.requested - sc.high_water;
                }
            }
        }
#if defined(CBM_MEMWASTE_ACCESS) && CBM_MEMWASTE_ACCESS
        acc_untrack(st, (uintptr_t)block, e.usable, scan_written);
#else
        (void)scan_written;
#endif
    } else {
        st->untracked_frees++;
    }
    st->busy = false;
}

void cbm_memev_free_foreign(void *block) {
    if (!block || !cbm_memev_enabled()) {
        return;
    }
    atomic_fetch_add_explicit(&g_foreign_frees, 1, memory_order_relaxed);
}

void cbm_memev_realloc(void *old_block, void *grown, size_t requested, size_t usable, void *site) {
    if (!cbm_memev_enabled()) {
        return;
    }
    tl_state_t *st = tl_get();
    if (!st || st->busy) {
        return;
    }
    bool hinted = st->hint_set;
    void *hint_site = st->hint_site;
    int hint_class = st->hint_class;
    st->hint_set = false;
    if (!grown) {
        return;
    }
    st->busy = true;
    ptr_entry_t e;
    bool had = old_block && take_entry((uintptr_t)old_block, &e);
    if (had) {
        /* The record keeps its ORIGINAL site: a realloc chain is one object
         * growing, and its copies are that object's cost. */
        site_counters_t *c = tl_site(st, e.site);
        c->reallocs++;
        c->live_bytes -= (int64_t)e.usable;
        c->live_blocks--;
        if (grown != old_block) {
            c->realloc_copy_bytes += e.usable < requested ? e.usable : requested;
        }
        uint64_t charged = usable ? usable : requested;
        uint8_t meta = e.meta;
        if ((meta & META_FILLED) && charged > e.usable && charged <= MEMEV_FILL_CAP) {
            /* the new tail is uninitialised: it gets the pattern too */
            memset((char *)grown + e.usable, MEMEV_FILL_BYTE, (size_t)(charged - e.usable));
        } else if (charged > e.usable) {
            meta &= (uint8_t)~(META_FILLED | META_ZEROED); /* mixed content: stop scanning */
        }
        shard_t *sh = shard_of((uintptr_t)grown);
        lock_take(&sh->lock);
        ptr_entry_t *n = shard_insert(sh, (uintptr_t)grown);
        if (n) {
            n->usable = charged;
            n->requested = requested;
            n->seq = e.seq;
            n->site = e.site;
            n->mem_class = e.mem_class;
            n->meta = meta;
        }
        lock_drop(&sh->lock);
        c->requested_bytes += requested;
        c->usable_bytes += charged;
        if (e.mem_class == CBM_MEMEV_CLASS_RAW) {
            c->raw_bytes += charged;
        }
        if (n) {
            c->live_bytes += (int64_t)charged;
            c->live_blocks++;
#if defined(CBM_MEMWASTE_ACCESS) && CBM_MEMWASTE_ACCESS
            (void)acc_move((uintptr_t)old_block, e.usable, (uintptr_t)grown, charged);
#endif
        } else {
            st->pointer_table_full++;
        }
    } else {
        /* The old block was never seen (allocated before the observer, or by a
         * path it does not cover): the grown block already holds the program's
         * data, so it is recorded without the fill. realloc(NULL, n) is a fresh
         * allocation and gets the fill like any malloc. */
        record_alloc(st, (uintptr_t)grown, requested, usable,
                     (uintptr_t)(hinted ? hint_site : site),
                     hinted ? hint_class : CBM_MEMEV_CLASS_RAW,
                     old_block ? CBM_MEMEV_WRITTEN : 0); /* realloc(NULL, n) is a malloc */
    }
    st->busy = false;
}

/* ── CPU work ─────────────────────────────────────────────────────────*/
static void work_add(tl_state_t *st, cbm_work_kind_t kind, uintptr_t site, uint64_t bytes,
                     uint64_t aux1, uint64_t aux2, uint64_t repeats) {
    st->ops++;
    uint32_t idx = work_index(kind, site);
    if (idx == MEMEV_NO_INDEX) {
        st->work_table_full++;
        return;
    }
    work_counters_t *w = tl_work(st, idx);
    w->calls++;
    w->bytes += bytes;
    w->aux1 += aux1;
    w->aux2 += aux2;
    w->repeats += repeats;
    bool container = kind == CBM_WORK_CT_ARENA || kind == CBM_WORK_CT_DYN_ARRAY ||
                     kind == CBM_WORK_CT_HASH_TABLE;
    uint64_t single = container ? (bytes > aux1 ? bytes - aux1 : 0) : bytes;
    if (single > w->peak) {
        w->peak = single;
    }
}

void cbm_memev_library_enter(void *site) {
    tl_state_t *st = cbm_memev_enabled() ? tl_get() : NULL;
    if (st && st->library_depth++ == 0) {
        st->library_site = site;
    }
}

void cbm_memev_library_leave(void) {
    tl_state_t *st = cbm_memev_enabled() ? tl_get() : NULL;
    if (st && st->library_depth > 0 && --st->library_depth == 0) {
        st->library_site = NULL;
    }
}

void *cbm_memev_library_site(void) {
    tl_state_t *st = cbm_memev_enabled() ? tl_get() : NULL;
    return st ? st->library_site : NULL;
}

#if defined(CBM_MEMWASTE_PROFILE) && CBM_MEMWASTE_PROFILE
void __llvm_profile_set_filename(const char *pattern);
int __llvm_profile_write_file(void);
#endif

void cbm_memev_process_role(const char *role) {
#if defined(CBM_MEMWASTE_PROFILE) && CBM_MEMWASTE_PROFILE
    const char *dir = getenv("CBM_PROFILE_DIR");
    if (!dir || !dir[0] || !role) {
        return;
    }
    /* The runtime keeps the pointer, not a copy: the pattern must outlive us. */
    static char pattern[1024];
    int n = snprintf(pattern, sizeof(pattern), "%s/%s-%%p.profraw", dir, role);
    if (n > 0 && (size_t)n < sizeof(pattern)) {
        __llvm_profile_set_filename(pattern);
    }
#else
    (void)role;
#endif
}

void cbm_memev_process_exit(void) {
    if (cbm_memev_enabled()) {
        const char *out = getenv("CBM_MEMWASTE_OUT");
        if (out && out[0]) {
            (void)cbm_memev_dump(out, "exit");
        }
    }
#if defined(CBM_MEMWASTE_PROFILE) && CBM_MEMWASTE_PROFILE
    (void)__llvm_profile_write_file();
#endif
}

uint64_t cbm_memev_thread_ops(void) {
    if (!cbm_memev_enabled()) {
        return 0;
    }
    tl_state_t *st = tl_get();
    return st ? st->ops : 0;
}

void cbm_work_note(cbm_work_kind_t kind, void *site, uint64_t bytes, uint64_t aux1, uint64_t aux2) {
    if ((int)kind < 0 || kind >= CBM_WORK_KIND_COUNT || !cbm_memev_enabled()) {
        return;
    }
    tl_state_t *st = tl_get();
    if (!st || st->busy) {
        return;
    }
    st->busy = true;
    work_add(st, kind, (uintptr_t)site, bytes, aux1, aux2, 0);
    st->busy = false;
}

__attribute__((noinline)) void cbm_memev_da_note(size_t cap_bytes, size_t used_bytes,
                                                 size_t cap_slots) {
    if (!cbm_memev_enabled() || cap_bytes == 0) {
        return;
    }
    uint64_t growths = 0;
    for (size_t c = 8; c < cap_slots; c <<= 1) {
        growths++;
    }
    cbm_work_note(CBM_WORK_CT_DYN_ARRAY, __builtin_return_address(0), cap_bytes, used_bytes,
                  growths);
}

/* FNV-1a over the first bytes of a string of known length; the length is
 * mixed in, so two strings that share a long prefix still differ. */
enum { MEMEV_CONTENT_HASH_MAX = 4096 };
static uint64_t bytes_hash(const char *s, size_t len) {
    uint64_t h = 0xcbf29ce484222325ULL ^ (uint64_t)len;
    size_t n = len < MEMEV_CONTENT_HASH_MAX ? len : MEMEV_CONTENT_HASH_MAX;
    for (size_t i = 0; s && i < n; i++) {
        h ^= (uint8_t)s[i];
        h *= 0x100000001b3ULL;
    }
    return h;
}

void cbm_work_note_strlen(void *site, const char *s, size_t len) {
    if (!cbm_memev_enabled()) {
        return;
    }
    tl_state_t *st = tl_get();
    if (!st || st->busy) {
        return;
    }
    st->busy = true;
    strlen_pair_t *slot = &st->strlen_cache[mix((uint64_t)(uintptr_t)s) & (MEMEV_REPEAT_SLOTS - 1)];
    uint64_t content = bytes_hash(s, len);
    uint64_t repeat = (slot->ptr == s && slot->len == len && slot->content_hash == content) ? 1 : 0;
    slot->ptr = s;
    slot->len = len;
    slot->content_hash = content;
    work_add(st, CBM_WORK_STRLEN, (uintptr_t)site, len, 0, 0, repeat);
    st->busy = false;
}

/* FNV-1a over a NUL-terminated path, without calling the strlen we wrap. */
static uint64_t path_hash(const char *p) {
    uint64_t h = 0xcbf29ce484222325ULL;
    for (; p && *p; p++) {
        h ^= (uint8_t)*p;
        h *= 0x100000001b3ULL;
    }
    return h;
}

void cbm_work_note_path(cbm_work_kind_t kind, void *site, const char *path, bool failed) {
    if (!cbm_memev_enabled()) {
        return;
    }
    tl_state_t *st = tl_get();
    if (!st || st->busy) {
        return;
    }
    st->busy = true;
    uint64_t h = mix(path_hash(path) ^ ((uint64_t)kind << 58)) | 1U;
    uint64_t repeat = 0;
    uint32_t idx = (uint32_t)(h & (MEMEV_PATH_CAP - 1));
    for (uint32_t probe = 0; probe < MEMEV_PATH_CAP; probe++) {
        uint64_t seen = atomic_load_explicit(&g_path_hashes[idx], memory_order_relaxed);
        if (seen == h) {
            repeat = 1;
            break;
        }
        if (seen == 0) {
            uint64_t expect = 0;
            if (atomic_compare_exchange_strong_explicit(
                    &g_path_hashes[idx], &expect, h, memory_order_relaxed, memory_order_relaxed)) {
                break;
            }
            if (expect == h) {
                repeat = 1;
                break;
            }
        }
        idx = (idx + 1) & (MEMEV_PATH_CAP - 1);
    }
    work_add(st, kind, (uintptr_t)site, 0, failed ? 1 : 0, 0, repeat);
    st->busy = false;
}

void cbm_work_note_ht(cbm_work_kind_t kind, void *site, const void *table, const char *key,
                      uint64_t generation, bool hit) {
    if (!cbm_memev_enabled()) {
        return;
    }
    tl_state_t *st = tl_get();
    if (!st || st->busy) {
        return;
    }
    st->busy = true;
    uint64_t repeat = 0;
    if (kind == CBM_WORK_HT_GET) {
        uint64_t key_hash = path_hash(key);
        ht_pair_t *slot =
            &st->ht_cache[mix((uint64_t)(uintptr_t)table ^ (key_hash * 0x9e3779b97f4a7c15ULL)) &
                          (MEMEV_REPEAT_SLOTS - 1)];
        repeat =
            (slot->table == table && slot->key_hash == key_hash && slot->generation == generation)
                ? 1
                : 0;
        slot->table = table;
        slot->key_hash = key_hash;
        slot->generation = generation;
    }
    work_add(st, kind, (uintptr_t)site, 0, (kind == CBM_WORK_HT_GET && !hit) ? 1 : 0, 0, repeat);
    st->busy = false;
}

/* ── merge ────────────────────────────────────────────────────────────*/
static void flush_state(tl_state_t *st) {
    lock_take(&g_merge_lock);
    for (uint32_t i = 0; i < st->dirty_count; i++) {
        uint32_t sid = st->dirty[i];
        site_counters_t *d = &st->delta[sid];
        site_counters_t *g = &g_site_counters[sid];
        g->allocs += d->allocs;
        g->frees += d->frees;
        g->reallocs += d->reallocs;
        g->requested_bytes += d->requested_bytes;
        g->usable_bytes += d->usable_bytes;
        g->realloc_copy_bytes += d->realloc_copy_bytes;
        g->short_lived += d->short_lived;
        g->raw_bytes += d->raw_bytes;
        g->scanned_blocks += d->scanned_blocks;
        g->never_written_bytes += d->never_written_bytes;
        g->over_requested_bytes += d->over_requested_bytes;
        g->zero_untouched_bytes += d->zero_untouched_bytes;
        g->acc_untouched_blocks += d->acc_untouched_blocks;
        g->acc_untouched_bytes += d->acc_untouched_bytes;
        g->acc_dead_blocks += d->acc_dead_blocks;
        g->acc_dead_bytes += d->acc_dead_bytes;
        g->acc_uninit_reads += d->acc_uninit_reads;
        g->acc_idle_bytes += d->acc_idle_bytes;
        g->acc_opaque_blocks += d->acc_opaque_blocks;
        g->live_bytes += d->live_bytes;
        g->live_blocks += d->live_blocks;
        memset(d, 0, sizeof(*d));
        st->is_dirty[sid] = 0;
    }
    st->dirty_count = 0;
    for (uint32_t i = 0; i < st->work_dirty_count; i++) {
        uint32_t idx = st->work_dirty[i];
        work_counters_t *d = &st->work_delta[idx];
        work_counters_t *g = &g_work_counters[idx];
        g->calls += d->calls;
        g->bytes += d->bytes;
        g->aux1 += d->aux1;
        g->aux2 += d->aux2;
        g->repeats += d->repeats;
        if (d->peak > g->peak) {
            g->peak = d->peak;
        }
        memset(d, 0, sizeof(*d));
        st->work_is_dirty[idx] = 0;
    }
    st->work_dirty_count = 0;
    lock_drop(&g_merge_lock);
    atomic_fetch_add_explicit(&g_untracked_frees, st->untracked_frees, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_pointer_table_full, st->pointer_table_full, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_site_table_full, st->site_table_full, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_work_table_full, st->work_table_full, memory_order_relaxed);
    st->untracked_frees = 0;
    st->pointer_table_full = 0;
    st->site_table_full = 0;
    st->work_table_full = 0;
}

void cbm_memev_flush_thread(void) {
    if (!cbm_memev_enabled()) {
        return;
    }
    tl_state_t *st = tl_peek();
    if (!st || st->busy) {
        return;
    }
    st->busy = true;
    flush_state(st);
    st->busy = false;
}

/* Thread exit (pthread key / FLS destructor): hand over what is pending, then
 * give the raw memory back. */
static void tl_release(void *raw);

void cbm_memev_thread_end(void) {
    if (atomic_load_explicit(&g_key_state, memory_order_acquire) != 2) {
        return; /* no thread ever had state: nothing to release */
    }
    void *v = tl_slot_value();
    if (v && v != TL_EXITED) {
        tl_release(v);
    }
}

static void tl_release(void *raw) {
    tl_state_t *st = (tl_state_t *)raw;
    if (!st || raw == TL_EXITED) {
        return;
    }
    st->busy = true;
    flush_state(st);
    /* Mark the slot before the memory goes: whatever the rest of the thread's
     * teardown allocates or frees must find the marker, neither a freed pointer
     * nor an empty slot that would build a new state. */
#ifdef _WIN32
    (void)TlsSetValue(g_key, TL_EXITED);
#else
    (void)pthread_setspecific(g_key, TL_EXITED);
#endif
    atomic_fetch_sub_explicit(&g_layer_threads, 1, memory_order_relaxed);
    vm_put(st, tl_bytes());
}

/* ── phase snapshot ───────────────────────────────────────────────────
 * Live blocks allocated in an earlier phase are RETAINED: the question is
 * whether the phase that is running still needs them. Small live blocks with
 * identical content are DUPLICATES: one interned copy would do. Both are
 * snapshots of the live set taken after the workers joined. */
static uint64_t content_hash(const uint8_t *p, size_t n) {
    uint64_t h = 0xcbf29ce484222325ULL ^ ((uint64_t)n * 0x9e3779b97f4a7c15ULL);
    for (size_t i = 0; i < n; i++) {
        h ^= p[i];
        h *= 0x100000001b3ULL;
    }
    return h | 1U;
}

static void snapshot_phase(uint32_t phase_now) {
    memset(g_snap_retained, 0, sizeof(g_snap_retained));
    memset(g_snap_dup_bytes, 0, sizeof(g_snap_dup_bytes));
    memset(g_snap_dup_blocks, 0, sizeof(g_snap_dup_blocks));
    bool dupes = dupes_enabled();
    uint64_t live_total = 0;
    for (uint32_t i = 0; i < MEMEV_SHARDS; i++) {
        live_total += g_shards[i].live;
    }
    size_t set_cap = 1024;
    while (set_cap < live_total * 2 && set_cap < ((size_t)1 << 30)) {
        set_cap <<= 1;
    }
    uint64_t *set = dupes ? (uint64_t *)vm_get(set_cap * sizeof(uint64_t)) : NULL;
    for (uint32_t i = 0; i < MEMEV_SHARDS; i++) {
        shard_t *sh = &g_shards[i];
        lock_take(&sh->lock);
        uint32_t cap = sh->entries ? (1U << sh->bits) : 0;
        for (uint32_t j = 0; j < cap; j++) {
            const ptr_entry_t *e = &sh->entries[j];
            if (e->block == 0 || e->block == MEMEV_TOMBSTONE) {
                continue;
            }
            if ((uint32_t)(e->meta & META_PHASE_MASK) < phase_now) {
                g_snap_retained[e->site] += e->usable;
            }
            if (!set || e->usable > CBM_MEMEV_DUP_MAX || !(e->meta & (META_FILLED | META_ZEROED))) {
                continue;
            }
            const uint8_t *p = (const uint8_t *)e->block;
            scan_t sc =
                scan_block(p, (size_t)e->usable, (e->meta & META_ZEROED) ? 0 : MEMEV_FILL_BYTE);
            if (sc.high_water == 0) {
                continue; /* nothing written: that is never-written waste, not a duplicate */
            }
            uint64_t h = content_hash(p, (size_t)sc.high_water);
            size_t s = (size_t)(h & (set_cap - 1));
            for (;;) {
                if (set[s] == 0) {
                    set[s] = h;
                    break;
                }
                if (set[s] == h) {
                    g_snap_dup_bytes[e->site] += e->usable;
                    g_snap_dup_blocks[e->site]++;
                    break;
                }
                s = (s + 1) & (set_cap - 1);
            }
        }
        lock_drop(&sh->lock);
    }
    vm_put(set, set_cap * sizeof(uint64_t));
}

void cbm_memev_phase(const char *label) {
    if (!cbm_memev_enabled()) {
        return;
    }
    tl_state_t *st = tl_get();
    if (!st || st->busy) {
        return;
    }
    st->busy = true;
    flush_state(st);
    uint32_t next = atomic_load_explicit(&g_phase, memory_order_relaxed) + 1;
    if (next <= MEMEV_PHASE_CAP) {
        atomic_store_explicit(&g_phase, next, memory_order_relaxed);
    }
    lock_take(&g_merge_lock);
    snprintf(g_phase_label, sizeof(g_phase_label), "%s", label ? label : "-");
    lock_drop(&g_merge_lock);
    snapshot_phase(atomic_load_explicit(&g_phase, memory_order_relaxed));
    st->busy = false;
    const char *out = getenv("CBM_MEMWASTE_OUT");
    if (out && out[0]) {
        char why[MEMEV_PHASE_LABEL + 8];
        snprintf(why, sizeof(why), "phase:%s", label ? label : "-");
        (void)cbm_memev_dump(out, why);
    }
}

/* ── report ───────────────────────────────────────────────────────────*/
void cbm_memev_totals(cbm_memev_totals_t *out) {
    if (!out) {
        return;
    }
    memset(out, 0, sizeof(*out));
    cbm_memev_flush_thread();
    lock_take(&g_merge_lock);
    for (uint32_t i = 0; i < MEMEV_SITE_CAP; i++) {
        if (atomic_load_explicit(&g_site_keys[i], memory_order_relaxed) == 0) {
            continue;
        }
        const site_counters_t *g = &g_site_counters[i];
        out->sites++;
        out->allocs += g->allocs;
        out->frees += g->frees;
        out->reallocs += g->reallocs;
        out->requested_bytes += g->requested_bytes;
        out->usable_bytes += g->usable_bytes;
        out->realloc_copy_bytes += g->realloc_copy_bytes;
        out->raw_bytes += g->raw_bytes;
        if (g->live_bytes > 0) {
            out->live_bytes += (uint64_t)g->live_bytes;
        }
        if (g->live_blocks > 0) {
            out->live_blocks += (uint64_t)g->live_blocks;
        }
    }
    lock_drop(&g_merge_lock);
    out->untracked_frees = atomic_load_explicit(&g_untracked_frees, memory_order_relaxed);
    out->foreign_frees = atomic_load_explicit(&g_foreign_frees, memory_order_relaxed);
    out->pointer_table_full = atomic_load_explicit(&g_pointer_table_full, memory_order_relaxed);
    out->site_table_full = atomic_load_explicit(&g_site_table_full, memory_order_relaxed);
    out->work_table_full = atomic_load_explicit(&g_work_table_full, memory_order_relaxed);
}

static void site_row_of(uint32_t i, uintptr_t key, cbm_memev_site_t *row) {
    const site_counters_t *g = &g_site_counters[i];
    *row = (cbm_memev_site_t){
        .site = key,
        .allocs = g->allocs,
        .frees = g->frees,
        .reallocs = g->reallocs,
        .requested_bytes = g->requested_bytes,
        .usable_bytes = g->usable_bytes,
        .realloc_copy_bytes = g->realloc_copy_bytes,
        .live_bytes = g->live_bytes > 0 ? (uint64_t)g->live_bytes : 0,
        .live_blocks = g->live_blocks > 0 ? (uint64_t)g->live_blocks : 0,
        .short_lived = g->short_lived,
        .raw_bytes = g->raw_bytes,
        .scanned_blocks = g->scanned_blocks,
        .never_written_bytes = g->never_written_bytes,
        .over_requested_bytes = g->over_requested_bytes,
        .zero_untouched_bytes = g->zero_untouched_bytes,
        .retained_bytes = g_snap_retained[i],
        .dup_bytes = g_snap_dup_bytes[i],
        .dup_blocks = g_snap_dup_blocks[i],
        .acc_untouched_blocks = g->acc_untouched_blocks,
        .acc_untouched_bytes = g->acc_untouched_bytes,
        .acc_dead_blocks = g->acc_dead_blocks,
        .acc_dead_bytes = g->acc_dead_bytes,
        .acc_uninit_reads = g->acc_uninit_reads,
        .acc_idle_bytes = g->acc_idle_bytes,
        .acc_opaque_blocks = g->acc_opaque_blocks,
    };
}

static int by_usable_desc(const void *a, const void *b) {
    const cbm_memev_site_t *x = (const cbm_memev_site_t *)a;
    const cbm_memev_site_t *y = (const cbm_memev_site_t *)b;
    if (x->usable_bytes != y->usable_bytes) {
        return x->usable_bytes < y->usable_bytes ? 1 : -1;
    }
    /* Equal bytes: order by address, so the report is a function of the data. */
    return x->site < y->site ? -1 : (x->site > y->site ? 1 : 0);
}

/* Top `max` sites by cumulative usable bytes, selected without allocating. */
size_t cbm_memev_sites(cbm_memev_site_t *out, size_t max) {
    if (!out || max == 0) {
        return 0;
    }
    cbm_memev_flush_thread();
    size_t n = 0;
    lock_take(&g_merge_lock);
    for (uint32_t i = 0; i < MEMEV_SITE_CAP; i++) {
        uintptr_t key = atomic_load_explicit(&g_site_keys[i], memory_order_relaxed);
        if (key == 0) {
            continue;
        }
        cbm_memev_site_t row;
        site_row_of(i, key, &row);
        if (n < max) {
            out[n++] = row;
            continue;
        }
        size_t smallest = 0;
        for (size_t k = 1; k < n; k++) {
            if (by_usable_desc(&out[k], &out[smallest]) > 0) {
                smallest = k;
            }
        }
        if (by_usable_desc(&row, &out[smallest]) < 0) {
            out[smallest] = row;
        }
    }
    lock_drop(&g_merge_lock);
    tl_state_t *st = tl_get();
    bool was_busy = st && st->busy;
    if (st) {
        st->busy = true; /* qsort is libc; keep anything it does out of the tables */
    }
    qsort(out, n, sizeof(out[0]), by_usable_desc);
    if (st) {
        st->busy = was_busy;
    }
    return n;
}

size_t cbm_work_rows(cbm_work_row_t *out, size_t max) {
    if (!out || max == 0) {
        return 0;
    }
    cbm_memev_flush_thread();
    size_t n = 0;
    lock_take(&g_merge_lock);
    for (uint32_t i = 0; i < MEMEV_WORK_CAP && n < max; i++) {
        uintptr_t key = atomic_load_explicit(&g_work_keys[i], memory_order_relaxed);
        if (key == 0) {
            continue;
        }
        const work_counters_t *g = &g_work_counters[i];
        out[n++] = (cbm_work_row_t){
            .site = key & MEMEV_SITE_MASK,
            .kind = (cbm_work_kind_t)((key >> 56) - 1),
            .calls = g->calls,
            .bytes = g->bytes,
            .aux1 = g->aux1,
            .aux2 = g->aux2,
            .repeats = g->repeats,
            .peak = g->peak,
        };
    }
    lock_drop(&g_merge_lock);
    return n;
}

static bool write_all(int fd, const char *buf, size_t len) {
    while (len > 0) {
#ifdef _WIN32
        int put = _write(fd, buf, (unsigned)len);
#else
        ssize_t put = write(fd, buf, len);
#endif
        if (put <= 0) {
            return false;
        }
        buf += put;
        len -= (size_t)put;
    }
    return true;
}

#if defined(__linux__)
static int first_object_bias(struct dl_phdr_info *info, size_t size, void *out) {
    (void)size;
    *(uintptr_t *)out = (uintptr_t)info->dlpi_addr;
    return 1; /* the first object is the main program */
}
#endif

/* Sites are raw return addresses; the report subtracts the image base to get
 * the address the symboliser expects. macOS records it when the zone observer
 * installs; Linux (the load bias: 0 for a non-PIE binary) and Windows (the
 * runtime module base) record it here, once, before the first dump. */
static void ensure_image_base(void) {
    if (atomic_load_explicit(&g_image_base, memory_order_relaxed) != 0) {
        return;
    }
#if defined(__linux__)
    uintptr_t bias = 0;
    dl_iterate_phdr(first_object_bias, &bias);
    atomic_store_explicit(&g_image_base, bias, memory_order_relaxed);
#elif defined(_WIN32)
    atomic_store_explicit(&g_image_base, (uintptr_t)GetModuleHandleW(NULL), memory_order_relaxed);
#endif
}

static bool dump_locked(const char *path, const char *why) {
    ensure_image_base();
    cbm_memev_totals_t t;
    cbm_memev_totals(&t);
#ifdef _WIN32
    int fd = _open(path, _O_WRONLY | _O_CREAT | _O_APPEND | _O_BINARY, 0600);
    int pid = (int)GetCurrentProcessId();
#else
    int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0600);
    int pid = (int)getpid();
#endif
    if (fd < 0) {
        return false;
    }
    char label[MEMEV_PHASE_LABEL];
    lock_take(&g_merge_lock);
    snprintf(label, sizeof(label), "%s", g_phase_label);
    lock_drop(&g_merge_lock);
    char line[MEMEV_LINE];
    int len = snprintf(
        line, sizeof(line),
        "{\"memwaste\":2,\"why\":\"%s\",\"pid\":%d,\"image_base\":\"0x%llx\",\"phase\":%u,"
        "\"phase_label\":\"%s\",\"rss_bytes\":%llu,\"allocs\":%llu,\"frees\":%llu,"
        "\"untracked_frees\":%llu,\"foreign_frees\":%llu,\"reallocs\":%llu,\"requested_bytes\":%"
        "llu,\"usable_bytes\":%llu,"
        "\"realloc_copy_bytes\":%llu,\"live_bytes\":%llu,\"live_blocks\":%llu,\"raw_bytes\":%llu,"
        "\"sites\":%llu,\"site_table_full\":%llu,\"pointer_table_full\":%llu,"
        "\"work_table_full\":%llu,\"layer_vm_bytes\":%llu,\"layer_threads\":%lld,"
        "\"fill\":%d,\"access\":%d}\n",
        why ? why : "-", pid,
        (unsigned long long)atomic_load_explicit(&g_image_base, memory_order_relaxed),
        atomic_load_explicit(&g_phase, memory_order_relaxed), label,
        (unsigned long long)cbm_mem_rss(), (unsigned long long)t.allocs,
        (unsigned long long)t.frees, (unsigned long long)t.untracked_frees,
        (unsigned long long)t.foreign_frees, (unsigned long long)t.reallocs,
        (unsigned long long)t.requested_bytes, (unsigned long long)t.usable_bytes,
        (unsigned long long)t.realloc_copy_bytes, (unsigned long long)t.live_bytes,
        (unsigned long long)t.live_blocks, (unsigned long long)t.raw_bytes,
        (unsigned long long)t.sites, (unsigned long long)t.site_table_full,
        (unsigned long long)t.pointer_table_full, (unsigned long long)t.work_table_full,
        (unsigned long long)atomic_load_explicit(&g_layer_vm_bytes, memory_order_relaxed),
        (long long)atomic_load_explicit(&g_layer_threads, memory_order_relaxed),
        fill_enabled() ? 1 : 0,
#if defined(CBM_MEMWASTE_ACCESS) && CBM_MEMWASTE_ACCESS
        1
#else
        0
#endif
    );
    bool ok = len > 0 && (size_t)len < sizeof(line) && write_all(fd, line, (size_t)len);

    lock_take(&g_merge_lock);
    for (uint32_t i = 0; ok && i < MEMEV_SITE_CAP; i++) {
        uintptr_t key = atomic_load_explicit(&g_site_keys[i], memory_order_relaxed);
        if (key == 0) {
            continue;
        }
        cbm_memev_site_t r;
        site_row_of(i, key, &r);
        len = snprintf(
            line, sizeof(line),
            "{\"site\":\"0x%llx\",\"allocs\":%llu,\"frees\":%llu,\"reallocs\":%llu,"
            "\"requested_bytes\":%llu,\"usable_bytes\":%llu,\"realloc_copy_bytes\":%llu,"
            "\"live_bytes\":%llu,\"live_blocks\":%llu,\"short_lived\":%llu,\"raw_bytes\":%llu,"
            "\"scanned_blocks\":%llu,\"never_written_bytes\":%llu,\"over_requested_bytes\":%llu,"
            "\"zero_untouched_bytes\":%llu,\"retained_bytes\":%llu,\"dup_bytes\":%llu,"
            "\"dup_blocks\":%llu,\"acc_untouched_blocks\":%llu,\"acc_untouched_bytes\":%llu,"
            "\"acc_dead_blocks\":%llu,\"acc_dead_bytes\":%llu,\"acc_uninit_reads\":%llu,"
            "\"acc_idle_bytes\":%llu,\"acc_opaque_blocks\":%llu}\n",
            (unsigned long long)r.site, (unsigned long long)r.allocs, (unsigned long long)r.frees,
            (unsigned long long)r.reallocs, (unsigned long long)r.requested_bytes,
            (unsigned long long)r.usable_bytes, (unsigned long long)r.realloc_copy_bytes,
            (unsigned long long)r.live_bytes, (unsigned long long)r.live_blocks,
            (unsigned long long)r.short_lived, (unsigned long long)r.raw_bytes,
            (unsigned long long)r.scanned_blocks, (unsigned long long)r.never_written_bytes,
            (unsigned long long)r.over_requested_bytes, (unsigned long long)r.zero_untouched_bytes,
            (unsigned long long)r.retained_bytes, (unsigned long long)r.dup_bytes,
            (unsigned long long)r.dup_blocks, (unsigned long long)r.acc_untouched_blocks,
            (unsigned long long)r.acc_untouched_bytes, (unsigned long long)r.acc_dead_blocks,
            (unsigned long long)r.acc_dead_bytes, (unsigned long long)r.acc_uninit_reads,
            (unsigned long long)r.acc_idle_bytes, (unsigned long long)r.acc_opaque_blocks);
        ok = len > 0 && (size_t)len < sizeof(line) && write_all(fd, line, (size_t)len);
    }
    for (uint32_t i = 0; ok && i < MEMEV_WORK_CAP; i++) {
        uintptr_t key = atomic_load_explicit(&g_work_keys[i], memory_order_relaxed);
        if (key == 0) {
            continue;
        }
        const work_counters_t *g = &g_work_counters[i];
        if (g->calls == 0) {
            continue;
        }
        len = snprintf(line, sizeof(line),
                       "{\"work\":\"%s\",\"site\":\"0x%llx\",\"calls\":%llu,\"bytes\":%llu,"
                       "\"aux1\":%llu,\"aux2\":%llu,\"repeats\":%llu,\"peak\":%llu}\n",
                       cbm_work_kind_name((cbm_work_kind_t)((key >> 56) - 1)),
                       (unsigned long long)(key & MEMEV_SITE_MASK), (unsigned long long)g->calls,
                       (unsigned long long)g->bytes, (unsigned long long)g->aux1,
                       (unsigned long long)g->aux2, (unsigned long long)g->repeats,
                       (unsigned long long)g->peak);
        ok = len > 0 && (size_t)len < sizeof(line) && write_all(fd, line, (size_t)len);
    }
    lock_drop(&g_merge_lock);
#ifdef _WIN32
    _close(fd);
#else
    close(fd);
#endif
    return ok;
}

bool cbm_memev_dump(const char *path, const char *why) {
    if (!path || !path[0]) {
        return false;
    }
    static memev_lock_t dump_lock = ATOMIC_FLAG_INIT;
    tl_state_t *st = tl_get();
    bool was_busy = st && st->busy;
    if (st && !was_busy) {
        flush_state(st); /* this thread's pending events belong in the dump */
    }
    if (st) {
        st->busy = true; /* the dump's own writes and allocations are not the program's work */
    }
    lock_take(&dump_lock);
    bool ok = dump_locked(path, why);
    lock_drop(&dump_lock);
    if (st) {
        st->busy = was_busy;
    }
    return ok;
}

void cbm_memev_reset_for_tests(void) {
    cbm_memev_flush_thread();
    lock_take(&g_merge_lock);
    for (uint32_t i = 0; i < MEMEV_SITE_CAP; i++) {
        atomic_store_explicit(&g_site_keys[i], 0, memory_order_relaxed);
    }
    for (uint32_t i = 0; i < MEMEV_WORK_CAP; i++) {
        atomic_store_explicit(&g_work_keys[i], 0, memory_order_relaxed);
    }
    for (uint32_t i = 0; i < MEMEV_PATH_CAP; i++) {
        atomic_store_explicit(&g_path_hashes[i], 0, memory_order_relaxed);
    }
    memset(g_site_counters, 0, sizeof(g_site_counters));
    memset(g_work_counters, 0, sizeof(g_work_counters));
    memset(g_snap_retained, 0, sizeof(g_snap_retained));
    memset(g_snap_dup_bytes, 0, sizeof(g_snap_dup_bytes));
    memset(g_snap_dup_blocks, 0, sizeof(g_snap_dup_blocks));
    lock_drop(&g_merge_lock);
    for (uint32_t i = 0; i < MEMEV_SHARDS; i++) {
        shard_t *sh = &g_shards[i];
        lock_take(&sh->lock);
        vm_put(sh->entries, sh->entries ? ((size_t)1 << sh->bits) * sizeof(ptr_entry_t) : 0);
        sh->entries = NULL;
        sh->bits = 0;
        sh->used = 0;
        sh->live = 0;
        lock_drop(&sh->lock);
    }
    atomic_store_explicit(&g_untracked_frees, 0, memory_order_relaxed);
    atomic_store_explicit(&g_foreign_frees, 0, memory_order_relaxed);
    atomic_store_explicit(&g_pointer_table_full, 0, memory_order_relaxed);
    atomic_store_explicit(&g_site_table_full, 0, memory_order_relaxed);
    atomic_store_explicit(&g_work_table_full, 0, memory_order_relaxed);
    atomic_store_explicit(&g_phase, 0, memory_order_relaxed);
    atomic_store_explicit(&g_fill, 0, memory_order_relaxed); /* tests use synthetic addresses */
    tl_state_t *st = tl_peek();
    if (st) {
        st->hint_set = false;
        memset(st->strlen_cache, 0, sizeof(st->strlen_cache));
        memset(st->ht_cache, 0, sizeof(st->ht_cache));
    }
}

#if defined(__clang__)
#pragma clang attribute pop
#endif

#else /* layer compiled out: every entry point is an empty stub */

bool cbm_memev_enabled(void) {
    return false;
}
void cbm_memev_alloc(void *block, size_t requested, size_t usable, void *site) {
    (void)block;
    (void)requested;
    (void)usable;
    (void)site;
}
void cbm_memev_alloc_ex(void *block, size_t requested, size_t usable, void *site, unsigned flags) {
    (void)block;
    (void)requested;
    (void)usable;
    (void)site;
    (void)flags;
}
void cbm_memev_free(void *block) {
    (void)block;
}
void cbm_memev_free_foreign(void *block) {
    (void)block;
}
void cbm_memev_realloc(void *old_block, void *grown, size_t requested, size_t usable, void *site) {
    (void)old_block;
    (void)grown;
    (void)requested;
    (void)usable;
    (void)site;
}
void cbm_memev_hint(void *site, int mem_class) {
    (void)site;
    (void)mem_class;
}
bool cbm_memev_hint_pending(void) {
    return false;
}
void cbm_memev_set_image_base(uintptr_t base) {
    (void)base;
}
void cbm_memev_set_observer_installed(bool installed) {
    (void)installed;
}
bool cbm_memev_observer_installed(void) {
    return false;
}
void cbm_memev_flush_thread(void) {}
void cbm_memev_phase(const char *label) {
    (void)label;
}
const char *cbm_work_kind_name(cbm_work_kind_t kind) {
    (void)kind;
    return "off";
}
uint64_t cbm_memev_thread_ops(void) {
    return 0;
}
void cbm_memev_process_role(const char *role) {
    (void)role;
}
void cbm_memev_thread_end(void) {}
void cbm_memev_process_exit(void) {}
void cbm_memev_library_enter(void *site) {
    (void)site;
}
void cbm_memev_library_leave(void) {}
void *cbm_memev_library_site(void) {
    return NULL;
}
void cbm_work_note(cbm_work_kind_t kind, void *site, uint64_t bytes, uint64_t aux1, uint64_t aux2) {
    (void)kind;
    (void)site;
    (void)bytes;
    (void)aux1;
    (void)aux2;
}
void cbm_memev_da_note(size_t cap_bytes, size_t used_bytes, size_t cap_slots) {
    (void)cap_bytes;
    (void)used_bytes;
    (void)cap_slots;
}
void cbm_work_note_strlen(void *site, const char *s, size_t len) {
    (void)site;
    (void)s;
    (void)len;
}
void cbm_work_note_path(cbm_work_kind_t kind, void *site, const char *path, bool failed) {
    (void)kind;
    (void)site;
    (void)path;
    (void)failed;
}
void cbm_work_note_ht(cbm_work_kind_t kind, void *site, const void *table, const char *key,
                      uint64_t generation, bool hit) {
    (void)kind;
    (void)site;
    (void)table;
    (void)key;
    (void)generation;
    (void)hit;
}
void cbm_memev_totals(cbm_memev_totals_t *out) {
    if (out) {
        *out = (cbm_memev_totals_t){0};
    }
}
size_t cbm_memev_sites(cbm_memev_site_t *out, size_t max) {
    (void)out;
    (void)max;
    return 0;
}
size_t cbm_work_rows(cbm_work_row_t *out, size_t max) {
    (void)out;
    (void)max;
    return 0;
}
bool cbm_memev_dump(const char *path, const char *why) {
    (void)path;
    (void)why;
    return false;
}
void cbm_memev_reset_for_tests(void) {}
void cbm_memev_scan_for_tests(bool fill, bool dupes) {
    (void)fill;
    (void)dupes;
}
void cbm_memev_force_for_tests(bool on) {
    (void)on;
}

#endif
