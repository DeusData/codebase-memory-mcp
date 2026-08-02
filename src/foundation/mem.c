/*
 * mem.c — Unified memory management via mimalloc.
 *
 * Budget tracking based on actual RSS via mi_process_info().
 * When MI_OVERRIDE=0 (ASan builds), falls back to OS-specific
 * RSS queries (task_info on macOS, /proc/self/statm on Linux,
 * GetProcessMemoryInfo on Windows).
 */
#include "mem.h"
#include "platform.h"
#include "log.h"
#include "compat_fs.h"
#include "compat_thread.h"

#include "foundation/constants.h"

#define MAX_RAM_FRACTION 1.0
#define DEFAULT_RAM_FRACTION 0.5
#include <errno.h>
#include <mimalloc.h>
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
#include <psapi.h>
#elif defined(__APPLE__)
#include <mach/mach.h>
#else
#include <unistd.h>
#endif

#ifdef _WIN32
/* mimalloc internals. Not in the public header, but the allocator is compiled
 * into this binary as a unity object, so the symbols are present.
 *
 * WHY WE REACH FOR THEM: arena creation (arena.c:515) and arena purging
 * (arena.c:2060) are BOTH gated on `_mi_preloading()`. If that flag never
 * clears, no arena is ever created and purge never runs — memory comes
 * straight from the OS and is never given back, which is exactly the Windows
 * behaviour in #581 and why no amount of purge configuration helped. The flag
 * is cleared only by `_mi_auto_process_init()`, which upstream invokes from a
 * `__attribute__((constructor))` for clang/gcc and from `.CRT$XIU` TLS
 * callbacks for MSVC. Whether that constructor actually runs in our static
 * MinGW link is the open question, so ask the allocator rather than assume. */
extern bool _mi_preloading(void);
extern void _mi_auto_process_init(void);
#endif

/* ── Static state ─────────────────────────────────────────────── */

static size_t g_budget;          /* budget in bytes */
static atomic_int g_initialized; /* init guard */
static atomic_int g_was_over;    /* pressure hysteresis */

#define MB_DIVISOR ((size_t)(CBM_SZ_1K * CBM_SZ_1K))

/* ── OS fallback for RSS (ASan builds where MI_OVERRIDE=0) ──── */

static size_t os_rss(void) {
#ifdef _WIN32
    PROCESS_MEMORY_COUNTERS pmc;
    if (GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc))) {
        return (size_t)pmc.WorkingSetSize;
    }
    return 0;
#elif defined(__APPLE__)
    struct mach_task_basic_info info = {0};
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &count) ==
        KERN_SUCCESS) {
        return (size_t)info.resident_size;
    }
    return 0;
#else
    FILE *f = fopen("/proc/self/statm", "r");
    if (!f) {
        return 0;
    }
    unsigned long pages = 0;
    unsigned long rss_pages = 0;
    if (fscanf(f, "%lu %lu", &pages, &rss_pages) != 2) {
        rss_pages = 0;
    }
    (void)fclose(f);
    long ps = sysconf(_SC_PAGESIZE);
    return rss_pages * (ps > 0 ? (size_t)ps : CBM_SZ_4K);
#endif
}

/* ── Pressure logging (hysteresis) ────────────────────────────── */

static void check_pressure(size_t rss) {
    if (g_budget == 0) {
        return;
    }

    bool over = rss > g_budget;
    int was = atomic_load(&g_was_over);

    if (over && !was) {
        atomic_store(&g_was_over, 1);
        char rss_mb[CBM_SZ_32];
        char budget_mb[CBM_SZ_32];
        char pct_str[CBM_SZ_16];
        snprintf(rss_mb, sizeof(rss_mb), "%zu", rss / MB_DIVISOR);
        snprintf(budget_mb, sizeof(budget_mb), "%zu", g_budget / MB_DIVISOR);
        snprintf(pct_str, sizeof(pct_str), "%zu",
                 g_budget > 0 ? (rss * CBM_PERCENT) / g_budget : 0);
        cbm_log_warn("mem.pressure.warn", "rss_mb", rss_mb, "budget_mb", budget_mb, "pct", pct_str);
    } else if (!over && was) {
        atomic_store(&g_was_over, 0);
        char rss_mb[CBM_SZ_32];
        char budget_mb[CBM_SZ_32];
        char pct_str[CBM_SZ_16];
        snprintf(rss_mb, sizeof(rss_mb), "%zu", rss / MB_DIVISOR);
        snprintf(budget_mb, sizeof(budget_mb), "%zu", g_budget / MB_DIVISOR);
        snprintf(pct_str, sizeof(pct_str), "%zu",
                 g_budget > 0 ? (rss * CBM_PERCENT) / g_budget : 0);
        cbm_log_info("mem.pressure.ok", "rss_mb", rss_mb, "budget_mb", budget_mb, "pct", pct_str);
    }
}

/* Allocator setup is not best-effort. A misconfigured allocator does not
 * degrade gracefully — it grows without bound until the OS kills the process
 * (#581 ran for months exactly this way, silently). So every step of the setup
 * is VERIFIED and a failure is fatal here rather than a slow death later.
 * There is deliberately no timeout or retry: these checks are synchronous, so
 * "not true yet" and "never going to be true" are the same answer. */
static void mem_setup_fatal(const char *what, const char *detail) {
    cbm_log_error("mem.allocator.setup_failed", "check", what, "detail", detail);
    /* Never continue with an allocator that cannot return memory: unbounded
     * growth is a worse failure than refusing to start. */
    abort();
}

/* Set an option and prove it took effect; a silently-ignored option is the
 * failure mode that made #581 invisible. */
static void mem_option_set_verified(mi_option_t option, long value, const char *name) {
    mi_option_set(option, value);
    if (mi_option_get(option) != value) {
        char detail[CBM_SZ_128];
        snprintf(detail, sizeof(detail), "%s did not take effect (wanted %ld, got %ld)", name,
                 value, mi_option_get(option));
        mem_setup_fatal("option", detail);
    }
}

/* ── Public API ────────────────────────────────────────────────── */

#define RAM_FRACTION_DEFAULT 0.5
#define RAM_FRACTION_16GB 0.25
#define RAM_FRACTION_32GB 0.35
#define RAM_BYTES_PER_GB (1024ULL * 1024 * 1024)

double cbm_mem_ram_fraction_for_total(size_t total_ram_bytes) {
    if (total_ram_bytes <= 16ULL * RAM_BYTES_PER_GB) {
        return RAM_FRACTION_16GB;
    }
    if (total_ram_bytes <= 32ULL * RAM_BYTES_PER_GB) {
        return RAM_FRACTION_32GB;
    }
    return RAM_FRACTION_DEFAULT;
}

cbm_mem_budget_t cbm_mem_resolve_budget(size_t total_ram, double ram_fraction,
                                        const char *budget_mb) {
    if (ram_fraction <= 0.0 || ram_fraction > MAX_RAM_FRACTION) {
        ram_fraction = DEFAULT_RAM_FRACTION;
    }
    cbm_mem_budget_t result = {
        .budget = (size_t)((double)total_ram * ram_fraction),
        .source = "ram_fraction",
        .clamped = false,
        .invalid = false,
        .hard_capped = false,
    };

    if (budget_mb == NULL || budget_mb[0] == '\0') {
        return result; /* no override → fraction-derived budget */
    }

    /* Strict parse, matching the src/foundation/limits.c convention: reject
     * trailing garbage (`*end`), overflow (errno==ERANGE), and non-positive
     * values. This turns a fat-fingered value (e.g. "8GB", or a 20-digit typo)
     * into a clean fallback-with-warning rather than a silently wrong budget. */
    errno = 0;
    char *end = NULL;
    long long want_mb = strtoll(budget_mb, &end, CBM_DECIMAL_BASE);
    if (errno != 0 || end == budget_mb || *end != '\0' || want_mb <= 0) {
        result.invalid = true; /* keep the fraction-derived budget */
        return result;
    }

    result.source = "CBM_MEM_BUDGET_MB";
    size_t want = (size_t)want_mb;
    if (total_ram > 0) {
        /* Compare in MiB space so a valid-but-huge request (e.g. 2^44 MiB, which
         * would overflow the ×MiB byte multiply) clamps cleanly to total_ram. */
        if (want > total_ram / MB_DIVISOR) {
            result.budget = total_ram;
            result.clamped = true;
        } else {
            result.budget = want * MB_DIVISOR;
        }
    } else if (want > SIZE_MAX / MB_DIVISOR) {
        /* RAM detection failed (no clamp target) and the request is
         * astronomically large — cap at SIZE_MAX rather than wrap. */
        result.budget = SIZE_MAX;
    } else {
        result.budget = want * MB_DIVISOR;
    }
    return result;
}

cbm_mem_budget_t cbm_mem_resolve_budget_capped(size_t total_ram, double ram_fraction,
                                               const char *budget_mb, size_t hard_cap_bytes) {
    cbm_mem_budget_t result = cbm_mem_resolve_budget(total_ram, ram_fraction, budget_mb);
    if (hard_cap_bytes > 0 && (result.budget == 0 || result.budget > hard_cap_bytes)) {
        result.budget = hard_cap_bytes;
        result.source = "daemon_worker_cap";
        result.hard_capped = true;
    }
    return result;
}

void cbm_mem_init_with_cap(double ram_fraction, size_t hard_cap_bytes) {
    int expected = 0;
    if (!atomic_compare_exchange_strong(&g_initialized, &expected, 1)) {
        return;
    }

    if (ram_fraction <= 0.0 || ram_fraction > MAX_RAM_FRACTION) {
        ram_fraction = DEFAULT_RAM_FRACTION;
    }

    /* Reduce upfront memory: don't eagerly commit arenas.
     * Force decommit on purge (MADV_FREE_REUSABLE on macOS) so RSS
     * drops immediately instead of staying high until memory pressure. */
#ifdef _WIN32
    /* Decisive check: is the allocator still "preloading"? If so its arena
     * machinery — including every purge path — is inert, and completing init
     * here is what turns the options above from decoration into behaviour. */
    if (_mi_preloading()) {
        _mi_auto_process_init();
        if (_mi_preloading()) {
            /* Arena creation and every purge path stay disabled while this is
             * set, so the process would grow until the OS killed it. Refuse. */
            mem_setup_fatal("preloading",
                            "allocator remained in preloading state after explicit process "
                            "init: arena creation and purging are disabled, memory would "
                            "never be returned to the OS");
        }
        cbm_log_warn("mem.allocator.preloading_completed", "still_preloading", "false", "detail",
                     "allocator was still preloading, so arena creation and purging were "
                     "disabled; process init completed explicitly");
    } else {
        cbm_log_info("mem.allocator.preloading", "state", "already_complete");
    }
#endif

    mem_option_set_verified(mi_option_arena_eager_commit, 0, "arena_eager_commit");
    mem_option_set_verified(mi_option_purge_decommits, SKIP_ONE, "purge_decommits");
    mem_option_set_verified(mi_option_purge_delay, 0, "purge_delay"); /* immediate */
    /* v3 (#832): reclaim abandoned pages on ANY thread's free (=1), restoring the
     * v2 behaviour. mimalloc v3 defaults page_reclaim_on_free=0, so pages a worker
     * thread abandons at exit are NOT reclaimed when the main thread later frees
     * their blocks (and mi_collect cannot touch abandoned pages) — RSS then
     * ratchets across repeated in-process index cycles. The supervised subprocess
     * is the primary cure (the child returns 100% RSS on exit); this is the
     * in-process fallback for any path that stays in-process (kill switch,
     * spawn-fail degrade, embedders). */
    mem_option_set_verified(mi_option_page_reclaim_on_free, 1, "page_reclaim_on_free");

    /* Every option above is inert unless ordinary malloc actually reaches this
     * allocator. That silently stopped being true on Windows — mimalloc's
     * static override is gated on _MSC_VER, which clang/MinGW never defines —
     * and nothing checked, so a long-lived daemon ratcheted committed memory
     * for months (#581). Probe it once, out loud: a real malloc asked whether
     * mimalloc owns it. Anything but true means the tuning here is decoration
     * and freed pages will not come back. */
    cbm_mem_ownership_audit_t audit;
    cbm_mem_audit_ownership(&audit);
    if (!audit.all_owned) {
        /* Name the classes that escaped, because "not owned" is actionable
         * only if you know WHICH sizes. A class listed here either bypasses
         * the override or is misclassified by the routing predicate, and both
         * mean freed pages will not come back. */
        char classes[CBM_SZ_256];
        int length = 0;
        for (int i = 0;
             i < CBM_MEM_OWNERSHIP_CLASSES && length >= 0 && (size_t)length < sizeof(classes);
             i++) {
            if (audit.allocated[i] && !audit.owned[i]) {
                int written = snprintf(classes + length, sizeof(classes) - (size_t)length, "%s%zu",
                                       length ? "," : "", audit.probe_bytes[i]);
                if (written < 0) {
                    break;
                }
                length += written;
            }
        }
        char owned_str[CBM_SZ_32];
        snprintf(owned_str, sizeof(owned_str), "%d/%d", audit.owned_count, audit.probed_count);
        cbm_log_warn("mem.allocator.not_owned", "owned_classes", owned_str, "unowned_bytes",
                     length > 0 ? classes : "?", "detail",
                     "allocations in these size classes are not allocator-owned: purge and "
                     "reclaim options do not apply to them and freed pages stay committed");
    } else {
        cbm_log_info("mem.allocator.owned", "classes", "all");
    }

    /* CBM_MEM_BUDGET_MB env override (memory analogue of CBM_WORKERS).
     * Lets users cap the budget directly without an enclosing cgroup —
     * useful on bare-metal hosts where cgroup memory limits are absent
     * (#363). Explicit override > implicit RAM/cgroup detection. The budget
     * math (fraction default, override, clamp-to-total) lives in the pure,
     * testable cbm_mem_resolve_budget(); this path only reads the env and
     * emits the log/warn lines. */
    cbm_system_info_t info = cbm_system_info();

    char env_buf[CBM_SZ_32];
    const char *env = cbm_safe_getenv("CBM_MEM_BUDGET_MB", env_buf, sizeof(env_buf), NULL);
    cbm_mem_budget_t resolved =
        cbm_mem_resolve_budget_capped(info.total_ram, ram_fraction, env, hard_cap_bytes);
    g_budget = resolved.budget;

    /* The resolver is the single source of truth for the parse + clamp; this
     * path only surfaces its outcome as log lines. */
    if (resolved.invalid) {
        cbm_log_warn("mem.budget.env.invalid", "value", env, "fallback", "ram_fraction");
    } else if (resolved.clamped) {
        char cap_mb[CBM_SZ_32];
        snprintf(cap_mb, sizeof(cap_mb), "%zu", info.total_ram / MB_DIVISOR);
        cbm_log_warn("mem.budget.clamped", "requested_mb", env, "cap_mb", cap_mb);
    }
    if (resolved.hard_capped) {
        char cap_bytes[CBM_SZ_32];
        snprintf(cap_bytes, sizeof(cap_bytes), "%zu", hard_cap_bytes);
        cbm_log_info("mem.budget.worker_cap", "cap_bytes", cap_bytes);
    }

    char budget_mb[CBM_SZ_32];
    char ram_mb[CBM_SZ_32];
    snprintf(budget_mb, sizeof(budget_mb), "%zu", g_budget / MB_DIVISOR);
    snprintf(ram_mb, sizeof(ram_mb), "%zu", info.total_ram / MB_DIVISOR);
    cbm_log_info("mem.init", "budget_mb", budget_mb, "total_ram_mb", ram_mb, "source",
                 resolved.source);
}

void cbm_mem_init(double ram_fraction) {
    cbm_mem_init_with_cap(ram_fraction, 0);
}

size_t cbm_mem_rss(void) {
#if defined(__linux__)
    /* Linux: mimalloc's _mi_prim_process_info() (vendored/mimalloc/src/prim/
     * unix/prim.c) never sets pinfo->current_rss on Linux — it only sets
     * peak_rss (from getrusage's ru_maxrss). current_rss therefore keeps
     * mi_process_info()'s default of pinfo.current_commit: mimalloc's OWN
     * committed-page counter, which this project deliberately tunes low via
     * mi_option_arena_eager_commit=0 + purge_decommits=1 + purge_delay=0
     * (cbm_mem_init) to reduce upfront memory. So on Linux "current_rss" is a
     * low-biased mimalloc-internal metric, not true RSS: under concurrent
     * large-file parsing it can read a few MB while real RSS is multiple GB,
     * silently blinding cbm_mem_over_budget()'s backpressure to real memory
     * pressure (small-but-nonzero, so the `current_rss > 0` guard below never
     * catches it). os_rss() reads /proc/self/statm — authoritative OS RSS,
     * unaffected by mimalloc's accounting — so it is the PRIMARY source on
     * Linux, not a last-resort fallback. macOS/Windows are unaffected:
     * mimalloc sets current_rss correctly there via task_info /
     * GetProcessMemoryInfo. */
    size_t proc_rss = os_rss();
    if (proc_rss > 0) {
        return proc_rss;
    }
    /* Extremely unlikely (/proc unavailable) — fall through to mimalloc. */
#endif
    size_t current_rss = 0;
    size_t peak_rss = 0;
    mi_process_info(NULL, NULL, NULL, &current_rss, &peak_rss, NULL, NULL, NULL);
    if (current_rss > 0) {
        return current_rss;
    }
    /* Fallback for ASan builds (MI_OVERRIDE=0) and any platform where
     * mimalloc's current_rss is unavailable/zero. */
    return os_rss();
}

size_t cbm_mem_peak_rss(void) {
    size_t peak_rss = 0;
    mi_process_info(NULL, NULL, NULL, NULL, &peak_rss, NULL, NULL, NULL);
    /* Peak RSS is by definition >= current RSS. On Linux cbm_mem_rss() reads the
     * live /proc/self/statm value (page-granular), while mimalloc's peak_rss
     * comes from getrusage's ru_maxrss (KB-granular, and it can lag the live
     * statm read by a few pages). Reading the two sources independently lets a
     * fresh current read momentarily exceed the reported peak, breaking the
     * peak >= current invariant. Reconcile them: the true peak is at least the
     * current RSS. (Not observable on macOS, where both come from mimalloc.) */
    size_t current = cbm_mem_rss();
    if (current > peak_rss) {
        peak_rss = current;
    }
    if (peak_rss > 0) {
        return peak_rss;
    }
    /* No OS fallback for peak — return current as best approximation */
    return os_rss();
}

size_t cbm_mem_budget(void) {
    return g_budget;
}

void cbm_mem_set_budget_for_tests(size_t bytes) {
    g_budget = bytes;
}

bool cbm_mem_over_budget(void) {
    size_t rss = cbm_mem_rss();
    check_pressure(rss);
    return rss > g_budget;
}

size_t cbm_mem_worker_budget(int num_workers) {
    if (num_workers <= 0) {
        num_workers = SKIP_ONE;
    }
    return g_budget / (size_t)num_workers;
}

void cbm_mem_collect(void) {
    mi_collect(true);
}

/* ── Memory map (see mem.h for how to read the triple) ─────────────── */

static const size_t MEM_MAP_BUCKET_LIMITS[CBM_MEM_MAP_BUCKETS] = {
    64, 256, 1024, 4096, 16384, 65536, 1048576, 0, /* 0 = open-ended tail */
};

size_t cbm_mem_map_bucket_limit(int bucket) {
    if (bucket < 0 || bucket >= CBM_MEM_MAP_BUCKETS) {
        return 0;
    }
    return MEM_MAP_BUCKET_LIMITS[bucket];
}

static int mem_map_bucket_of(size_t block_size) {
    for (int i = 0; i < CBM_MEM_MAP_BUCKETS - 1; i++) {
        if (block_size <= MEM_MAP_BUCKET_LIMITS[i]) {
            return i;
        }
    }
    return CBM_MEM_MAP_BUCKETS - 1;
}

/* Area visitor: visit_blocks=false means one call per area, so `used` (live
 * block count) x `block_size` is the live byte total without touching blocks. */
static bool mem_map_visit_area(const mi_heap_t *heap, const mi_heap_area_t *area, void *block,
                               size_t block_size, void *arg) {
    (void)heap;
    (void)block;
    (void)block_size;
    cbm_mem_map_t *map = arg;
    if (!area || !map) {
        return true;
    }
    size_t live = area->used * area->block_size;
    int bucket = mem_map_bucket_of(area->block_size);
    map->live_bytes += live;
    map->live_blocks += area->used;
    map->bucket_bytes[bucket] += live;
    map->bucket_blocks[bucket] += area->used;
    map->area_committed_bytes += area->committed;
    map->area_reserved_bytes += area->reserved;
    return true; /* keep walking */
}

static bool mem_map_collect_impl(cbm_mem_map_t *out, bool walk_allocator);

bool cbm_mem_map_collect_os(cbm_mem_map_t *out) {
    return mem_map_collect_impl(out, false);
}

bool cbm_mem_map_collect(cbm_mem_map_t *out) {
    return mem_map_collect_impl(out, true);
}

static bool mem_map_collect_impl(cbm_mem_map_t *out, bool walk_allocator) {
    if (!out) {
        return false;
    }
    memset(out, 0, sizeof(*out));

    size_t elapsed_ms = 0;
    size_t user_ms = 0;
    size_t sys_ms = 0;
    size_t current_rss = 0;
    size_t peak_rss = 0;
    size_t current_commit = 0;
    size_t peak_commit = 0;
    size_t page_faults = 0;
    mi_process_info(&elapsed_ms, &user_ms, &sys_ms, &current_rss, &peak_rss, &current_commit,
                    &peak_commit, &page_faults);
    out->os_rss_bytes = current_rss ? current_rss : cbm_mem_rss();
    out->os_committed_bytes = current_commit;

    /* Does plain malloc actually land in the allocator's regions? This single
     * bit distinguishes "nothing is live" from "the allocator does not own
     * this build's allocations", which are opposite conclusions from the same
     * zero. Probe with a real malloc/free pair rather than inferring from
     * build flags, because the Windows static-CRT override is defined at
     * compile time yet can still fail to take effect at link time. */
    void *probe = malloc(CBM_SZ_64);
    if (probe) {
        out->malloc_is_allocator_owned = mi_is_in_heap_region(probe);
        free(probe);
    }

    /* Walk only heaps this thread may safely read.
     *
     * mi_heap_main() is the process-wide aggregate: any other thread can be
     * allocating into it while the walk runs, which is a data race by
     * construction and TSan reports it as one (init.c:452 in mi_heap_main).
     * A diagnostic must not introduce a race into the code it measures, so the
     * aggregate walk is gone. What remains is safe by ownership: this thread's
     * own theap, plus abandoned pages, which by definition have no owning
     * thread left to race with.
     *
     * That includes mi_heap_main() itself: merely CALLING it reads the
     * allocator's main-heap pointer, which a thread exiting concurrently
     * rewrites from _mi_thread_done -> _mi_theap_default_set. TSan caught
     * exactly that pairing on macOS, so the abandoned-page walk goes too --
     * it could only be reached through mi_heap_main().
     *
     * The cost is coverage -- other threads' live blocks and abandoned pages
     * are not attributed -- and that is exactly what the residual in mem.h
     * exists to carry. An unmeasured map must never read as an empty one. */
    if (walk_allocator) {
        (void)mi_theap_visit_blocks(mi_theap_get_default(), false, mem_map_visit_area, out);
    }
    return true;
}

/* ── Allocator ownership audit (see mem.h) ─────────────────────────── */

static atomic_size_t g_foreign_pointers = ATOMIC_VAR_INIT(0);
static atomic_size_t g_owned_pointers = ATOMIC_VAR_INIT(0);

void cbm_mem_record_pointer_ownership(bool owned) {
    atomic_fetch_add_explicit(owned ? &g_owned_pointers : &g_foreign_pointers, 1,
                              memory_order_relaxed);
}

size_t cbm_mem_foreign_pointer_count(void) {
    return atomic_load_explicit(&g_foreign_pointers, memory_order_relaxed);
}

size_t cbm_mem_owned_pointer_count(void) {
    return atomic_load_explicit(&g_owned_pointers, memory_order_relaxed);
}

void cbm_mem_audit_ownership(cbm_mem_ownership_audit_t *out) {
    if (!out) {
        return;
    }
    memset(out, 0, sizeof(*out));
    /* Span the classes an allocator treats differently: small (bins), page
     * scale, medium, large, and past the point where allocators typically stop
     * using their own pages and go to the OS directly — the class most likely
     * to escape both an override and an ownership predicate. */
    static const size_t PROBES[CBM_MEM_OWNERSHIP_CLASSES] = {
        64, 4096, 65536, 262144, 1048576, 8388608,
    };
    for (int i = 0; i < CBM_MEM_OWNERSHIP_CLASSES; i++) {
        out->probe_bytes[i] = PROBES[i];
        void *block = malloc(PROBES[i]);
        if (!block) {
            continue;
        }
        ((char *)block)[0] = 1; /* touch it so it is really backed */
        out->allocated[i] = true;
        out->probed_count++;
        out->owned[i] = mi_is_in_heap_region(block);
        if (out->owned[i]) {
            out->owned_count++;
        }
        free(block);
    }
    out->all_owned = out->probed_count > 0 && out->owned_count == out->probed_count;
}

/* ── Phase map (see mem.h for how to read it, and its limits) ──────── */

enum { MEM_PHASE_SLOTS = 24 };

typedef struct {
    const char *label; /* static string, compared by pointer then by text */
    int64_t bytes;
    int64_t hits;
} mem_phase_slot_t;

static mem_phase_slot_t g_mem_phases[MEM_PHASE_SLOTS];
static int g_mem_phase_count;
static const char *g_mem_phase_open;
static size_t g_mem_phase_baseline;
static cbm_mutex_t g_mem_phase_mutex;
static atomic_int g_mem_phase_enabled = ATOMIC_VAR_INIT(-1); /* -1 = undecided */

static size_t mem_phase_committed(void) {
    size_t elapsed_ms = 0;
    size_t user_ms = 0;
    size_t sys_ms = 0;
    size_t rss = 0;
    size_t peak_rss = 0;
    size_t commit = 0;
    size_t peak_commit = 0;
    size_t faults = 0;
    mi_process_info(&elapsed_ms, &user_ms, &sys_ms, &rss, &peak_rss, &commit, &peak_commit,
                    &faults);
    return commit;
}

static bool mem_phase_enabled(void) {
    int state = atomic_load_explicit(&g_mem_phase_enabled, memory_order_acquire);
    if (state >= 0) {
        return state == 1;
    }
    char buf[CBM_SZ_16];
    bool on = cbm_safe_getenv("CBM_MEM_PHASES", buf, sizeof(buf), NULL) != NULL && buf[0] == '1';
    if (on) {
        cbm_mutex_init(&g_mem_phase_mutex);
    }
    atomic_store_explicit(&g_mem_phase_enabled, on ? 1 : 0, memory_order_release);
    return on;
}

void cbm_mem_phase_mark(const char *label) {
    if (!mem_phase_enabled()) {
        return;
    }
    size_t now = mem_phase_committed();
    cbm_mutex_lock(&g_mem_phase_mutex);
    if (g_mem_phase_open) {
        int slot = -1;
        for (int i = 0; i < g_mem_phase_count; i++) {
            if (g_mem_phases[i].label == g_mem_phase_open ||
                strcmp(g_mem_phases[i].label, g_mem_phase_open) == 0) {
                slot = i;
                break;
            }
        }
        if (slot < 0 && g_mem_phase_count < MEM_PHASE_SLOTS) {
            slot = g_mem_phase_count++;
            g_mem_phases[slot].label = g_mem_phase_open;
            g_mem_phases[slot].bytes = 0;
            g_mem_phases[slot].hits = 0;
        }
        if (slot >= 0) {
            g_mem_phases[slot].bytes += (int64_t)now - (int64_t)g_mem_phase_baseline;
            g_mem_phases[slot].hits++;
        }
    }
    g_mem_phase_open = label;
    g_mem_phase_baseline = now;
    cbm_mutex_unlock(&g_mem_phase_mutex);
}

void cbm_mem_phase_reset(void) {
    if (!mem_phase_enabled()) {
        return;
    }
    cbm_mutex_lock(&g_mem_phase_mutex);
    memset(g_mem_phases, 0, sizeof(g_mem_phases));
    g_mem_phase_count = 0;
    g_mem_phase_open = NULL;
    g_mem_phase_baseline = mem_phase_committed();
    cbm_mutex_unlock(&g_mem_phase_mutex);
}

int cbm_mem_phase_report_json(char *out, size_t size) {
    if (!out || size == 0) {
        return 0;
    }
    out[0] = '\0';
    if (!mem_phase_enabled()) {
        return 0;
    }
    mem_phase_slot_t copy[MEM_PHASE_SLOTS];
    int count = 0;
    cbm_mutex_lock(&g_mem_phase_mutex);
    count = g_mem_phase_count;
    memcpy(copy, g_mem_phases, sizeof(copy));
    cbm_mutex_unlock(&g_mem_phase_mutex);

    /* Biggest retainer first — insertion sort, count <= MEM_PHASE_SLOTS. */
    for (int i = 1; i < count; i++) {
        mem_phase_slot_t key = copy[i];
        int j = i - 1;
        while (j >= 0 && copy[j].bytes < key.bytes) {
            copy[j + 1] = copy[j];
            j--;
        }
        copy[j + 1] = key;
    }

    int length = 0;
    for (int i = 0; i < count && length >= 0 && (size_t)length < size; i++) {
        int written = snprintf(out + length, size - (size_t)length,
                               "%s{\"label\": \"%s\", \"bytes\": %lld, \"hits\": %lld}",
                               i == 0 ? "" : ", ", copy[i].label ? copy[i].label : "?",
                               (long long)copy[i].bytes, (long long)copy[i].hits);
        if (written < 0) {
            break;
        }
        length += written;
    }
    return length > 0 && (size_t)length < size ? length : 0;
}

/* ── Windows region census (see mem.h) ─────────────────────────────── */

static atomic_int g_mem_census_enabled = -1;

bool cbm_mem_census_enabled(void) {
    int state = atomic_load_explicit(&g_mem_census_enabled, memory_order_acquire);
    if (state >= 0) {
        return state == 1;
    }
    char buf[CBM_SZ_16];
    bool on = cbm_safe_getenv("CBM_MEM_CENSUS", buf, sizeof(buf), NULL) != NULL && buf[0] == '1';
    atomic_store_explicit(&g_mem_census_enabled, on ? 1 : 0, memory_order_release);
    return on;
}

bool cbm_mem_win_census(cbm_mem_win_census_t *out) {
    if (!out) {
        return false;
    }
    memset(out, 0, sizeof(*out));
#ifndef _WIN32
    return false;
#else
    /* Walk the whole user address space once. VirtualQuery reports the largest
     * run of pages sharing a state/type, so the region count also tells us how
     * fragmented the space has become. */
    MEMORY_BASIC_INFORMATION mbi;
    unsigned char *address = NULL;
    while (VirtualQuery(address, &mbi, sizeof(mbi)) == sizeof(mbi)) {
        out->regions++;
        if (mbi.State == MEM_COMMIT) {
            out->committed_total += mbi.RegionSize;
            switch (mbi.Type) {
            case MEM_PRIVATE:
                out->committed_private += mbi.RegionSize;
                if (mbi.RegionSize < (size_t)CBM_SZ_64K) {
                    out->private_small_bytes += mbi.RegionSize;
                    out->private_small_count++;
                } else if (mbi.RegionSize < (size_t)CBM_SZ_1K * CBM_SZ_1K) {
                    out->private_medium_bytes += mbi.RegionSize;
                    out->private_medium_count++;
                } else {
                    out->private_large_bytes += mbi.RegionSize;
                    out->private_large_count++;
                    if (out->large_tracked < CBM_MEM_TRACKED_REGIONS) {
                        out->large_base[out->large_tracked] = mbi.BaseAddress;
                        out->large_size[out->large_tracked] = mbi.RegionSize;
                        out->large_tracked++;
                    }
                }
                if (mbi.RegionSize > out->largest_private_region) {
                    out->largest_private_region = mbi.RegionSize;
                    out->largest_private_base = mbi.BaseAddress;
                }
                if (mbi.RegionSize >= (size_t)CBM_SZ_1K * CBM_SZ_1K) {
                    out->private_regions_1mb++;
                }
                break;
            case MEM_MAPPED:
                out->committed_mapped += mbi.RegionSize;
                break;
            case MEM_IMAGE:
                out->committed_image += mbi.RegionSize;
                break;
            default:
                break;
            }
        } else if (mbi.State == MEM_RESERVE) {
            out->reserved_total += mbi.RegionSize;
        }
        unsigned char *next = (unsigned char *)mbi.BaseAddress + mbi.RegionSize;
        if (next <= address) {
            break; /* no forward progress: stop rather than spin */
        }
        address = next;
    }

    /* The CRT heaps are the pool --wrap cannot reach: anything ucrtbase or a
     * system DLL allocates internally lands here, and Windows heaps do not
     * decommit on free. HeapWalk needs the heap locked to be coherent. */
    HANDLE heaps[64];
    DWORD count = GetProcessHeaps((DWORD)(sizeof(heaps) / sizeof(heaps[0])), heaps);
    if (count > 0) {
        out->heap_walk_ok = true;
        out->crt_heap_count = (unsigned)(count > 64 ? 64 : count);
        for (DWORD i = 0; i < out->crt_heap_count; i++) {
            if (!HeapLock(heaps[i])) {
                continue;
            }
            PROCESS_HEAP_ENTRY entry;
            memset(&entry, 0, sizeof(entry));
            while (HeapWalk(heaps[i], &entry)) {
                if ((entry.wFlags & PROCESS_HEAP_REGION) != 0) {
                    out->crt_heap_committed += entry.Region.dwCommittedSize;
                } else if ((entry.wFlags & PROCESS_HEAP_ENTRY_BUSY) != 0) {
                    out->crt_heap_busy += entry.cbData + entry.cbOverhead;
                }
            }
            HeapUnlock(heaps[i]);
        }
    }
    return true;
#endif
}

/* mi_stats_print_out hands us one chunk at a time; append each to the file. */
static void mem_stats_writer(const char *text, void *arg) {
    FILE *out = arg;
    if (out && text) {
        (void)fputs(text, out);
    }
}

void cbm_mem_census_log(const char *tag) {
    if (!cbm_mem_census_enabled()) {
        return;
    }
    cbm_mem_win_census_t census;
    /* A false return means the OS-region walk is unavailable (non-Windows), not
     * that there is nothing to report: the allocation-site half is the whole
     * point of the cross-platform comparison, so it still runs and the pool
     * fields simply read zero. */
    (void)cbm_mem_win_census(&census);
    /* Kilobytes throughout: the CRT heap measured 0 at megabyte resolution,
     * which reads as "empty" when it may simply be small — a diagnostic that
     * rounds its own evidence away is worse than none. */
    char priv_kb[CBM_SZ_32];
    char crt_kb[CBM_SZ_32];
    char busy_kb[CBM_SZ_32];
    char mapped_kb[CBM_SZ_32];
    char rss_kb[CBM_SZ_32];
    char biggest_kb[CBM_SZ_32];
    char regions[CBM_SZ_32];
    char base[CBM_SZ_32];
    char big_regions[CBM_SZ_32];
    char psmall[CBM_SZ_32];
    char pmed[CBM_SZ_32];
    char plarge[CBM_SZ_32];
    char pcounts[CBM_SZ_64];
    char large_map[CBM_SZ_256];
    char heaps[CBM_SZ_32];
    (void)snprintf(priv_kb, sizeof(priv_kb), "%zu", census.committed_private / CBM_SZ_1K);
    (void)snprintf(crt_kb, sizeof(crt_kb), "%zu", census.crt_heap_committed / CBM_SZ_1K);
    (void)snprintf(busy_kb, sizeof(busy_kb), "%zu", census.crt_heap_busy / CBM_SZ_1K);
    (void)snprintf(mapped_kb, sizeof(mapped_kb), "%zu", census.committed_mapped / CBM_SZ_1K);
    (void)snprintf(rss_kb, sizeof(rss_kb), "%zu", cbm_mem_rss() / CBM_SZ_1K);
    (void)snprintf(biggest_kb, sizeof(biggest_kb), "%zu",
                   census.largest_private_region / CBM_SZ_1K);
    (void)snprintf(regions, sizeof(regions), "%u", census.regions);
    (void)snprintf(base, sizeof(base), "%p", census.largest_private_base);
    (void)snprintf(big_regions, sizeof(big_regions), "%u", census.private_regions_1mb);
    (void)snprintf(psmall, sizeof(psmall), "%zu", census.private_small_bytes / CBM_SZ_1K);
    (void)snprintf(pmed, sizeof(pmed), "%zu", census.private_medium_bytes / CBM_SZ_1K);
    (void)snprintf(plarge, sizeof(plarge), "%zu", census.private_large_bytes / CBM_SZ_1K);
    (void)snprintf(pcounts, sizeof(pcounts), "%u/%u/%u", census.private_small_count,
                   census.private_medium_count, census.private_large_count);
    large_map[0] = '\0';
    for (unsigned r = 0; r < census.large_tracked; r++) {
        char one[CBM_SZ_64];
        (void)snprintf(one, sizeof(one), "%s%p:%zuM", r == 0 ? "" : ",", census.large_base[r],
                       census.large_size[r] / ((size_t)CBM_SZ_1K * CBM_SZ_1K));
        (void)strncat(large_map, one, sizeof(large_map) - strlen(large_map) - 1);
    }
    (void)snprintf(heaps, sizeof(heaps), "%u", census.heap_walk_ok ? census.crt_heap_count : 0);
    /* Attribution alongside the pool totals: the census says which pool holds
     * the memory, the profile says which call sites put it there. Emitting
     * both from one place keeps the two views on the same sample. */
    /* The allocator's OWN view, next to the OS view. If mimalloc's committed
     * area tracks the process's private commit, the memory is the allocator's
     * and it is holding pages against tiny live data; if it stays flat while
     * private climbs, the growth is outside the allocator entirely and no
     * allocator option will ever move it (thirteen of them did not). */
    cbm_mem_map_t map;
    (void)cbm_mem_map_collect(&map);
    char map_live_kb[CBM_SZ_32];
    char map_area_kb[CBM_SZ_32];
    (void)snprintf(map_live_kb, sizeof(map_live_kb), "%zu", map.live_bytes / CBM_SZ_1K);
    (void)snprintf(map_area_kb, sizeof(map_area_kb), "%zu", map.area_committed_bytes / CBM_SZ_1K);
    cbm_log_info("mem.census", "at", tag ? tag : "?", "mi_area_kb", map_area_kb, "mi_live_kb",
                 map_live_kb, "priv_kb", priv_kb, "crt_kb", crt_kb, "crt_busy_kb", busy_kb,
                 "mapped_kb", mapped_kb, "rss_kb", rss_kb, "biggest_kb", biggest_kb, "regions",
                 regions, "big_regions", big_regions, "heaps", heaps, "biggest_base", base,
                 "priv_small_kb", psmall, "priv_med_kb", pmed, "priv_large_kb", plarge,
                 "priv_counts", pcounts, "large_map", large_map);
    /* The phase table answers WHICH bracketed path the committed bytes stayed
     * in. Emitted once at the end rather than per request: it is cumulative,
     * so only the final table carries the verdict. */
    if (tag && strcmp(tag, "connection.exit") == 0) {
        /* Dump the allocator's own stats to a known path. The theap count is
         * the question: if it tracks the request count, each request is
         * getting a fresh thread-heap whose pages are abandoned on exit --
         * invisible to mi_heap_visit, which only sees main and current. */
        char stats_path[CBM_SZ_1K];
        if (cbm_safe_getenv("CBM_MEM_STATS_OUT", stats_path, sizeof(stats_path), NULL) != NULL) {
            FILE *stats = cbm_fopen(stats_path, "wb");
            if (stats) {
                mi_stats_print_out(mem_stats_writer, stats);
                (void)fclose(stats);
            }
        }
        char phases[CBM_SZ_1K];
        if (cbm_mem_phase_report_json(phases, sizeof(phases)) > 0) {
            cbm_log_info("mem.phases", "table", phases);
        }
    }
}
