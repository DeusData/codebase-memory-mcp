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

#include "foundation/constants.h"

#define MAX_RAM_FRACTION 1.0
#define DEFAULT_RAM_FRACTION 0.5
#include <mimalloc.h>
#include <stdatomic.h>
#include <stdio.h>

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

void cbm_mem_init(double ram_fraction) {
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
    mi_option_set(mi_option_arena_eager_commit, 0);
    mi_option_set(mi_option_purge_decommits, SKIP_ONE);
    mi_option_set(mi_option_purge_delay, 0); /* immediate purge, no 1s delay */
    /* v3 (#832): reclaim abandoned pages on ANY thread's free (=1), restoring the
     * v2 behaviour. mimalloc v3 defaults page_reclaim_on_free=0, so pages a worker
     * thread abandons at exit are NOT reclaimed when the main thread later frees
     * their blocks (and mi_collect cannot touch abandoned pages) — RSS then
     * ratchets across repeated in-process index cycles. The supervised subprocess
     * is the primary cure (the child returns 100% RSS on exit); this is the
     * in-process fallback for any path that stays in-process (kill switch,
     * spawn-fail degrade, embedders). */
    mi_option_set(mi_option_page_reclaim_on_free, 1);

    /* CBM_MEM_BUDGET_MB env override (memory analogue of CBM_WORKERS).
     * Lets users cap the budget directly without an enclosing cgroup —
     * useful on bare-metal hosts where cgroup memory limits are absent
     * (#363). Explicit override > implicit RAM/cgroup detection. */
    char env_buf[CBM_SZ_32];
    if (cbm_safe_getenv("CBM_MEM_BUDGET_MB", env_buf, sizeof(env_buf), NULL) != NULL) {
        long mb = strtol(env_buf, NULL, CBM_DECIMAL_BASE);
        if (mb > 0) {
            g_budget = (size_t)mb * MB_DIVISOR;
            char ovr_mb[CBM_SZ_32];
            snprintf(ovr_mb, sizeof(ovr_mb), "%ld", mb);
            cbm_log_info("mem.init", "budget_mb", ovr_mb, "source", "CBM_MEM_BUDGET_MB");
            return;
        }
        cbm_log_warn("mem.budget.env.invalid", "value", env_buf, "fallback", "ram_fraction");
    }

    cbm_system_info_t info = cbm_system_info();
    g_budget = (size_t)((double)info.total_ram * ram_fraction);

    char budget_mb[CBM_SZ_32];
    char ram_mb[CBM_SZ_32];
    snprintf(budget_mb, sizeof(budget_mb), "%zu", g_budget / MB_DIVISOR);
    snprintf(ram_mb, sizeof(ram_mb), "%zu", info.total_ram / MB_DIVISOR);
    cbm_log_info("mem.init", "budget_mb", budget_mb, "total_ram_mb", ram_mb);
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
