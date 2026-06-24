/*
 * vmem.c — Budget-tracked virtual memory allocator.
 *
 * Allocates via mmap (POSIX) or VirtualAlloc (Windows) to bypass
 * ptmalloc2's per-thread arena fragmentation. All allocations are
 * page-aligned, zeroed by the OS, and tracked against a configurable
 * budget (fraction of physical RAM).
 *
 * Pressure events are logged with hysteresis to avoid log storms
 * near the budget boundary.
 */
#include "vmem.h"
#include "platform.h"
#include "log.h"

#include <stdatomic.h>
#include <stdio.h>  /* snprintf */
#include <string.h> /* memset */

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <sys/mman.h>
#include <unistd.h> /* sysconf, _SC_PAGESIZE */
#endif

/* ── Static state (initialized once) ──────────────────────────── */

static atomic_size_t g_allocated = 0; /* current total allocated */
static atomic_size_t g_peak = 0;      /* high-water mark */
static size_t g_budget = 0;           /* budget in bytes */
static atomic_int g_was_over = 0;     /* hysteresis: was over budget? */
static atomic_int g_initialized = 0;  /* init guard */

/* ── Page size ─────────────────────────────────────────────────── */

static size_t page_size(void) {
#ifdef _WIN32
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    return (size_t)si.dwPageSize;
#else
    long ps = sysconf(_SC_PAGESIZE);
    return ps > 0 ? (size_t)ps : 4096;
#endif
}

/* Round up to page boundary. */
static size_t round_to_page(size_t size) {
    size_t ps = page_size();
    return (size + ps - SKIP_ONE) & ~(ps - SKIP_ONE);
}

/* ── Pressure logging ──────────────────────────────────────────── */

#define MB_DIVISOR ((size_t)(CBM_SZ_1K * CBM_SZ_1K))

static void check_pressure(size_t allocated) {
    if (g_budget == 0) {
        return;
    }

    bool over = allocated > g_budget;
    int was = atomic_load(&g_was_over);

    if (over && !was) {
        /* Transition: under → over */
        atomic_store(&g_was_over, 1);
        char alloc_mb[CBM_SZ_32];
        char budget_mb[CBM_SZ_32];
        char pct_str[CBM_SZ_16];
        snprintf(alloc_mb, sizeof(alloc_mb), "%zu", allocated / MB_DIVISOR);
        snprintf(budget_mb, sizeof(budget_mb), "%zu", g_budget / MB_DIVISOR);
        snprintf(pct_str, sizeof(pct_str), "%zu",
                 g_budget > 0 ? (allocated * CBM_PERCENT) / g_budget : 0);
        cbm_log_warn("mem.pressure.warn", "allocated_mb", alloc_mb, "budget_mb", budget_mb, "pct",
                     pct_str);
    } else if (!over && was) {
        /* Transition: over → under */
        atomic_store(&g_was_over, 0);
        char alloc_mb[CBM_SZ_32];
        char budget_mb[CBM_SZ_32];
        char pct_str[CBM_SZ_16];
        snprintf(alloc_mb, sizeof(alloc_mb), "%zu", allocated / MB_DIVISOR);
        snprintf(budget_mb, sizeof(budget_mb), "%zu", g_budget / MB_DIVISOR);
        snprintf(pct_str, sizeof(pct_str), "%zu",
                 g_budget > 0 ? (allocated * CBM_PERCENT) / g_budget : 0);
        cbm_log_info("mem.pressure.ok", "allocated_mb", alloc_mb, "budget_mb", budget_mb, "pct",
                     pct_str);
    }
}

/* ── Update peak ───────────────────────────────────────────────── */

static void update_peak(size_t allocated) {
    size_t old_peak = atomic_load(&g_peak);
    while (allocated > old_peak) {
        if (atomic_compare_exchange_weak(&g_peak, &old_peak, allocated)) {
            break;
        }
    }
}

/* ── Public API ────────────────────────────────────────────────── */

void cbm_vmem_init(double ram_fraction) {
    /* Only first call takes effect */
    int expected = 0;
    if (!atomic_compare_exchange_strong(&g_initialized, &expected, 1)) {
        return;
    }

    if (ram_fraction <= 0.0 || ram_fraction > 1.0) {
        ram_fraction = 0.5;
    }

    cbm_system_info_t info = cbm_system_info();
    g_budget = (size_t)((double)info.total_ram * ram_fraction);

    char budget_mb[CBM_SZ_32];
    char ram_mb[CBM_SZ_32];
    snprintf(budget_mb, sizeof(budget_mb), "%zu", g_budget / MB_DIVISOR);
    snprintf(ram_mb, sizeof(ram_mb), "%zu", info.total_ram / MB_DIVISOR);
    cbm_log_info("vmem.init", "budget_mb", budget_mb, "total_ram_mb", ram_mb);
}

void *cbm_vmem_alloc(size_t size) {
    if (size == 0) {
        return NULL;
    }

    size_t alloc_size = round_to_page(size);

#ifdef _WIN32
    void *ptr = VirtualAlloc(NULL, alloc_size, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
#else
    void *ptr = mmap(NULL, alloc_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ptr == MAP_FAILED) {
        ptr = NULL;
    }
#endif

    if (!ptr) {
        cbm_log_error("vmem.alloc.fail", "size_mb", size > MB_DIVISOR ? "large" : "small");
        return NULL;
    }

    size_t new_total = atomic_fetch_add(&g_allocated, alloc_size) + alloc_size;
    update_peak(new_total);
    check_pressure(new_total);

    return ptr;
}

void cbm_vmem_free(void *ptr, size_t size) {
    if (!ptr || size == 0) {
        return;
    }

    size_t free_size = round_to_page(size);

#ifdef _WIN32
    VirtualFree(ptr, 0, MEM_RELEASE);
#else
    munmap(ptr, free_size);
#endif

    size_t new_total = atomic_fetch_sub(&g_allocated, free_size) - free_size;
    check_pressure(new_total);
}

size_t cbm_vmem_allocated(void) {
    return atomic_load(&g_allocated);
}

size_t cbm_vmem_peak(void) {
    return atomic_load(&g_peak);
}

size_t cbm_vmem_budget(void) {
    return g_budget;
}

bool cbm_vmem_over_budget(void) {
    return atomic_load(&g_allocated) > g_budget;
}

size_t cbm_vmem_worker_budget(int num_workers) {
    if (num_workers <= 0) {
        num_workers = SKIP_ONE;
    }
    return g_budget / (size_t)num_workers;
}
