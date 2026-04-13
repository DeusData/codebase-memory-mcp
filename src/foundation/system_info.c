/*
 * system_info.c — CPU core count and RAM detection.
 *
 * macOS: sysctlbyname for core counts, hw.memsize for RAM.
 * Linux: sysconf + sysinfo().
 * Windows: GetSystemInfo + GlobalMemoryStatusEx.
 *
 * Results are cached after first call (immutable hardware properties).
 */
#include "foundation/constants.h"

enum { DEFAULT_CORES = 1, MIN_WORKERS = 1 };
#include "foundation/platform.h"
#include <stdint.h> // uint64_t
#include <string.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#elif defined(__APPLE__)
#include <sys/sysctl.h>
#elif !defined(_WIN32) /* Linux */
#include <unistd.h>
#include <sys/sysinfo.h>

#endif

/* ── macOS detection ─────────────────────────────────────────────── */

#ifdef __APPLE__

static int sysctl_int(const char *name, int fallback) {
    int val = 0;
    size_t len = sizeof(val);
    if (sysctlbyname(name, &val, &len, NULL, 0) == 0 && val > 0) {
        return val;
    }
    return fallback;
}

static size_t sysctl_size(const char *name, size_t fallback) {
    size_t val = 0;
    size_t len = sizeof(val);
    if (sysctlbyname(name, &val, &len, NULL, 0) == 0 && val > 0) {
        return val;
    }
    /* Try CBM_SZ_64-bit variant */
    uint64_t val64 = 0;
    len = sizeof(val64);
    if (sysctlbyname(name, &val64, &len, NULL, 0) == 0 && val64 > 0) {
        return (size_t)val64;
    }
    return fallback;
}

static cbm_system_info_t detect_system_macos(void) {
    cbm_system_info_t info;
    memset(&info, 0, sizeof(info));

    info.total_cores = sysctl_int("hw.ncpu", DEFAULT_CORES);
    info.perf_cores = sysctl_int("hw.perflevel0.physicalcpu", info.total_cores);

    /* If perflevel sysctls fail (Intel Mac), perf = total */
    int eff = sysctl_int("hw.perflevel1.physicalcpu", 0);
    if (info.perf_cores + eff > info.total_cores) {
        info.perf_cores = info.total_cores;
    }

    info.total_ram = sysctl_size("hw.memsize", 0);
    return info;
}

#elif !defined(_WIN32) /* Linux */

static cbm_system_info_t detect_system_linux(void) {
    cbm_system_info_t info;
    memset(&info, 0, sizeof(info));

    long nprocs = sysconf(_SC_NPROCESSORS_ONLN);
    info.total_cores = nprocs > 0 ? (int)nprocs : 1;
    info.perf_cores = info.total_cores; /* Linux doesn't distinguish P/E */

    struct sysinfo si;
    if (sysinfo(&si) == 0) {
        info.total_ram = (size_t)si.totalram * (size_t)si.mem_unit;
    }

    return info;
}

#endif /* __APPLE__ / Linux */

/* ── Windows detection ───────────────────────────────────────────── */

#ifdef _WIN32
static cbm_system_info_t detect_system_windows(void) {
    cbm_system_info_t info;
    memset(&info, 0, sizeof(info));

    SYSTEM_INFO si;
    GetSystemInfo(&si);
    info.total_cores = (int)si.dwNumberOfProcessors;
    if (info.total_cores < 1) {
        info.total_cores = SKIP_ONE;
    }
    info.perf_cores = info.total_cores;

    MEMORYSTATUSEX ms;
    ms.dwLength = sizeof(ms);
    if (GlobalMemoryStatusEx(&ms)) {
        info.total_ram = (size_t)ms.ullTotalPhys;
    }

    return info;
}
#endif

/* ── Public API ──────────────────────────────────────────────────── */

static int info_cached = 0;
static cbm_system_info_t cached_info;

cbm_system_info_t cbm_system_info(void) {
    if (!info_cached) {
#ifdef _WIN32
        cached_info = detect_system_windows();
#elif defined(__APPLE__)
        cached_info = detect_system_macos();
#else
        cached_info = detect_system_linux();
#endif
        info_cached = SKIP_ONE;
    }
    return cached_info;
}

int cbm_default_worker_count(bool initial) {
    cbm_system_info_t info = cbm_system_info();
    if (initial) {
        /* Use all cores for initial indexing — user is waiting */
        return info.total_cores;
    }
    /* Incremental: leave headroom for user's apps */
    int workers = info.perf_cores - SKIP_ONE;
    return workers > 0 ? workers : MIN_WORKERS;
}
