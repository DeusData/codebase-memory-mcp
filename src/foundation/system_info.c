/*
 * system_info.c — CPU core count and RAM detection.
 *
 * macOS: sysctlbyname for core counts, hw.memsize for RAM.
 * BSD: sysconf + sysctl(HW_PHYSMEM64 / HW_PHYSMEM).
 * Linux: sysconf + sysinfo(), with cgroup-aware overrides when running
 *        inside a container so the limits reflect the cgroup's effective
 *        CPU quota and memory cap rather than the host's totals.
 * Windows: GetSystemInfo + GlobalMemoryStatusEx.
 *
 * Results are cached after first call (immutable hardware properties).
 */
#include "foundation/constants.h"

enum { DEFAULT_CORES = 1, MIN_WORKERS = 1, CBM_WORKERS_MAX = 256 };
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/system_info_internal.h"
#include <stdint.h> // uint64_t
#include <stdlib.h> // strtol
#include <string.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#elif defined(__APPLE__)
#include <sys/sysctl.h>
#elif defined(__NetBSD__) || defined(__FreeBSD__) || defined(__OpenBSD__)
#include <unistd.h>
#include <sys/types.h>
#include <sys/sysctl.h>
#else /* Linux */
/* limits.h for ULLONG_MAX, stdio.h for fopen/fread, stdlib.h for strto*. */
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/sysinfo.h>
#include <unistd.h>
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

#elif defined(__NetBSD__) || defined(__FreeBSD__) || defined(__OpenBSD__)

static cbm_system_info_t detect_system_bsd(void) {
    cbm_system_info_t info;
    memset(&info, 0, sizeof(info));

    long nprocs = sysconf(_SC_NPROCESSORS_ONLN);
    info.total_cores = nprocs > 0 ? (int)nprocs : 1;
    info.perf_cores = info.total_cores;

#if defined(__OpenBSD__)
    int mib[2] = {CTL_HW, HW_PHYSMEM};
#else
    int mib[2] = {CTL_HW, HW_PHYSMEM64};
#endif
    uint64_t physmem = 0;
    size_t len = sizeof(physmem);
    if (sysctl(mib, 2, &physmem, &len, NULL, 0) == 0 && physmem > 0) {
        info.total_ram = (size_t)physmem;
    }

    return info;
}

#elif !defined(_WIN32) /* Linux */

/* Read up to (bufsz-1) bytes from `path` into `buf`, NUL-terminate, and strip
 * trailing whitespace. Returns the (stripped) byte count, or -1 if the file
 * could not be opened or read. */
static int read_small_file(const char *path, char *buf, size_t bufsz) {
    FILE *fp = fopen(path, "re");
    if (fp == NULL) {
        return -1;
    }
    size_t n = fread(buf, 1, bufsz - 1, fp);
    fclose(fp);
    while (n > 0 && (buf[n - 1] == '\n' || buf[n - 1] == ' ' || buf[n - 1] == '\t')) {
        n--;
    }
    buf[n] = '\0';
    return (int)n;
}

/* Effective CPU count from a cgroup file tree. See header for contract. */
int cbm_detect_cgroup_cpus(const char *cgroup_root) {
    char path[CBM_PATH_MAX];
    char buf[CBM_SZ_64];

    /* cgroup v2: "<root>/cpu.max" — "<quota> <period>" or "max <period>". */
    snprintf(path, sizeof(path), "%s/cpu.max", cgroup_root);
    if (read_small_file(path, buf, sizeof(buf)) > 0) {
        if (strncmp(buf, "max", 3) == 0) {
            return -1; /* no quota → caller falls back to sysconf */
        }
        long quota = 0;
        long period = 0;
        if (sscanf(buf, "%ld %ld", &quota, &period) == 2 && quota > 0 && period > 0) {
            long n = (quota + period - 1) / period; /* ceil(quota/period) */
            return n > 0 ? (int)n : MIN_WORKERS;
        }
        return -1;
    }

    /* cgroup v1: ".../cpu/cpu.cfs_quota_us" and ".../cpu/cpu.cfs_period_us".
     * A quota of -1 means unlimited in cgroup v1. */
    snprintf(path, sizeof(path), "%s/cpu/cpu.cfs_quota_us", cgroup_root);
    if (read_small_file(path, buf, sizeof(buf)) <= 0) {
        return -1;
    }
    long quota = strtol(buf, NULL, CBM_DECIMAL_BASE);
    if (quota <= 0) {
        return -1;
    }

    snprintf(path, sizeof(path), "%s/cpu/cpu.cfs_period_us", cgroup_root);
    if (read_small_file(path, buf, sizeof(buf)) <= 0) {
        return -1;
    }
    long period = strtol(buf, NULL, CBM_DECIMAL_BASE);
    if (period <= 0) {
        return -1;
    }

    long n = (quota + period - 1) / period;
    return n > 0 ? (int)n : MIN_WORKERS;
}

/* Effective memory limit from a cgroup file tree. See header for contract. */
size_t cbm_detect_cgroup_mem(const char *cgroup_root) {
    char path[CBM_PATH_MAX];
    char buf[CBM_SZ_64];

    /* cgroup v2: "<root>/memory.max" — "max" or integer bytes. */
    snprintf(path, sizeof(path), "%s/memory.max", cgroup_root);
    if (read_small_file(path, buf, sizeof(buf)) > 0) {
        if (strncmp(buf, "max", 3) == 0) {
            return 0;
        }
        char *end = NULL;
        unsigned long long n = strtoull(buf, &end, CBM_DECIMAL_BASE);
        if (end == buf || n == 0) {
            return 0;
        }
        return (size_t)n;
    }

    /* cgroup v1: ".../memory/memory.limit_in_bytes". The sentinel for
     * "unlimited" is a very large value (~PAGE_COUNTER_MAX); treat anything
     * past half of ULLONG_MAX as effectively unlimited. */
    snprintf(path, sizeof(path), "%s/memory/memory.limit_in_bytes", cgroup_root);
    if (read_small_file(path, buf, sizeof(buf)) <= 0) {
        return 0;
    }
    char *end = NULL;
    unsigned long long n = strtoull(buf, &end, CBM_DECIMAL_BASE);
    if (end == buf || n == 0 || n >= (ULLONG_MAX / 2)) {
        return 0;
    }
    return (size_t)n;
}

static cbm_system_info_t detect_system_linux(void) {
    cbm_system_info_t info;
    memset(&info, 0, sizeof(info));

    /* Host fallbacks. */
    long nprocs = sysconf(_SC_NPROCESSORS_ONLN);
    int host_cpus = nprocs > 0 ? (int)nprocs : DEFAULT_CORES;

    size_t host_ram = 0;
    struct sysinfo si;
    if (sysinfo(&si) == 0) {
        host_ram = (size_t)si.totalram * (size_t)si.mem_unit;
    }

    /* Cgroup-aware overrides. min(cgroup, host) defends against
     * mis-mounted cgroups that report values larger than the host. */
    int cg_cpus = cbm_detect_cgroup_cpus("/sys/fs/cgroup");
    info.total_cores = (cg_cpus > 0 && cg_cpus < host_cpus) ? cg_cpus : host_cpus;
    info.perf_cores = info.total_cores; /* Linux doesn't distinguish P/E */

    size_t cg_ram = cbm_detect_cgroup_mem("/sys/fs/cgroup");
    info.total_ram = (cg_ram > 0 && (host_ram == 0 || cg_ram < host_ram)) ? cg_ram : host_ram;

    return info;
}

#endif /* __APPLE__ / BSD / Linux */

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
#elif defined(__NetBSD__) || defined(__FreeBSD__) || defined(__OpenBSD__)
        cached_info = detect_system_bsd();
#else
        cached_info = detect_system_linux();
#endif
        info_cached = SKIP_ONE;
    }
    return cached_info;
}

int cbm_default_worker_count(bool initial) {
    /* CBM_WORKERS env override (clamped to [1, CBM_WORKERS_MAX]).
     * Useful inside containers where sysconf(_SC_NPROCESSORS_ONLN)
     * reports host CPUs rather than the cgroup's effective CPU quota.
     * Same precedence shape as other CBM_* env overrides:
     * explicit override > implicit detection. */
    char buf[CBM_SZ_32];
    if (cbm_safe_getenv("CBM_WORKERS", buf, sizeof(buf), NULL) != NULL) {
        long n = strtol(buf, NULL, CBM_DECIMAL_BASE);
        if (n >= MIN_WORKERS && n <= CBM_WORKERS_MAX) {
            return (int)n;
        }
        cbm_log_warn("workers.env.invalid", "value", buf, "fallback", "sysconf");
    }

    cbm_system_info_t info = cbm_system_info();
    if (initial) {
        /* Use all cores for initial indexing — user is waiting */
        return info.total_cores;
    }
    /* Incremental: leave headroom for user's apps */
    int workers = info.perf_cores - SKIP_ONE;
    return workers > 0 ? workers : MIN_WORKERS;
}
