/*
 * platform.c — OS abstraction implementations.
 *
 * macOS, Linux, and Windows. Platform-specific code behind #ifdef guards.
 */
#include "platform.h"
#include "compat.h"

#include <stdint.h> // uint64_t, int64_t

#ifdef _WIN32

/* ── Windows implementation ───────────────────────────────────── */

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <io.h>
#include <sys/stat.h>

void *cbm_mmap_read(const char *path, size_t *out_size) {
    if (!path || !out_size) {
        return NULL;
    }
    *out_size = 0;

    HANDLE file = CreateFileA(path, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING,
                              FILE_ATTRIBUTE_NORMAL, NULL);
    if (file == INVALID_HANDLE_VALUE) {
        return NULL;
    }
    LARGE_INTEGER sz;
    if (!GetFileSizeEx(file, &sz) || sz.QuadPart == 0) {
        CloseHandle(file);
        return NULL;
    }
    HANDLE mapping = CreateFileMappingA(file, NULL, PAGE_READONLY, 0, 0, NULL);
    if (!mapping) {
        CloseHandle(file);
        return NULL;
    }
    void *addr = MapViewOfFile(mapping, FILE_MAP_READ, 0, 0, 0);
    CloseHandle(mapping);
    CloseHandle(file);
    if (!addr) {
        return NULL;
    }
    *out_size = (size_t)sz.QuadPart;
    return addr;
}

void cbm_munmap(void *addr, size_t size) {
    (void)size;
    if (addr) {
        UnmapViewOfFile(addr);
    }
}

uint64_t cbm_now_ns(void) {
    LARGE_INTEGER freq, count;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&count);
    return (uint64_t)count.QuadPart * 1000000000ULL / (uint64_t)freq.QuadPart;
}

#define CBM_USEC_PER_SEC 1000000ULL

uint64_t cbm_now_ms(void) {
    return cbm_now_ns() / CBM_USEC_PER_SEC;
}

int cbm_nprocs(void) {
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    return (int)si.dwNumberOfProcessors > 0 ? (int)si.dwNumberOfProcessors : 1;
}

bool cbm_file_exists(const char *path) {
    DWORD attr = GetFileAttributesA(path);
    return attr != INVALID_FILE_ATTRIBUTES;
}

bool cbm_is_dir(const char *path) {
    DWORD attr = GetFileAttributesA(path);
    return attr != INVALID_FILE_ATTRIBUTES && (attr & FILE_ATTRIBUTE_DIRECTORY);
}

int64_t cbm_file_size(const char *path) {
    WIN32_FILE_ATTRIBUTE_DATA fad;
    if (!GetFileAttributesExA(path, GetFileExInfoStandard, &fad)) {
        return -1;
    }
    LARGE_INTEGER sz;
    sz.HighPart = (LONG)fad.nFileSizeHigh; // cppcheck-suppress unreadVariable
    sz.LowPart = fad.nFileSizeLow;         // cppcheck-suppress unreadVariable
    return (int64_t)sz.QuadPart;
}

const char *cbm_self_exe_path(void) {
    static char buf[1024];
    DWORD len = GetModuleFileNameA(NULL, buf, sizeof(buf));
    if (len == 0 || len >= sizeof(buf)) {
        return "";
    }
    cbm_normalize_path_sep(buf);
    return buf;
}

char *cbm_normalize_path_sep(char *path) {
    if (path) {
        for (char *p = path; *p; p++) {
            if (*p == '\\') {
                *p = '/';
            }
        }
    }
    return path;
}

/* Convert MSYS2/Git Bash path to native Windows path (in-place).
 * /c/Users/... → C:/Users/...   (single drive letter after leading /) */
static void msys_to_native(char *path) {
    if (path[0] == '/' && path[1] != '\0' && path[2] == '/') {
        char drive = path[1];
        if ((drive >= 'a' && drive <= 'z') || (drive >= 'A' && drive <= 'Z')) {
            /* Shift left: overwrite leading '/' with drive letter, inject ':' */
            path[0] = (char)(drive >= 'a' ? drive - 32 : drive); /* uppercase */
            path[1] = ':';
            /* rest of path starting from path[2] is already correct */
        }
    }
}

const char *cbm_home_dir(void) {
    static char buf[1024];
    const char *h = getenv("HOME");
    if (!h || !h[0]) {
        h = getenv("USERPROFILE");
    }
    if (!h || !h[0]) {
        return NULL;
    }
    snprintf(buf, sizeof(buf), "%s", h);
    cbm_normalize_path_sep(buf);
    msys_to_native(buf);
    return buf;
}

const char *cbm_app_config_dir(void) {
    static char buf[1024];
    const char *d = getenv("APPDATA");
    if (!d || !d[0]) {
        /* Fallback: USERPROFILE/AppData/Roaming */
        const char *home = cbm_home_dir();
        if (!home) {
            return NULL;
        }
        snprintf(buf, sizeof(buf), "%s/AppData/Roaming", home);
        return buf;
    }
    snprintf(buf, sizeof(buf), "%s", d);
    cbm_normalize_path_sep(buf);
    msys_to_native(buf);
    return buf;
}

const char *cbm_app_local_dir(void) {
    static char buf[1024];
    const char *d = getenv("LOCALAPPDATA");
    if (!d || !d[0]) {
        /* Fallback: USERPROFILE/AppData/Local */
        const char *home = cbm_home_dir();
        if (!home) {
            return NULL;
        }
        snprintf(buf, sizeof(buf), "%s/AppData/Local", home);
        return buf;
    }
    snprintf(buf, sizeof(buf), "%s", d);
    cbm_normalize_path_sep(buf);
    msys_to_native(buf);
    return buf;
}

#else /* POSIX (macOS + Linux) */

/* ── POSIX implementation ─────────────────────────────────────── */

#include <fcntl.h> // open, O_RDONLY
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>

#ifdef __APPLE__
#include <mach/mach_time.h>
#include <mach-o/dyld.h> // _NSGetExecutablePath
#include <sys/sysctl.h>
#else
#include <sched.h>
#endif

/* ── Memory mapping ────────────────────────────────────────────── */

void *cbm_mmap_read(const char *path, size_t *out_size) {
    if (!path || !out_size) {
        return NULL;
    }
    *out_size = 0;

    // NOLINTNEXTLINE(misc-include-cleaner) — open provided by standard header
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        return NULL;
    }

    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size == 0) {
        close(fd);
        return NULL;
    }

    void *addr = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (addr == MAP_FAILED) {
        return NULL;
    }
    *out_size = (size_t)st.st_size;
    return addr;
}

void cbm_munmap(void *addr, size_t size) {
    if (addr && size > 0) {
        munmap(addr, size);
    }
}

/* ── Timing ────────────────────────────────────────────────────── */

#ifdef __APPLE__
static mach_timebase_info_data_t timebase_info;
static int timebase_init = 0;

uint64_t cbm_now_ns(void) {
    if (!timebase_init) {
        mach_timebase_info(&timebase_info);
        timebase_init = 1;
    }
    uint64_t ticks = mach_absolute_time();
    return ticks * timebase_info.numer / timebase_info.denom;
}
#else
uint64_t cbm_now_ns(void) {
    struct timespec ts;
    cbm_clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}
#endif

#define CBM_USEC_PER_SEC 1000000ULL

uint64_t cbm_now_ms(void) {
    return cbm_now_ns() / CBM_USEC_PER_SEC;
}

/* ── System info ───────────────────────────────────────────────── */

int cbm_nprocs(void) {
#ifdef __APPLE__
    int ncpu = 0;
    size_t len = sizeof(ncpu);
    if (sysctlbyname("hw.ncpu", &ncpu, &len, NULL, 0) == 0 && ncpu > 0) {
        return ncpu;
    }
    return 1;
#else
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    return n > 0 ? (int)n : 1;
#endif
}

/* ── File system ───────────────────────────────────────────────── */

bool cbm_file_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0;
}

bool cbm_is_dir(const char *path) {
    struct stat st;
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    return stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}

int64_t cbm_file_size(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) {
        return -1;
    }
    return (int64_t)st.st_size;
}

const char *cbm_self_exe_path(void) {
    static char buf[1024];
#ifdef __APPLE__
    uint32_t sz = sizeof(buf);
    if (_NSGetExecutablePath(buf, &sz) != 0) {
        return "";
    }
    /* Resolve symlinks to get canonical path */
    char *resolved = realpath(buf, NULL);
    if (resolved) {
        snprintf(buf, sizeof(buf), "%s", resolved);
        free(resolved);
    }
#else
    ssize_t len = readlink("/proc/self/exe", buf, sizeof(buf) - 1);
    if (len <= 0) {
        return "";
    }
    buf[len] = '\0';
#endif
    return buf;
}

char *cbm_normalize_path_sep(char *path) {
    /* No-op on POSIX — paths already use forward slashes. */
    (void)path;
    return path;
}

const char *cbm_home_dir(void) {
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    return getenv("HOME");
}

const char *cbm_app_config_dir(void) {
    static char buf[1024];
#ifdef __APPLE__
    /* macOS: callers prepend "Library/Application Support/..." */
    return cbm_home_dir();
#else
    /* Linux: XDG_CONFIG_HOME or ~/.config */
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    const char *xdg = getenv("XDG_CONFIG_HOME");
    if (xdg && xdg[0]) {
        snprintf(buf, sizeof(buf), "%s", xdg);
        return buf;
    }
    const char *home = cbm_home_dir();
    if (!home) {
        return NULL;
    }
    snprintf(buf, sizeof(buf), "%s/.config", home);
    return buf;
#endif
}

const char *cbm_app_local_dir(void) {
    /* On POSIX there is no distinction between "roaming" and "local" app data.
     * Delegate to cbm_app_config_dir() so callers compile on all platforms. */
    return cbm_app_config_dir();
}

#endif /* _WIN32 */
