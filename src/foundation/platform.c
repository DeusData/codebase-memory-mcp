/*
 * platform.c — OS abstraction implementations.
 *
 * macOS, Linux, and Windows. Platform-specific code behind #ifdef guards.
 */
#include "platform.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/platform_internal.h"
#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CBM_NSEC_PER_SEC 1000000000ULL

static uint64_t cbm_platform_scale_fraction(uint64_t remainder, uint64_t multiplier,
                                            uint64_t divisor) {
    uint64_t quotient = 0;
    uint64_t reduced = 0;
    uint64_t mask = 1;
    while (mask <= multiplier / 2) {
        mask <<= 1U;
    }
    for (; mask != 0; mask >>= 1U) {
        quotient *= 2;
        if (reduced >= divisor - reduced) {
            reduced -= divisor - reduced;
            quotient++;
        } else {
            reduced += reduced;
        }
        if ((multiplier & mask) != 0) {
            if (reduced >= divisor - remainder) {
                reduced -= divisor - remainder;
                quotient++;
            } else {
                reduced += remainder;
            }
        }
    }
    return quotient;
}

uint64_t cbm_platform_scale_counter_ns(uint64_t counter, uint64_t frequency) {
    if (frequency == 0) {
        return UINT64_MAX;
    }
    uint64_t whole_seconds = counter / frequency;
    uint64_t remainder = counter % frequency;
    if (whole_seconds > UINT64_MAX / CBM_NSEC_PER_SEC) {
        return UINT64_MAX;
    }
    uint64_t whole_ns = whole_seconds * CBM_NSEC_PER_SEC;
    uint64_t fraction_ns = cbm_platform_scale_fraction(remainder, CBM_NSEC_PER_SEC, frequency);
    return fraction_ns > UINT64_MAX - whole_ns ? UINT64_MAX : whole_ns + fraction_ns;
}

bool cbm_platform_parse_proc_stat_group(const char *stat_line, int64_t *process_group,
                                        bool *execution_quiescent) {
    if (!stat_line || !process_group || !execution_quiescent) {
        return false;
    }
    const char *command_end = strrchr(stat_line, ')');
    char state = '\0';
    long long parent = 0;
    long long group = 0;
    if (!command_end || sscanf(command_end + 1, " %c %lld %lld", &state, &parent, &group) != 3 ||
        state == '\0' || group <= 0) {
        return false;
    }
    (void)parent;
    *process_group = (int64_t)group;
    *execution_quiescent = state == 'Z' || state == 'X';
    return true;
}

/* Canonicalize a Windows drive letter to upper-case in place: "c:/x" -> "C:/x".
 * Windows drive letters are case-insensitive, but a lowercase one (as agent
 * CWDs often report, e.g. Claude Code's "c:\...") otherwise produces a distinct
 * project key ("c-..." vs "C-...") and, on a case-insensitive FS, a colliding
 * cache file that clobbers the good index (#227/#367/#394). Folding to a single
 * canonical form here — at the one path-normalization choke point — keeps the
 * project key, cache file and integrity check consistent regardless of case.
 * Only the strict drive-root form `X:/` or bare `X:` is touched, so ordinary
 * POSIX paths (which never start that way) are unaffected. */
static void cbm_canonicalize_drive(char *path) {
    if (path && path[0] >= 'a' && path[0] <= 'z' && path[1] == ':' &&
        (path[2] == '/' || path[2] == '\0')) {
        path[0] = (char)(path[0] - 'a' + 'A');
    }
}

#ifdef _WIN32

/* ── Windows implementation ────────────────────────────────── */

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <io.h>
#include <sys/stat.h>
#include "foundation/win_utf8.h"

cbm_platform_process_group_state_t cbm_platform_process_group_state(int64_t pgid) {
    (void)pgid;
    return CBM_PLATFORM_PROCESS_GROUP_UNKNOWN;
}

void *cbm_mmap_read(const char *path, size_t *out_size) {
    if (!path || !out_size) {
        return NULL;
    }
    *out_size = 0;

    wchar_t *wpath = cbm_path_to_wide(path);
    if (!wpath) {
        return NULL;
    }

    HANDLE file = CreateFileW(wpath, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING,
                              FILE_ATTRIBUTE_NORMAL, NULL);
    if (file == INVALID_HANDLE_VALUE) {
        free(wpath);
        return NULL;
    }
    LARGE_INTEGER sz;
    if (!GetFileSizeEx(file, &sz) || sz.QuadPart == 0) {
        CloseHandle(file);
        free(wpath);
        return NULL;
    }
    HANDLE mapping = CreateFileMappingW(file, NULL, PAGE_READONLY, 0, 0, NULL);
    if (!mapping) {
        CloseHandle(file);
        free(wpath);
        return NULL;
    }
    void *addr = MapViewOfFile(mapping, FILE_MAP_READ, 0, 0, 0);
    CloseHandle(mapping);
    CloseHandle(file);
    free(wpath);
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
    return cbm_platform_scale_counter_ns((uint64_t)count.QuadPart, (uint64_t)freq.QuadPart);
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
    wchar_t *wpath = cbm_path_to_wide(path);
    if (!wpath) {
        return false;
    }
    DWORD attr = GetFileAttributesW(wpath);
    free(wpath);
    return attr != INVALID_FILE_ATTRIBUTES;
}

bool cbm_is_dir(const char *path) {
    wchar_t *wpath = cbm_path_to_wide(path);
    if (!wpath) {
        return false;
    }
    DWORD attr = GetFileAttributesW(wpath);
    free(wpath);
    return attr != INVALID_FILE_ATTRIBUTES && (attr & FILE_ATTRIBUTE_DIRECTORY);
}

int64_t cbm_file_size(const char *path) {
    wchar_t *wpath = cbm_path_to_wide(path);
    if (!wpath) {
        return CBM_NOT_FOUND;
    }
    WIN32_FILE_ATTRIBUTE_DATA fad;
    BOOL ok = GetFileAttributesExW(wpath, GetFileExInfoStandard, &fad);
    free(wpath);
    if (!ok) {
        return CBM_NOT_FOUND;
    }
    LARGE_INTEGER sz;
    sz.HighPart = (LONG)fad.nFileSizeHigh; // cppcheck-suppress unreadVariable
    sz.LowPart = fad.nFileSizeLow;         // cppcheck-suppress unreadVariable
    return (int64_t)sz.QuadPart;
}

char *cbm_normalize_path_sep(char *path) {
    if (path) {
        for (char *p = path; *p; p++) {
            if (*p == '\\') {
                *p = '/';
            }
        }
        cbm_canonicalize_drive(path);
    }
    return path;
}

#else /* POSIX (macOS + Linux) */

/* ── POSIX implementation ────────────────────────────────── */

#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>

#ifdef __APPLE__
#include <mach/mach_time.h>
#include <pthread.h>
#include <sys/proc.h>
#include <sys/sysctl.h>
#else
#include <sched.h>
#endif

cbm_platform_process_group_state_t cbm_platform_process_group_state(int64_t pgid) {
    if (pgid <= 0) {
        return CBM_PLATFORM_PROCESS_GROUP_UNKNOWN;
    }
#ifdef __APPLE__
    /* KERN_PROC_PGRP omits retained zombies on current macOS kernels. Query the
     * zombie-inclusive table and filter e_pgid ourselves. */
    int mib[] = {CTL_KERN, KERN_PROC, KERN_PROC_ALL, 0};
    size_t required = 0;
    if (sysctl(mib, 4, NULL, &required, NULL, 0) != 0) {
        return CBM_PLATFORM_PROCESS_GROUP_UNKNOWN;
    }
    size_t slack = 16U * sizeof(struct kinfo_proc);
    if (required > SIZE_MAX - slack) {
        return CBM_PLATFORM_PROCESS_GROUP_UNKNOWN;
    }
    size_t capacity = required + slack;
    struct kinfo_proc *entries = (struct kinfo_proc *)calloc(1, capacity);
    if (!entries) {
        return CBM_PLATFORM_PROCESS_GROUP_UNKNOWN;
    }
    size_t received = capacity;
    if (sysctl(mib, 4, entries, &received, NULL, 0) != 0 || received > capacity ||
        received % sizeof(*entries) != 0) {
        free(entries);
        return CBM_PLATFORM_PROCESS_GROUP_UNKNOWN;
    }

    bool saw_member = false;
    size_t count = received / sizeof(*entries);
    for (size_t i = 0; i < count; i++) {
        if ((int64_t)entries[i].kp_eproc.e_pgid != pgid) {
            continue;
        }
        saw_member = true;
        if (entries[i].kp_proc.p_stat != SZOMB) {
            free(entries);
            return CBM_PLATFORM_PROCESS_GROUP_ACTIVE;
        }
    }
    free(entries);
    return saw_member ? CBM_PLATFORM_PROCESS_GROUP_QUIESCED : CBM_PLATFORM_PROCESS_GROUP_UNKNOWN;
#elif defined(__linux__)
    cbm_dir_t *directory = cbm_opendir("/proc");
    if (!directory) {
        return CBM_PLATFORM_PROCESS_GROUP_UNKNOWN;
    }
    bool saw_member = false;
    bool snapshot_unknown = false;
    cbm_dirent_t *entry = NULL;
    while ((entry = cbm_readdir(directory)) != NULL) {
        if (!entry->name[0]) {
            continue;
        }
        bool numeric = true;
        for (const unsigned char *p = (const unsigned char *)entry->name; *p; p++) {
            if (*p < (unsigned char)'0' || *p > (unsigned char)'9') {
                numeric = false;
                break;
            }
        }
        if (!numeric) {
            continue;
        }
        char path[CBM_PATH_MAX];
        int written = snprintf(path, sizeof(path), "/proc/%s/stat", entry->name);
        if (written < 0 || (size_t)written >= sizeof(path)) {
            snapshot_unknown = true;
            continue;
        }
        errno = 0;
        FILE *file = cbm_fopen(path, "r");
        if (!file) {
            if (errno != ENOENT && errno != ESRCH) {
                snapshot_unknown = true;
            }
            continue;
        }
        char stat_line[4096];
        bool read_ok = fgets(stat_line, sizeof(stat_line), file) != NULL;
        (void)fclose(file);
        if (!read_ok) {
            snapshot_unknown = true;
            continue;
        }
        int64_t process_group = 0;
        bool execution_quiescent = false;
        if (!cbm_platform_parse_proc_stat_group(stat_line, &process_group, &execution_quiescent)) {
            snapshot_unknown = true;
            continue;
        }
        if (process_group != pgid) {
            continue;
        }
        saw_member = true;
        if (!execution_quiescent) {
            cbm_closedir(directory);
            return CBM_PLATFORM_PROCESS_GROUP_ACTIVE;
        }
    }
    cbm_closedir(directory);
    if (snapshot_unknown) {
        return CBM_PLATFORM_PROCESS_GROUP_UNKNOWN;
    }
    return saw_member ? CBM_PLATFORM_PROCESS_GROUP_QUIESCED : CBM_PLATFORM_PROCESS_GROUP_UNKNOWN;
#else
    return CBM_PLATFORM_PROCESS_GROUP_UNKNOWN;
#endif
}

/* ── Memory mapping ──────────────────────────── */

void *cbm_mmap_read(const char *path, size_t *out_size) {
    if (!path || !out_size) {
        return NULL;
    }
    *out_size = 0;

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

/* ── Timing ───────────────────────────── */

#ifdef __APPLE__
/* Both histories fixed the same lazy-initialization race here. pthread_once
 * replaces the hand-rolled compare-exchange plus spin-wait, and the identity
 * fallback keeps a zeroed timebase from dividing by zero. */
static mach_timebase_info_data_t timebase_info = {1, 1};
static pthread_once_t timebase_once = PTHREAD_ONCE_INIT;

static void cbm_timebase_initialize(void) {
    (void)mach_timebase_info(&timebase_info);
    if (timebase_info.numer == 0 || timebase_info.denom == 0) {
        timebase_info.numer = 1;
        timebase_info.denom = 1;
    }
}

uint64_t cbm_now_ns(void) {
    (void)pthread_once(&timebase_once, cbm_timebase_initialize);
    uint64_t ticks = mach_absolute_time();
    return ticks * timebase_info.numer / timebase_info.denom;
}
#else
uint64_t cbm_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}
#endif

#define CBM_USEC_PER_SEC 1000000ULL

uint64_t cbm_now_ms(void) {
    return cbm_now_ns() / CBM_USEC_PER_SEC;
}

/* ── System info ───────────────────────────── */

int cbm_nprocs(void) {
#ifdef __APPLE__
    int ncpu = 0;
    size_t len = sizeof(ncpu);
    if (sysctlbyname("hw.ncpu", &ncpu, &len, NULL, 0) == 0 && ncpu > 0) {
        return ncpu;
    }
    enum { FILE_EXISTS = 1 };
    return FILE_EXISTS;
#else
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    return n > 0 ? (int)n : 1;
#endif
}

/* ── File system ──────────────────────────── */

bool cbm_file_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0;
}

bool cbm_is_dir(const char *path) {
    struct stat st;
    return stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}

int64_t cbm_file_size(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) {
        return CBM_NOT_FOUND;
    }
    return (int64_t)st.st_size;
}

char *cbm_normalize_path_sep(char *path) {
    /* Normalize on ALL platforms — backslash paths can arrive via stored
     * data, cross-platform DB files, or Windows-style arguments. */
    if (path) {
        for (char *p = path; *p; p++) {
            if (*p == '\\') {
                *p = '/';
            }
        }
        cbm_canonicalize_drive(path);
    }
    return path;
}

#endif /* _WIN32 */

/* ── Environment variables ──────────────────────────── */

/* Thread-safe getenv: iterates environ directly instead of calling getenv().
 * getenv() is flagged by concurrency-mt-unsafe because the returned pointer
 * can be invalidated by setenv/putenv in another thread. We copy to a
 * caller-owned buffer immediately. */
#ifdef _WIN32
#include <stdlib.h>
#define CBM_ENVIRON _environ
#elif defined(__APPLE__)
#include <crt_externs.h>
#define CBM_ENVIRON (*_NSGetEnviron())
#else
extern char **environ;
#define CBM_ENVIRON environ
#endif

static const char *cbm_platform_copy_environment_value(char *buf, size_t buf_sz,
                                                       const char *value) {
    if (!buf || buf_sz == 0 || !value) {
        return NULL;
    }
    size_t length = strlen(value);
    if (length >= buf_sz) {
        buf[0] = '\0';
        return NULL;
    }
    memcpy(buf, value, length + 1U);
    return buf;
}

typedef enum {
    CBM_PLATFORM_ENV_MISSING = 0,
    CBM_PLATFORM_ENV_EMPTY,
    CBM_PLATFORM_ENV_VALUE,
    CBM_PLATFORM_ENV_TOO_LONG,
    CBM_PLATFORM_ENV_ERROR,
} cbm_platform_env_status_t;

static cbm_platform_env_status_t cbm_platform_read_environment_value(const char *name, char *buf,
                                                                     size_t buf_sz) {
    if (!name || !name[0] || !buf || buf_sz == 0) {
        return CBM_PLATFORM_ENV_ERROR;
    }
    buf[0] = '\0';
#ifdef _WIN32
    /* #996 Layer 2: _environ holds ANSI-code-page bytes, NOT UTF-8. A
     * non-ASCII value (USERPROFILE of C:\Users\Kovács János, or a Greek/CJK
     * CBM_CACHE_DIR) arrives here either mojibake'd or with unrepresentable
     * characters replaced by '?', which is INVALID in Windows paths — every
     * downstream wide-safe file API then fails no matter how correct it is.
     * Read the value wide and convert to genuine UTF-8, matching the
     * UTF-8-path convention the rest of the codebase (cbm_fopen, _wmkdir,
     * SQLite VFS) already assumes. */
    {
        wchar_t wname[CBM_SZ_256];
        int wn = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, name, -1, wname, CBM_SZ_256);
        if (wn > 0) {
            SetLastError(ERROR_SUCCESS);
            DWORD needed = GetEnvironmentVariableW(wname, NULL, 0U);
            DWORD environment_error = GetLastError();
            if (needed == 0U) {
                if (environment_error == ERROR_ENVVAR_NOT_FOUND) {
                    return CBM_PLATFORM_ENV_MISSING;
                }
                /* An existing empty variable is distinct from a missing one. */
                return environment_error == ERROR_SUCCESS ? CBM_PLATFORM_ENV_EMPTY
                                                          : CBM_PLATFORM_ENV_ERROR;
            }
            wchar_t *wval = calloc((size_t)needed, sizeof(*wval));
            if (!wval) {
                return CBM_PLATFORM_ENV_ERROR;
            }
            SetLastError(ERROR_SUCCESS);
            DWORD got = GetEnvironmentVariableW(wname, wval, needed);
            DWORD read_error = GetLastError();
            if (got >= needed || (got == 0U && read_error != ERROR_SUCCESS)) {
                free(wval);
                return CBM_PLATFORM_ENV_ERROR;
            }
            if (got == 0U) {
                free(wval);
                return CBM_PLATFORM_ENV_EMPTY;
            }
            char *utf8 = cbm_wide_to_utf8(wval);
            free(wval);
            if (!utf8) {
                return CBM_PLATFORM_ENV_ERROR;
            }
            const char *copied = cbm_platform_copy_environment_value(buf, buf_sz, utf8);
            free(utf8);
            return copied ? CBM_PLATFORM_ENV_VALUE : CBM_PLATFORM_ENV_TOO_LONG;
        }
        return CBM_PLATFORM_ENV_ERROR;
    }
#else
    char **env = CBM_ENVIRON;
    if (env) {
        size_t nlen = strlen(name);
        for (; *env; env++) {
            if (strncmp(*env, name, nlen) == 0 && (*env)[nlen] == '=') {
                const char *value = *env + nlen + SKIP_ONE;
                if (!value[0]) {
                    return CBM_PLATFORM_ENV_EMPTY;
                }
                return cbm_platform_copy_environment_value(buf, buf_sz, value)
                           ? CBM_PLATFORM_ENV_VALUE
                           : CBM_PLATFORM_ENV_TOO_LONG;
            }
        }
    }
#endif
    return CBM_PLATFORM_ENV_MISSING;
}

const char *cbm_safe_getenv(const char *name, char *buf, size_t buf_sz, const char *fallback) {
    cbm_platform_env_status_t result = cbm_platform_read_environment_value(name, buf, buf_sz);
    if (result == CBM_PLATFORM_ENV_VALUE || result == CBM_PLATFORM_ENV_EMPTY) {
        return buf;
    }
    if (result == CBM_PLATFORM_ENV_MISSING && fallback) {
        return cbm_platform_copy_environment_value(buf, buf_sz, fallback);
    }
    return NULL;
}

static char cbm_ascii_lower(char c) {
    return (c >= 'A' && c <= 'Z') ? (char)(c + ('a' - 'A')) : c;
}

static bool cbm_ascii_eq_ignore_case(const char *a, const char *b) {
    if (!a || !b) {
        return false;
    }
    while (*a && *b) {
        if (cbm_ascii_lower(*a) != cbm_ascii_lower(*b)) {
            return false;
        }
        a++;
        b++;
    }
    return *a == '\0' && *b == '\0';
}

bool cbm_env_flag_enabled(const char *name) {
    char buf[CBM_SZ_32];
    const char *value = cbm_safe_getenv(name, buf, sizeof(buf), NULL);
    if (!value || value[0] == '\0') {
        return false;
    }
    return strcmp(value, "0") != 0 && !cbm_ascii_eq_ignore_case(value, "false") &&
           !cbm_ascii_eq_ignore_case(value, "off") && !cbm_ascii_eq_ignore_case(value, "no");
}

bool cbm_getenv_fits(const char *name, char *buf, size_t buf_sz, bool *present) {
    if (present) {
        *present = false;
    }
    cbm_platform_env_status_t result = cbm_platform_read_environment_value(name, buf, buf_sz);
    if (present && (result == CBM_PLATFORM_ENV_VALUE || result == CBM_PLATFORM_ENV_TOO_LONG)) {
        *present = true;
    }
    return result == CBM_PLATFORM_ENV_VALUE;
}

/* ── Home directory (cross-platform) ───────────────────── */

const char *cbm_get_home_dir(void) {
    static CBM_TLS char buf[CBM_SZ_1K];
    char tmp[CBM_SZ_256] = "";

    cbm_safe_getenv("HOME", tmp, sizeof(tmp), NULL);
    if (tmp[0]) {
        snprintf(buf, sizeof(buf), "%s", tmp);
        cbm_normalize_path_sep(buf);
        return buf;
    }

    cbm_safe_getenv("USERPROFILE", tmp, sizeof(tmp), NULL);
    if (tmp[0]) {
        snprintf(buf, sizeof(buf), "%s", tmp);
        cbm_normalize_path_sep(buf);
        return buf;
    }
    return NULL;
}

/* ── App config directories (cross-platform) ────────── */

const char *cbm_app_config_dir(void) {
    static CBM_TLS char buf[CBM_SZ_1K];
    char tmp[CBM_SZ_256] = "";
#ifdef _WIN32
    cbm_safe_getenv("APPDATA", tmp, sizeof(tmp), NULL);
    if (tmp[0]) {
        snprintf(buf, sizeof(buf), "%s", tmp);
        cbm_normalize_path_sep(buf);
        return buf;
    }
    const char *home = cbm_get_home_dir();
    if (home) {
        snprintf(buf, sizeof(buf), "%s/AppData/Roaming", home);
        return buf;
    }
    return NULL;
#else
    /* Linux: XDG_CONFIG_HOME or ~/.config */
    cbm_safe_getenv("XDG_CONFIG_HOME", tmp, sizeof(tmp), NULL);
    if (tmp[0]) {
        snprintf(buf, sizeof(buf), "%s", tmp);
        return buf;
    }
    const char *home = cbm_get_home_dir();
    if (home) {
        snprintf(buf, sizeof(buf), "%s/.config", home);
        return buf;
    }
    return NULL;
#endif /* _WIN32 */
}

const char *cbm_app_local_dir(void) {
#ifdef _WIN32
    static CBM_TLS char buf[CBM_SZ_1K];
    char tmp[CBM_SZ_256] = "";
    cbm_safe_getenv("LOCALAPPDATA", tmp, sizeof(tmp), NULL);
    if (tmp[0]) {
        snprintf(buf, sizeof(buf), "%s", tmp);
        cbm_normalize_path_sep(buf);
        return buf;
    }
    const char *home = cbm_get_home_dir();
    if (home) {
        snprintf(buf, sizeof(buf), "%s/AppData/Local", home);
        return buf;
    }
    return NULL;
#else
    return cbm_app_config_dir();
#endif
}

/* ── Cache directory ────────────────────────── */

const char *cbm_resolve_cache_dir(void) {
    static CBM_TLS char buf[CBM_SZ_4K];
    static const char missing[] = "\x1f"
                                  "CBM_CACHE_DIR_MISSING"
                                  "\x1f";
    const char *configured = cbm_safe_getenv("CBM_CACHE_DIR", buf, sizeof(buf), missing);
    if (!configured) {
        /* Present but not representable in the product path bound. */
        return NULL;
    }
    if (strcmp(configured, missing) != 0 && configured[0]) {
        cbm_normalize_path_sep(buf);
        return buf;
    }
    const char *home = cbm_get_home_dir();
    if (!home) {
        return NULL;
    }
    int written = snprintf(buf, sizeof(buf), "%s/.cache/codebase-memory-mcp", home);
    if (written <= 0 || (size_t)written >= sizeof(buf)) {
        buf[0] = '\0';
        return NULL;
    }
    return buf;
}
