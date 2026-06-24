/*
 * compat.h — Cross-platform compatibility macros and shims.
 *
 * Provides portable TLS, sleep, strdup/strndup, and getline across
 * POSIX (macOS/Linux) and Windows. Include this instead of using
 * platform-specific macros directly.
 */
#ifndef CBM_COMPAT_H
#define CBM_COMPAT_H

#include <stddef.h>
#include <stdio.h>

/* ── Thread-local storage ─────────────────────────────────────── */
/* _Thread_local is C11 standard — works on GCC, Clang, and MSVC (2019+).
 * __declspec(thread) is MSVC-only and doesn't work on MinGW GCC. */
#define CBM_TLS _Thread_local

/* ── Sleep ────────────────────────────────────────────────────── */
#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#define cbm_usleep(us) Sleep((DWORD)((us) / 1000))
#else
#include <unistd.h>
#define cbm_usleep(us) usleep((useconds_t)(us))
#endif

/* ── strdup / strndup ─────────────────────────────────────────── */
#ifdef _WIN32
#define cbm_strdup _strdup
/* Implemented in compat.c */
char *cbm_strndup(const char *s, size_t n);
#else
#define cbm_strdup strdup
#define cbm_strndup strndup
#endif

/* ── getline (Windows lacks it) ───────────────────────────────── */
#ifdef _WIN32
/* Implemented in compat.c */
ssize_t cbm_getline(char **lineptr, size_t *n, FILE *stream);
#else
#define cbm_getline getline
#endif

/* ── fileno ───────────────────────────────────────────────────── */
#ifdef _WIN32
#define cbm_fileno _fileno
#else
#define cbm_fileno fileno
#endif

/* ── strcasestr (Windows lacks it) ────────────────────────────── */
#ifdef _WIN32
/* Implemented in compat.c */
char *cbm_strcasestr(const char *haystack, const char *needle);
#else
#define cbm_strcasestr strcasestr
#endif

/* ── mkdir portability ───────────────────────────────────────── */
#ifdef _WIN32
#include <direct.h>
#define cbm_mkdir(path) _mkdir(path)
#else
#include <sys/stat.h>
#define cbm_mkdir(path) mkdir(path, 0755)
#endif

/* ── clock_gettime / nanosleep (Windows lacks them) ──────────── */
#include <time.h>
#ifdef _WIN32
#ifndef CLOCK_MONOTONIC
#define CLOCK_MONOTONIC 1
#endif
/* Implemented in compat.c */
int cbm_clock_gettime(int clk_id, struct timespec *tp);
static inline int cbm_nanosleep(const struct timespec *req, struct timespec *rem) {
    (void)rem;
    Sleep((DWORD)(req->tv_sec * 1000 + req->tv_nsec / 1000000));
    return 0;
}
#else
#define cbm_clock_gettime clock_gettime
#define cbm_nanosleep nanosleep
#endif

/* ── gmtime_r (Windows lacks it) ─────────────────────────────── */
#ifdef _WIN32
static inline struct tm *cbm_gmtime_r(const time_t *timep, struct tm *result) {
    return gmtime_s(result, timep) == 0 ? result : NULL;
}
#else
#define cbm_gmtime_r gmtime_r
#endif

/* ── mkdtemp (Windows lacks it) ──────────────────────────────── */
#ifdef _WIN32
/* Translates /tmp/ to %TEMP%\ and copies result back to tmpl.
 * Callers MUST use char buf[CBM_SZ_256] or larger. */
char *cbm_mkdtemp(char *tmpl);
#else
#define cbm_mkdtemp mkdtemp
#endif

/* ── mkstemp (Windows lacks it) ──────────────────────────────── */
#ifdef _WIN32
int cbm_mkstemp(char *tmpl);
#else
#define cbm_mkstemp mkstemp
#endif

/* ── setenv / unsetenv (Windows lacks them) ──────────────────── */
#ifdef _WIN32
static inline int cbm_setenv(const char *name, const char *value, int overwrite) {
    (void)overwrite;
    return _putenv_s(name, value);
}
static inline int cbm_unsetenv(const char *name) {
    return _putenv_s(name, "");
}
#else
#define cbm_setenv setenv
#define cbm_unsetenv unsetenv
#endif

/* ── pipe (Windows uses _pipe) ───────────────────────────────── */
#ifdef _WIN32
#include <io.h>
#include <fcntl.h>
#define cbm_pipe(fds) _pipe(fds, 4096, _O_BINARY)
#else
#define cbm_pipe(fds) pipe(fds)
#endif

/* ── Temp directory helper ───────────────────────────────────── */
static inline const char *cbm_tmpdir(void) {
#ifdef _WIN32
    const char *t = getenv("TEMP");
    if (!t)
        t = getenv("TMP");
    return t ? t : ".";
#else
    return "/tmp";
#endif
}

/* ── Signal handling ──────────────────────────────────────────── */
/* Windows doesn't have sigaction; provide macro to select signal API. */
#ifdef _WIN32
#define CBM_HAS_SIGACTION 0
#else
#define CBM_HAS_SIGACTION 1
#endif

#endif /* CBM_COMPAT_H */
