/*
 * compat.c — Implementations for Windows-only shims.
 *
 * On POSIX, these functions are provided by the standard library via
 * macros in compat.h. On Windows, we implement them here.
 */
#include "foundation/compat.h"
#include "foundation/constants.h"

#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <io.h>
#include <fcntl.h>
#include <sys/stat.h>
#endif

/* ── strndup (Windows lacks it) ───────────────────────────────── */

#ifdef _WIN32
char *cbm_strndup(const char *s, size_t n) {
    if (!s) {
        return NULL;
    }
    size_t len = 0;
    while (len < n && s[len]) {
        len++;
    }
    char *d = (char *)malloc(len + SKIP_ONE);
    if (d) {
        memcpy(d, s, len);
        d[len] = '\0';
    }
    return d;
}
#endif

/* ── strcasestr (Windows lacks it) ────────────────────────────── */

#ifdef _WIN32
char *cbm_strcasestr(const char *haystack, const char *needle) {
    if (!needle[0])
        return (char *)haystack;
    size_t nlen = strlen(needle);
    for (; *haystack; haystack++) {
        if (_strnicmp(haystack, needle, nlen) == 0)
            return (char *)haystack;
    }
    return NULL;
}
#endif

/* ── mkdtemp (Windows lacks it) ───────────────────────────────── */

#ifdef _WIN32
#include <direct.h>
char *cbm_mkdtemp(char *tmpl) {
    /* Build path in static buffer, then copy back to caller.
     * Callers must provide buffers >= CBM_SZ_256 bytes (all test code does). */
    static char buf[CBM_SZ_512];
    if (strncmp(tmpl, "/tmp/", 5) == 0) {
        const char *tmp = getenv("TEMP");
        if (!tmp)
            tmp = getenv("TMP");
        if (!tmp)
            tmp = ".";
        snprintf(buf, sizeof(buf), "%s\\%s", tmp, tmpl + 5);
    } else {
        snprintf(buf, sizeof(buf), "%s", tmpl);
    }
    if (!_mktemp(buf))
        return NULL;
    if (_mkdir(buf) != 0)
        return NULL;
    /* Copy result back — callers now use char[CBM_SZ_256]+ buffers */
    strcpy(tmpl, buf);
    return tmpl;
}
#endif

/* ── mkstemp (Windows lacks it) ───────────────────────────────── */

#ifdef _WIN32
int cbm_mkstemp(char *tmpl) {
    /* Rewrite /tmp/ to %TEMP%\ like cbm_mkdtemp */
    static char buf[CBM_SZ_512];
    if (strncmp(tmpl, "/tmp/", 5) == 0) {
        const char *tmp = getenv("TEMP");
        if (!tmp)
            tmp = getenv("TMP");
        if (!tmp)
            tmp = ".";
        snprintf(buf, sizeof(buf), "%s\\%s", tmp, tmpl + 5);
    } else {
        snprintf(buf, sizeof(buf), "%s", tmpl);
    }
    if (!_mktemp(buf))
        return CBM_NOT_FOUND;
    int fd = _open(buf, _O_CREAT | _O_RDWR | _O_BINARY, _S_IREAD | _S_IWRITE);
    if (fd >= 0)
        strcpy(tmpl, buf);
    return fd;
}
#endif

/* ── clock_gettime (Windows lacks it) ─────────────────────────── */

#ifdef _WIN32
int cbm_clock_gettime(int clk_id, struct timespec *tp) {
    (void)clk_id;
    LARGE_INTEGER freq, count;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&count);
    tp->tv_sec = (time_t)(count.QuadPart / freq.QuadPart);
    tp->tv_nsec = (long)((count.QuadPart % freq.QuadPart) * 1000000000LL / freq.QuadPart);
    return 0;
}
#endif

/* ── getline (Windows lacks it) ───────────────────────────────── */

#ifdef _WIN32
ssize_t cbm_getline(char **lineptr, size_t *n, FILE *stream) {
    if (!lineptr || !n || !stream) {
        return CBM_NOT_FOUND;
    }
    if (!*lineptr || *n == 0) {
        *n = CBM_SZ_128;
        *lineptr = (char *)malloc(*n);
        if (!*lineptr) {
            return CBM_NOT_FOUND;
        }
    }
    size_t pos = 0;
    int c;
    while ((c = fgetc(stream)) != EOF) {
        if (pos + 1 >= *n) {
            size_t new_n = *n * PAIR_LEN;
            char *tmp = (char *)realloc(*lineptr, new_n);
            if (!tmp) {
                return CBM_NOT_FOUND;
            }
            *lineptr = tmp;
            *n = new_n;
        }
        (*lineptr)[pos++] = (char)c;
        if (c == '\n') {
            break;
        }
    }
    if (pos == 0 && c == EOF) {
        return CBM_NOT_FOUND;
    }
    (*lineptr)[pos] = '\0';
    return (ssize_t)pos;
}
#endif
