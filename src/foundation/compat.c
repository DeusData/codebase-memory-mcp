/*
 * compat.c — Implementations for Windows-only shims.
 *
 * On POSIX, these functions are provided by the standard library via
 * macros in compat.h. On Windows, we implement them here.
 */
#include "foundation/compat.h"
#include "foundation/constants.h"

#include <errno.h>
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

static int rewrite_tmp_template(char *tmpl, size_t tmpl_sz) {
    if (!tmpl || tmpl_sz == 0) {
        errno = EINVAL;
        return CBM_NOT_FOUND;
    }

    size_t len = 0;
    while (len < tmpl_sz && tmpl[len]) {
        len++;
    }
    if (len == tmpl_sz || len >= CBM_PATH_MAX) {
        errno = ENAMETOOLONG;
        return CBM_NOT_FOUND;
    }

    char original[CBM_PATH_MAX];
    memcpy(original, tmpl, len + SKIP_ONE);

    enum { TMP_PREFIX_LEN = sizeof("/tmp/") - SKIP_ONE };
    int n;
    if (strncmp(original, "/tmp/", TMP_PREFIX_LEN) == 0) {
        const char *tmp = getenv("TEMP");
        if (!tmp)
            tmp = getenv("TMP");
        if (!tmp)
            tmp = ".";
        n = snprintf(tmpl, tmpl_sz, "%s\\%s", tmp, original + TMP_PREFIX_LEN);
    } else {
        n = snprintf(tmpl, tmpl_sz, "%s", original);
    }
    if (n < 0 || (size_t)n >= tmpl_sz) {
        errno = ENAMETOOLONG;
        return CBM_NOT_FOUND;
    }
    return 0;
}

char *cbm_mkdtemp(char *tmpl) {
    /* Build path in thread-local storage, then copy back to caller.
     * Callers must provide buffers >= CBM_SZ_256 bytes (all test code does). */
    static CBM_TLS char buf[CBM_PATH_MAX];
    int n = snprintf(buf, sizeof(buf), "%s", tmpl ? tmpl : "");
    if (n < 0 || (size_t)n >= sizeof(buf)) {
        errno = ENAMETOOLONG;
        return NULL;
    }
    if (rewrite_tmp_template(buf, sizeof(buf)) != 0)
        return NULL;
    if (!_mktemp(buf))
        return NULL;
    if (_mkdir(buf) != 0)
        return NULL;
    /* Normalize to forward slashes. Callers embed this path in JSON repo_path
     * (where "\t"/"\a" are invalid escapes → index fails) and pass it to git -C.
     * Windows file APIs accept forward slashes, so the created dir is unaffected. */
    for (char *p = buf; *p; p++) {
        if (*p == '\\') {
            *p = '/';
        }
    }
    /* Copy result back — callers now use char[CBM_SZ_256]+ buffers */
    strcpy(tmpl, buf);
    return tmpl;
}
#endif

/* ── mkstemp (Windows lacks it) ───────────────────────────────── */

#ifdef _WIN32
int cbm_mkstemp(char *tmpl) {
    /* Legacy ABI: caller owns an unsized template buffer. New shared code should
     * use cbm_mkstemp_s() so long paths fail instead of copying back blindly. */
    static CBM_TLS char buf[CBM_PATH_MAX];
    int n = snprintf(buf, sizeof(buf), "%s", tmpl ? tmpl : "");
    if (n < 0 || (size_t)n >= sizeof(buf)) {
        errno = ENAMETOOLONG;
        return CBM_NOT_FOUND;
    }
    int fd = cbm_mkstemp_s(buf, sizeof(buf));
    if (fd >= 0)
        strcpy(tmpl, buf);
    return fd;
}

int cbm_mkstemp_s(char *tmpl, size_t tmpl_sz) {
    if (rewrite_tmp_template(tmpl, tmpl_sz) != 0)
        return CBM_NOT_FOUND;

    char *pattern = cbm_strdup(tmpl);
    if (!pattern) {
        errno = ENOMEM;
        return CBM_NOT_FOUND;
    }

    enum { MKSTEMP_COLLISION_RETRIES = CBM_SZ_32 };
    for (int attempt = 0; attempt < MKSTEMP_COLLISION_RETRIES; attempt++) {
        int n = snprintf(tmpl, tmpl_sz, "%s", pattern);
        if (n < 0 || (size_t)n >= tmpl_sz) {
            free(pattern);
            errno = ENAMETOOLONG;
            return CBM_NOT_FOUND;
        }
        if (!_mktemp(tmpl)) {
            free(pattern);
            return CBM_NOT_FOUND;
        }
        int fd = _open(tmpl, _O_CREAT | _O_EXCL | _O_RDWR | _O_BINARY, _S_IREAD | _S_IWRITE);
        if (fd >= 0) {
            free(pattern);
            return fd;
        }
        if (errno != EEXIST) {
            break;
        }
    }

    free(pattern);
    return CBM_NOT_FOUND;
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
