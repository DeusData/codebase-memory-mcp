/*
 * compat.c — Implementations for Windows-only shims.
 *
 * On POSIX, these functions are provided by the standard library via
 * macros in compat.h. On Windows, we implement them here.
 */
#include "foundation/compat.h"
#include "foundation/constants.h"
#include "foundation/secure_random.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <io.h>
#include <fcntl.h>
#include <sys/stat.h>
#endif

int64_t cbm_stat_mtime_ns(const struct stat *st) {
    if (!st) {
        return 0;
    }
#if defined(__APPLE__)
    return ((int64_t)st->st_mtimespec.tv_sec * (int64_t)CBM_NSEC_PER_SEC) +
           (int64_t)st->st_mtimespec.tv_nsec;
#elif defined(_WIN32)
    return (int64_t)st->st_mtime * (int64_t)CBM_NSEC_PER_SEC;
#else
    return ((int64_t)st->st_mtim.tv_sec * (int64_t)CBM_NSEC_PER_SEC) + (int64_t)st->st_mtim.tv_nsec;
#endif
}

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
#include <aclapi.h>
#include "foundation/win_utf8.h"

/* Create `path` with an explicit private security descriptor: owner stamped
 * to the token user and a protected, inheritable, user-only DACL. A plain
 * _mkdir takes the token's DEFAULT owner and the parent's inheritable DACL;
 * under an Administrators-default-owner policy (standard on Windows Server
 * and GitHub's elevated runners) the directory is then born owned by
 * BUILTIN\Administrators with foreign inherited grants, and every private-
 * namespace validation (activation-transaction staging, launcher directory
 * policy) rejects the temp directory this function just made. Returns false
 * if the descriptor cannot be built or creation fails; the caller falls
 * back to plain _mkdir so degraded environments (Wine) keep working —
 * downstream validation still gates security there. */
static bool win_mkdtemp_private_create(const char *path) {
    bool created = false;
    HANDLE token = NULL;
    TOKEN_USER *user = NULL;
    PACL acl = NULL;
    DWORD needed = 0;
    wchar_t *wide = cbm_path_to_wide(path);
    if (wide && OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token) &&
        !GetTokenInformation(token, TokenUser, NULL, 0, &needed) &&
        GetLastError() == ERROR_INSUFFICIENT_BUFFER && (user = malloc(needed)) != NULL &&
        GetTokenInformation(token, TokenUser, user, needed, &needed) && user->User.Sid &&
        IsValidSid(user->User.Sid)) {
        EXPLICIT_ACCESSW access;
        memset(&access, 0, sizeof(access));
        access.grfAccessPermissions = GENERIC_ALL;
        access.grfAccessMode = SET_ACCESS;
        access.grfInheritance = SUB_CONTAINERS_AND_OBJECTS_INHERIT;
        access.Trustee.TrusteeForm = TRUSTEE_IS_SID;
        access.Trustee.TrusteeType = TRUSTEE_IS_USER;
        access.Trustee.ptstrName = (LPWSTR)user->User.Sid;
        SECURITY_DESCRIPTOR descriptor;
        if (SetEntriesInAclW(1, &access, NULL, &acl) == ERROR_SUCCESS &&
            InitializeSecurityDescriptor(&descriptor, SECURITY_DESCRIPTOR_REVISION) &&
            SetSecurityDescriptorDacl(&descriptor, TRUE, acl, FALSE) &&
            SetSecurityDescriptorOwner(&descriptor, user->User.Sid, FALSE) &&
            SetSecurityDescriptorControl(&descriptor, SE_DACL_PROTECTED, SE_DACL_PROTECTED)) {
            SECURITY_ATTRIBUTES attributes;
            attributes.nLength = sizeof(attributes);
            attributes.lpSecurityDescriptor = &descriptor;
            attributes.bInheritHandle = FALSE;
            created = CreateDirectoryW(wide, &attributes) != 0;
        }
    }
    if (acl) {
        (void)LocalFree(acl);
    }
    free(user);
    if (token) {
        (void)CloseHandle(token);
    }
    free(wide);
    return created;
}

/* Single owner of the "/tmp/" -> %TEMP% template rewrite, shared by
 * cbm_mkdtemp and cbm_mkstemp_s so the rule is stated once. */
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

/* Suffix length is the mkdtemp(3) contract ("XXXXXX"); the attempt ceiling
 * bounds collision retries so an exhausted namespace fails instead of looping. */
enum { MKDTEMP_SUFFIX_LEN = 6, MKDTEMP_MAX_ATTEMPTS = 128 };

char *cbm_mkdtemp(char *tmpl) {
    /* Per-call storage: daemon project workers create staging directories
     * concurrently, so a shared scratch buffer would be a data race. The
     * expanded path is copied back into the caller's template before return. */
    char buf[CBM_PATH_MAX];
    int n = snprintf(buf, sizeof(buf), "%s", tmpl ? tmpl : "");
    if (n < 0 || (size_t)n >= sizeof(buf)) {
        errno = ENAMETOOLONG;
        return NULL;
    }
    if (rewrite_tmp_template(buf, sizeof(buf)) != 0)
        return NULL;

    size_t length = strlen(buf);
    if (length < MKDTEMP_SUFFIX_LEN ||
        strcmp(buf + length - MKDTEMP_SUFFIX_LEN, "XXXXXX") != 0) {
        errno = EINVAL;
        return NULL;
    }

    /* The suffix is minted here rather than by _wmktemp: _wmktemp derives the
     * name from the process id and returns the SAME name to every caller until
     * that name exists on disk, so two threads expanding one template raced to
     * the identical directory. Generating it from cbm_secure_random removes the
     * shared derivation entirely, and doing it on the UTF-8 buffer keeps the
     * wide conversion at the creation calls below, where non-ASCII %TEMP%
     * components are handled correctly anyway. */
    static const char alphabet[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    bool created = false;
    for (int attempt = 0; attempt < MKDTEMP_MAX_ATTEMPTS; attempt++) {
        unsigned char random_suffix[MKDTEMP_SUFFIX_LEN];
        if (!cbm_secure_random(random_suffix, sizeof(random_suffix))) {
            errno = EIO;
            return NULL;
        }
        for (size_t index = 0; index < sizeof(random_suffix); index++) {
            buf[length - sizeof(random_suffix) + index] =
                alphabet[random_suffix[index] % (sizeof(alphabet) - SKIP_ONE)];
        }

        if (win_mkdtemp_private_create(buf)) {
            created = true;
            break;
        }

        /* Keep the existing compatibility fallback when an explicit private
         * descriptor is unavailable: every private-namespace validation
         * downstream depends on the explicit descriptor, so a silent fallback
         * turns into unexplained owner/DACL refusals far from this call site.
         * A name collision is retried; any other filesystem refusal is returned
         * to the caller immediately. */
        DWORD create_error = GetLastError();
        wchar_t *wide_directory = cbm_utf8_to_wide(buf);
        errno = 0;
        int mkdir_result = wide_directory ? _wmkdir(wide_directory) : -1;
        int mkdir_error = errno;
        free(wide_directory);
        if (mkdir_result == 0) {
            static volatile LONG fallback_reported;
            if (InterlockedCompareExchange(&fallback_reported, 1, 0) == 0) {
                (void)fprintf(stderr,
                              "warning: private temp-directory descriptor unavailable "
                              "(os %lu); using default directory security\n",
                              (unsigned long)create_error);
            }
            created = true;
            break;
        }
        if (create_error == ERROR_ALREADY_EXISTS || create_error == ERROR_FILE_EXISTS ||
            mkdir_error == EEXIST) {
            continue;
        }
        errno = mkdir_error != 0 ? mkdir_error : EACCES;
        return NULL;
    }
    if (!created) {
        errno = EEXIST;
        return NULL;
    }
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

bool cbm_path_for_file_api(const char *path, char *out, size_t out_size) {
    if (!path || !out || out_size == 0) {
        return false;
    }
#ifdef _WIN32
    wchar_t *wide = cbm_path_to_wide(path);
    char *narrow = wide ? cbm_wide_to_utf8(wide) : NULL;
    free(wide);
    if (!narrow) {
        return false;
    }
    size_t needed = strlen(narrow);
    bool fits = needed < out_size;
    if (fits) {
        memcpy(out, narrow, needed + 1U);
    }
    free(narrow);
    return fits;
#else
    size_t needed = strlen(path);
    if (needed >= out_size) {
        return false;
    }
    memcpy(out, path, needed + 1U);
    return true;
#endif
}

/* ── mkstemp (Windows lacks it) ───────────────────────────────── */

#ifdef _WIN32
int cbm_mkstemp(char *tmpl) {
    /* Legacy ABI: caller owns an unsized template buffer. New shared code should
     * use cbm_mkstemp_s() so long paths fail instead of copying back blindly.
     * Per-call storage: daemon project workers create staging files
     * concurrently, so a shared scratch buffer would be a data race. */
    char buf[CBM_PATH_MAX];
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
        /* Wide-API expansion and open: worker staging files land inside
         * CBM_CACHE_DIR, which users may place at non-ASCII paths; the ANSI CRT
         * (_mktemp/_open) mangles those bytes in the local codepage. */
        wchar_t *wide_template = cbm_utf8_to_wide(tmpl);
        if (!wide_template || !_wmktemp(wide_template)) {
            free(wide_template);
            break;
        }
        char *expanded = cbm_wide_to_utf8(wide_template);
        free(wide_template);
        if (!expanded) {
            break;
        }
        size_t expanded_len = strlen(expanded);
        if (expanded_len >= tmpl_sz) {
            free(expanded);
            errno = ENAMETOOLONG;
            break;
        }
        wchar_t *wide_open = cbm_path_to_wide(expanded);
        if (!wide_open) {
            free(expanded);
            break;
        }
        int fd = _wopen(wide_open, _O_CREAT | _O_EXCL | _O_RDWR | _O_BINARY, _S_IREAD | _S_IWRITE);
        free(wide_open);
        if (fd >= 0) {
            memcpy(tmpl, expanded, expanded_len + SKIP_ONE);
            free(expanded);
            free(pattern);
            return fd;
        }
        free(expanded);
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
