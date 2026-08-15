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

char *cbm_mkdtemp(char *tmpl) {
    /* Per-call storage is required: daemon sessions invoke mkdtemp concurrently.
     * A process-global buffer lets one request overwrite another request's path
     * between expansion, creation, and the copy back to its caller. */
    char buf[CBM_SZ_512];
    int written;
    if (strncmp(tmpl, "/tmp/", 5) == 0) {
        const char *tmp = getenv("TEMP");
        if (!tmp)
            tmp = getenv("TMP");
        if (!tmp)
            tmp = ".";
        written = snprintf(buf, sizeof(buf), "%s\\%s", tmp, tmpl + 5);
    } else {
        written = snprintf(buf, sizeof(buf), "%s", tmpl);
    }
    if (written < 0 || (size_t)written >= sizeof(buf)) {
        errno = ENAMETOOLONG;
        return NULL;
    }

    size_t length = strlen(buf);
    if (length < 6 || strcmp(buf + length - 6, "XXXXXX") != 0) {
        errno = EINVAL;
        return NULL;
    }

    static const char alphabet[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    bool created = false;
    for (int attempt = 0; attempt < 128; attempt++) {
        unsigned char random_suffix[6];
        if (!cbm_secure_random(random_suffix, sizeof(random_suffix))) {
            errno = EIO;
            return NULL;
        }
        for (size_t index = 0; index < sizeof(random_suffix); index++) {
            buf[length - sizeof(random_suffix) + index] =
                alphabet[random_suffix[index] % (sizeof(alphabet) - 1)];
        }

        if (win_mkdtemp_private_create(buf)) {
            created = true;
            break;
        }

        /* Keep the existing compatibility fallback when an explicit private
         * descriptor is unavailable. A name collision is retried; any other
         * filesystem refusal is returned to the caller immediately. */
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
    /* Rewrite /tmp/ to %TEMP%\ like cbm_mkdtemp */
    /* Per-call storage: daemon project workers can create staging files
     * concurrently, so a process-global scratch buffer is a data race. */
    char buf[CBM_SZ_4K];
    int written;
    if (strncmp(tmpl, "/tmp/", 5) == 0) {
        const char *tmp = getenv("TEMP");
        if (!tmp)
            tmp = getenv("TMP");
        if (!tmp)
            tmp = ".";
        written = snprintf(buf, sizeof(buf), "%s\\%s", tmp, tmpl + 5);
    } else {
        written = snprintf(buf, sizeof(buf), "%s", tmpl);
    }
    if (written < 0 || (size_t)written >= sizeof(buf)) {
        errno = ENAMETOOLONG;
        return CBM_NOT_FOUND;
    }
    /* Wide-API expansion and open: worker staging files land inside
     * CBM_CACHE_DIR, which users may place at non-ASCII paths; the ANSI CRT
     * (_mktemp/_open) mangles those bytes in the local codepage. */
    wchar_t *wide_template = cbm_utf8_to_wide(buf);
    if (!wide_template || !_wmktemp(wide_template)) {
        free(wide_template);
        return CBM_NOT_FOUND;
    }
    char *expanded_for_open = cbm_wide_to_utf8(wide_template);
    wchar_t *wide_open = expanded_for_open ? cbm_path_to_wide(expanded_for_open) : NULL;
    free(expanded_for_open);
    if (!wide_open) {
        free(wide_template);
        return CBM_NOT_FOUND;
    }
    int fd = _wopen(wide_open, _O_CREAT | _O_EXCL | _O_RDWR | _O_BINARY, _S_IREAD | _S_IWRITE);
    free(wide_open);
    if (fd >= 0) {
        char *expanded = cbm_wide_to_utf8(wide_template);
        if (!expanded || strlen(expanded) >= sizeof(buf)) {
            free(expanded);
            free(wide_template);
            (void)_close(fd);
            return CBM_NOT_FOUND;
        }
        strcpy(tmpl, expanded);
        free(expanded);
    }
    free(wide_template);
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
