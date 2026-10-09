#include "if_native_git_path.h"

#include <string.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <errno.h>
#include <foundation/compat_fs.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

enum {
    IF_GIT_PATH_CAP = 32768,
    IF_GIT_COMPONENTS = 256,
    IF_GIT_NATIVE_CAP = 4096,
    IF_GIT_MISS = 0,
    IF_GIT_FOUND = 1,
    IF_GIT_ERROR = -1
};

static bool if_git_publish(char *out, size_t capacity, const char *candidate, size_t length) {
    if (!length || length >= capacity)
        return false;
    memcpy(out, candidate, length + 1);
    return true;
}

#ifdef _WIN32

typedef struct {
    wchar_t path[IF_GIT_PATH_CAP];
    wchar_t roundtrip[IF_GIT_PATH_CAP];
    char encoded[IF_GIT_PATH_CAP];
    wchar_t candidate[IF_GIT_NATIVE_CAP];
    wchar_t full[IF_GIT_NATIVE_CAP];
} if_git_windows_scratch;

static bool if_git_separator(wchar_t c) {
    return c == L'/' || c == L'\\';
}

static bool if_git_windows_absolute(const wchar_t *path, size_t length) {
    bool drive = length >= 3 &&
                 ((path[0] >= L'A' && path[0] <= L'Z') ||
                  (path[0] >= L'a' && path[0] <= L'z')) &&
                 path[1] == L':' && if_git_separator(path[2]);
    if (drive)
        return true;
    if (length < 5 || !if_git_separator(path[0]) || !if_git_separator(path[1]) ||
        if_git_separator(path[2]) || path[2] == L'?' || path[2] == L'.')
        return false;
    size_t server_end = 2;
    while (server_end < length && !if_git_separator(path[server_end]))
        server_end++;
    return server_end + 1 < length && !if_git_separator(path[server_end + 1]);
}

/* All conversion buffers are fixed and caller-owned. No ANSI code page,
 * replacement conversion, separator normalization or heap wrapper is used. */
static bool if_git_windows_roundtrip(if_git_windows_scratch *s, const wchar_t *wide,
                                     size_t length, size_t *byte_length) {
    if (length >= IF_GIT_PATH_CAP)
        return false;
    int bytes = WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, wide, (int)length + 1,
                                    s->encoded, IF_GIT_PATH_CAP, NULL, NULL);
    if (bytes <= 0 || s->encoded[bytes - 1] != '\0')
        return false;
    int count = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, s->encoded, bytes,
                                    s->roundtrip, IF_GIT_PATH_CAP);
    if (count != (int)length + 1 ||
        memcmp(wide, s->roundtrip, ((size_t)count) * sizeof(wchar_t)) != 0)
        return false;
    *byte_length = (size_t)bytes - 1;
    return true;
}

static bool if_git_windows_preflight(const wchar_t *path, size_t length) {
    size_t start = 0, components = 0;
    for (size_t i = 0; i <= length; i++) {
        if (i < length && path[i] != L';') {
            /* Quoted/device namespace PATH spellings are outside this small
             * test locator; never silently reinterpret them. */
            if (path[i] == L'"')
                return false;
            continue;
        }
        if (++components > IF_GIT_COMPONENTS || i - start + 9 > IF_GIT_NATIVE_CAP)
            return false;
        if (i - start >= 4 && if_git_separator(path[start]) &&
            if_git_separator(path[start + 1]) &&
            (path[start + 2] == L'?' || path[start + 2] == L'.') &&
            if_git_separator(path[start + 3]))
            return false;
        start = i + 1;
    }
    return true;
}

static int if_git_windows_candidate(if_git_windows_scratch *s, const wchar_t *component,
                                    size_t length, size_t *byte_length) {
    if (!if_git_windows_absolute(component, length))
        return IF_GIT_MISS;
    memcpy(s->candidate, component, length * sizeof(wchar_t));
    size_t used = length;
    if (!if_git_separator(s->candidate[used - 1]))
        s->candidate[used++] = L'\\';
    static const wchar_t tail[] = L"git.exe";
    memcpy(s->candidate + used, tail, sizeof(tail));
    DWORD count = GetFullPathNameW(s->candidate, IF_GIT_NATIVE_CAP, s->full, NULL);
    if (!count || count >= IF_GIT_NATIVE_CAP ||
        !if_git_windows_absolute(s->full, count))
        return IF_GIT_ERROR;
    if (!if_git_windows_roundtrip(s, s->full, count, byte_length) ||
        *byte_length >= IF_GIT_NATIVE_CAP)
        return IF_GIT_ERROR;
    DWORD attributes = GetFileAttributesW(s->full);
    if (attributes == INVALID_FILE_ATTRIBUTES) {
        DWORD error = GetLastError();
        return error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND ||
                       error == ERROR_ACCESS_DENIED
                   ? IF_GIT_MISS
                   : IF_GIT_ERROR;
    }
    if (attributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_DEVICE |
                      FILE_ATTRIBUTE_REPARSE_POINT))
        return IF_GIT_MISS;
    DWORD binary_type = 0;
    if (!GetBinaryTypeW(s->full, &binary_type)) {
        DWORD error = GetLastError();
        return error == ERROR_BAD_EXE_FORMAT || error == ERROR_FILE_NOT_FOUND ||
                       error == ERROR_PATH_NOT_FOUND || error == ERROR_ACCESS_DENIED
                   ? IF_GIT_MISS
                   : IF_GIT_ERROR;
    }
    if (binary_type != SCS_32BIT_BINARY && binary_type != SCS_64BIT_BINARY)
        return IF_GIT_MISS;
    /* This is image-format evidence, not FILE_EXECUTE authorization or Git
     * identity. The existing real Git fixture commands are the positive oracle. */
    return IF_GIT_FOUND;
}

static bool if_git_lookup(char *out, size_t capacity) {
    if_git_windows_scratch scratch;
    DWORD length = GetEnvironmentVariableW(L"PATH", scratch.path, IF_GIT_PATH_CAP);
    if (!length || length >= IF_GIT_PATH_CAP)
        return false;
    size_t path_bytes = 0;
    if (!if_git_windows_roundtrip(&scratch, scratch.path, length, &path_bytes) ||
        !if_git_windows_preflight(scratch.path, length))
        return false;
    size_t start = 0;
    for (size_t i = 0; i <= length; i++) {
        if (i < length && scratch.path[i] != L';')
            continue;
        size_t bytes = 0;
        int result = if_git_windows_candidate(&scratch, scratch.path + start, i - start, &bytes);
        if (result == IF_GIT_ERROR)
            return false;
        if (result == IF_GIT_FOUND)
            return if_git_publish(out, capacity, scratch.encoded, bytes);
        start = i + 1;
    }
    return false;
}

#else

static bool if_git_posix_snapshot(char path[IF_GIT_PATH_CAP], size_t *length) {
    const char *raw = getenv("PATH");
    if (!raw)
        return false;
    size_t n = 0;
    while (n < IF_GIT_PATH_CAP && raw[n])
        n++;
    if (!n || n == IF_GIT_PATH_CAP)
        return false;
    memcpy(path, raw, n + 1);
    *length = n;
    size_t start = 0, components = 0;
    for (size_t i = 0; i <= n; i++) {
        if (i < n && path[i] != ':')
            continue;
        if (++components > IF_GIT_COMPONENTS || i - start + 5 > IF_GIT_NATIVE_CAP)
            return false;
        start = i + 1;
    }
    return true;
}

static int if_git_posix_candidate(const char *component, size_t length,
                                  char canonical[IF_GIT_NATIVE_CAP], size_t *canonical_length) {
    if (!length || component[0] != '/')
        return IF_GIT_MISS;
    char candidate[IF_GIT_NATIVE_CAP];
    memcpy(candidate, component, length);
    size_t used = length;
    if (candidate[used - 1] != '/')
        candidate[used++] = '/';
    memcpy(candidate + used, "git", sizeof("git"));
    struct stat st;
    if (stat(candidate, &st) != 0) {
        return errno == ENOENT || errno == ENOTDIR || errno == EACCES ? IF_GIT_MISS : IF_GIT_ERROR;
    }
    if (!S_ISREG(st.st_mode))
        return IF_GIT_MISS;
    if (access(candidate, X_OK) != 0)
        return errno == EACCES ? IF_GIT_MISS : IF_GIT_ERROR;
    /* Clean existing POSIX implementation uses caller-buffer realpath. Its
     * documented >=4096 output precondition is met; no NULL-buffer allocation. */
    if (!cbm_canonical_path(candidate, canonical, IF_GIT_NATIVE_CAP))
        return IF_GIT_ERROR;
    size_t n = 0;
    while (n < IF_GIT_NATIVE_CAP && canonical[n])
        n++;
    if (!n || n == IF_GIT_NATIVE_CAP || canonical[0] != '/')
        return IF_GIT_ERROR;
    if (stat(canonical, &st) != 0 || !S_ISREG(st.st_mode) || access(canonical, X_OK) != 0)
        return IF_GIT_ERROR;
    *canonical_length = n;
    return IF_GIT_FOUND;
}

static bool if_git_lookup(char *out, size_t capacity) {
    char path[IF_GIT_PATH_CAP], canonical[IF_GIT_NATIVE_CAP];
    size_t length = 0;
    if (!if_git_posix_snapshot(path, &length))
        return false;
    size_t start = 0;
    for (size_t i = 0; i <= length; i++) {
        if (i < length && path[i] != ':')
            continue;
        size_t found_length = 0;
        int result = if_git_posix_candidate(path + start, i - start, canonical, &found_length);
        if (result == IF_GIT_ERROR)
            return false;
        if (result == IF_GIT_FOUND)
            return if_git_publish(out, capacity, canonical, found_length);
        start = i + 1;
    }
    return false;
}

#endif

bool if_native_git_path(char *out, size_t capacity) {
    if (out && capacity)
        out[0] = '\0';
    return out && capacity && if_git_lookup(out, capacity);
}
