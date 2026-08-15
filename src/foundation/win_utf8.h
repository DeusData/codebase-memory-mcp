#ifndef CBM_WIN_UTF8_H
#define CBM_WIN_UTF8_H

#ifdef _WIN32

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <stdlib.h>
#include <wchar.h>

static inline wchar_t *cbm_utf8_to_wide(const char *utf8) {
    if (!utf8) {
        return NULL;
    }
    int len = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, utf8, -1, NULL, 0);
    if (len <= 0) {
        return NULL;
    }
    wchar_t *w = (wchar_t *)malloc((size_t)len * sizeof(wchar_t));
    if (!w || MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, utf8, -1, w, len) != len) {
        free(w);
        return NULL;
    }
    return w;
}

static inline int cbm_win_path_separator(wchar_t value) {
    return value == L'\\' || value == L'/';
}

static inline int cbm_win_path_has_namespace_prefix(const wchar_t *path, size_t length) {
    return length >= 4U && cbm_win_path_separator(path[0]) && cbm_win_path_separator(path[1]) &&
           (path[2] == L'?' || path[2] == L'.') && cbm_win_path_separator(path[3]);
}

static inline void cbm_win_path_normalize_wide_separators(wchar_t *path) {
    if (!path) {
        return;
    }
    for (wchar_t *cursor = path; *cursor; cursor++) {
        if (*cursor == L'/') {
            *cursor = L'\\';
        }
    }
}

/* Return an owned FILE-SYSTEM spelling safe for wide Win32 APIs. Paths
 * approaching the legacy limit are first made absolute (so dot segments keep
 * their ordinary Win32 meaning), then expressed in the extended-length
 * namespace. Both drive and UNC paths are supported, as are already-prefixed
 * paths whose separators were normalized to //?/ by the UTF-8 platform layer.
 * This makes behavior independent of the process manifest and LongPathsEnabled
 * policy. Never use this for non-file strings (command lines, pipe names,
 * registry paths). */
static inline wchar_t *cbm_wide_path_to_extended(const wchar_t *wide_path) {
    if (!wide_path) {
        return NULL;
    }
    size_t input_length = wcslen(wide_path);
    if (input_length >= 32767U) {
        return NULL;
    }
    wchar_t *wide = (wchar_t *)malloc((input_length + 1U) * sizeof(*wide));
    if (!wide) {
        return NULL;
    }
    wmemcpy(wide, wide_path, input_length + 1U);
    size_t length = wcslen(wide);
    if (cbm_win_path_has_namespace_prefix(wide, length)) {
        cbm_win_path_normalize_wide_separators(wide);
        return wide;
    }
    if (length < 240U) {
        return wide;
    }

    DWORD needed = GetFullPathNameW(wide, 0, NULL, NULL);
    wchar_t *full = needed > 0U ? (wchar_t *)malloc((size_t)needed * sizeof(wchar_t)) : NULL;
    DWORD copied = full ? GetFullPathNameW(wide, needed, full, NULL) : 0U;
    free(wide);
    if (!full || copied == 0U || copied >= needed) {
        free(full);
        return NULL;
    }
    cbm_win_path_normalize_wide_separators(full);

    int drive_absolute =
        copied >= 3U &&
        ((full[0] >= L'A' && full[0] <= L'Z') || (full[0] >= L'a' && full[0] <= L'z')) &&
        full[1] == L':' && full[2] == L'\\';
    int unc_absolute = copied >= 5U && full[0] == L'\\' && full[1] == L'\\';
    if (!drive_absolute && !unc_absolute) {
        free(full);
        return NULL;
    }

    const wchar_t *prefix = drive_absolute ? L"\\\\?\\" : L"\\\\?\\UNC\\";
    size_t prefix_length = drive_absolute ? 4U : 8U;
    size_t tail_offset = drive_absolute ? 0U : 2U;
    size_t tail_length = (size_t)copied - tail_offset;
    size_t extended_length = prefix_length + tail_length;
    /* The documented extended-length limit includes the terminating NUL. */
    if (extended_length >= 32767U) {
        free(full);
        return NULL;
    }
    wchar_t *prefixed = (wchar_t *)malloc((extended_length + 1U) * sizeof(wchar_t));
    if (!prefixed) {
        free(full);
        return NULL;
    }
    wmemcpy(prefixed, prefix, prefix_length);
    wmemcpy(prefixed + prefix_length, full + tail_offset, tail_length + 1U);
    free(full);
    return prefixed;
}

static inline wchar_t *cbm_path_to_wide(const char *utf8_path) {
    wchar_t *wide = cbm_utf8_to_wide(utf8_path);
    wchar_t *extended = cbm_wide_path_to_extended(wide);
    free(wide);
    return extended;
}

/* GetModuleFileNameW can preserve the \\?\ spelling used to launch a process.
 * Internal UTF-8 paths use conventional DOS/UNC spelling and add the prefix
 * only at the Win32 file-API boundary, so remove it before widening callers
 * compose adjacent runtime-asset paths. */
static inline void cbm_module_path_unprefix(wchar_t *wide) {
    if (!wide) {
        return;
    }
    if (_wcsnicmp(wide, L"\\\\?\\UNC\\", 8U) == 0) {
        size_t tail_length = wcslen(wide + 8U);
        wmemmove(wide + 2U, wide + 8U, tail_length + 1U);
        wide[0] = L'\\';
        wide[1] = L'\\';
    } else if (wcsncmp(wide, L"\\\\?\\", 4U) == 0) {
        size_t tail_length = wcslen(wide + 4U);
        wmemmove(wide, wide + 4U, tail_length + 1U);
    }
}

static inline char *cbm_wide_to_utf8(const wchar_t *wide) {
    if (!wide) {
        return NULL;
    }
    int len = WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, wide, -1, NULL, 0, NULL, NULL);
    if (len <= 0) {
        return NULL;
    }
    char *u8 = (char *)malloc((size_t)len);
    if (!u8 ||
        WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, wide, -1, u8, len, NULL, NULL) != len) {
        free(u8);
        return NULL;
    }
    return u8;
}

/* Resolve the running image without passing through the active ANSI code page.
 * The returned UTF-8 string is heap allocated. Grow dynamically so an
 * extended-length install path is never mistaken for a complete path. */
static inline char *cbm_module_path_utf8(void) {
    DWORD capacity = 512U;
    while (capacity <= 32768U) {
        wchar_t *wide = (wchar_t *)calloc((size_t)capacity, sizeof(*wide));
        if (!wide) {
            return NULL;
        }
        SetLastError(ERROR_SUCCESS);
        DWORD length = GetModuleFileNameW(NULL, wide, capacity);
        if (length > 0U && length < capacity) {
            cbm_module_path_unprefix(wide);
            char *utf8 = cbm_wide_to_utf8(wide);
            free(wide);
            return utf8;
        }
        DWORD error = GetLastError();
        free(wide);
        if (length == 0U || (length < capacity && error != ERROR_INSUFFICIENT_BUFFER) ||
            capacity == 32768U) {
            return NULL;
        }
        capacity *= 2U;
        if (capacity > 32768U) {
            capacity = 32768U;
        }
    }
    return NULL;
}

#endif /* _WIN32 */
#endif /* CBM_WIN_UTF8_H */
